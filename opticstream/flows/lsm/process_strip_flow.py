import os
import os.path as op
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

import dask
import psutil
from niizarr.multizarr import ZarrConfig
from opticstream.utils.slack_notification import notify_slack
from prefect import flow, get_run_logger, task
from prefect.events import emit_event

from opticstream.config.lsm_scan_config import LSMScanConfigModel, get_lsm_scan_config
from opticstream.flows.lsm.event import STRIP_COMPRESSED
from opticstream.utils.filename_utils import parse_lsm_run_folder_name
from opticstream.state.lsm_project_state import (
    LSMProjectStateService,
    LSM_STATE_SERVICE,
    ProcessingState,
)


@task(
    task_run_name="{project_name}-compress-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def compress_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    info_file: str,
    output_path: str,
    zarr_config: "ZarrConfig",
    num_workers: int = 6,
    cpu_affinity: Optional[List[int]] = None,
    mip_output_path: Optional[str] = None,
    camera_id: Optional[int] = None,
    force_rerun: bool = False,
) -> None:
    """
    Compress a strip of a slice.
    """
    from linc_convert.modalities.lsm import strip

    logger = get_run_logger()
    logger.info(f"Compressing strip {strip_id} of slice {slice_id}")

    # Optionally skip if strip already compressed and not forcing rerun.
    channel_id = camera_id if camera_id is not None else 1
    strip_view = LSM_STATE_SERVICE.peek_strip(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        channel_id=channel_id,
    )
    if strip_view is not None and strip_view.compressed:
        if not force_rerun:
            logger.info(
                f"Strip {strip_id} of slice {slice_id} already marked compressed; "
                "skipping compression"
            )
            return
        else:
            logger.info(
                f"Strip {strip_id} of slice {slice_id} already marked compressed; "
                "forcing rerun"
            )

    if cpu_affinity is not None:
        p = psutil.Process(os.getpid())
        p.cpu_affinity(list(range(*cpu_affinity)))
        # set_pid_io_priority(os.getpid(), 1)
        logger.info(f"cpu affinity:{p.cpu_affinity()}")

    with dask.config.set(pool=ThreadPoolExecutor(num_workers)):
        strip.convert(
            inp=strip_path,
            info_file=info_file,
            mip_image_output=mip_output_path,
            out=output_path,
            zarr_config=zarr_config,
            nii=False,
        )
    logger.info(f"Compressed strip {strip_id} of slice {slice_id} to {output_path}")


@task(
    task_run_name="{project_name}-backup-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def backup_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    output_path: str,
    camera_id: Optional[int] = None,
    force_rerun: bool = False,
) -> None:
    """
    Backup a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Backing up strip {strip_id} of slice {slice_id}")

    # Optionally skip if strip already backed up and not forcing rerun.
    channel_id = camera_id if camera_id is not None else 1
    strip_view = LSM_STATE_SERVICE.peek_strip(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        channel_id=channel_id,
    )
    if strip_view is not None and strip_view.backed_up:
        if not force_rerun:
            logger.info(
                f"Strip {strip_id} of slice {slice_id} already marked backed_up; "
                "skipping backup because force_rerun is False"
            )
            return
        else:
            logger.info(
                f"Strip {strip_id} of slice {slice_id} already marked backed_up; "
                "forcing rerun"
            )
    # copy the strip to the backup path
    shutil.copytree(strip_path, output_path, dirs_exist_ok=True)
    logger.info(f"Backed up strip {strip_id} of slice {slice_id} to {output_path}")


def get_directory_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def format_bytes(num_bytes: int) -> str:
    """
    Format a byte count into a human-readable string.
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:3.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def describe_cleanup(delete_strip: bool, rename_strip: bool) -> str:
    """
    Describe what will happen to the raw strip folder based on configuration.
    """
    if delete_strip:
        return "raw strip folder will be deleted"
    if rename_strip:
        return "raw strip folder will be renamed into processed/"
    return "raw strip folder will be kept in place"


def run_cleanup_tasks(
    delete_strip: bool,
    rename_strip: bool,
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    camera_id: Optional[int] = None,
) -> None:
    """
    Run cleanup tasks synchronously according to delete/rename flags.
    """
    if delete_strip:
        delete_strip_task(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            delete_strip=delete_strip,
        )
    elif rename_strip:
        rename_strip_task(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            rename_strip=rename_strip,
            camera_id=camera_id,
        )


def build_status_message(
    success: bool, failure_reasons: List[str], slice_id: int, strip_id: int
) -> tuple[str, str]:
    """
    Build a status key and human-readable message for Slack notifications.
    """
    if success:
        message = f"Strip {strip_id} of slice {slice_id} processed successfully"
        return "success", message

    base = f"Strip {strip_id} of slice {slice_id} failed validation"
    if failure_reasons:
        base += ": " + ", ".join(failure_reasons)
    return "error", base


@task(
    task_run_name="{project_name}-strip-usage-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def get_strip_usage_stats(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    zarr_output_path: str,
    zarr_size_bytes: int,
    backup_path: Optional[str] = None,
    backup_size_bytes: int = 0,
    camera_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Report raw strip folder size, Zarr folder size, and filesystem disk usage.

    If a backup path is provided, also report backup folder size and disk usage
    for the volume containing the backup folder.
    """
    strip_size_bytes = (
        get_directory_size(strip_path) if os.path.exists(strip_path) else 0
    )
    strip_total, strip_used, strip_free = shutil.disk_usage(strip_path)
    strip_used_percent = (strip_used / strip_total * 100.0) if strip_total else 0.0

    # If backup_path is set and exists, compute its disk usage separately; otherwise
    # fall back to zeros for the backup-related fields.
    if backup_path and os.path.exists(backup_path):
        backup_total, backup_used, backup_free = shutil.disk_usage(backup_path)
        backup_used_percent = (
            backup_used / backup_total * 100.0 if backup_total else 0.0
        )
    else:
        backup_total = backup_used = backup_free = 0
        backup_used_percent = 0.0

    return {
        "strip_size_bytes": strip_size_bytes,
        "strip_size_human": format_bytes(strip_size_bytes),
        "zarr_size_bytes": zarr_size_bytes,
        "zarr_size_human": format_bytes(zarr_size_bytes),
        "backup_size_bytes": backup_size_bytes,
        "backup_size_human": format_bytes(backup_size_bytes),
        "strip_disk_total_bytes": strip_total,
        "strip_disk_used_bytes": strip_used,
        "strip_disk_free_bytes": strip_free,
        "strip_disk_total_human": format_bytes(strip_total),
        "strip_disk_used_human": format_bytes(strip_used),
        "strip_disk_free_human": format_bytes(strip_free),
        "strip_disk_used_percent": f"{strip_used_percent:.2f}%",
        "backup_disk_total_bytes": backup_total,
        "backup_disk_used_bytes": backup_used,
        "backup_disk_free_bytes": backup_free,
        "backup_disk_total_human": format_bytes(backup_total),
        "backup_disk_used_human": format_bytes(backup_used),
        "backup_disk_free_human": format_bytes(backup_free),
        "backup_disk_used_percent": f"{backup_used_percent:.2f}%",
    }


@task(
    task_run_name="{project_name}-check-compressed-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def check_compressed_result(
    project_name: str,
    slice_id: int,
    strip_id: int,
    output_path: str,
    zarr_size_threshold: int = 10**9,
    camera_id: Optional[int] = None,
) -> tuple[bool, int]:
    """
    Check if the compressed strip is valid and report its size.
    """
    logger = get_run_logger()
    logger.info(
        f"Checking if the compressed strip {strip_id} of slice {slice_id} is valid"
    )
    # check if the output path exists
    if not os.path.exists(output_path):
        logger.error(f"Compressed strip {strip_id} of slice {slice_id} does not exist")
        return False, 0

    zarr_size_bytes = get_directory_size(output_path)
    # check if the size of the zarr file is greater than the threshold
    if zarr_size_bytes < zarr_size_threshold:
        logger.error(
            f"Compressed strip {strip_id} of slice {slice_id} is smaller than the "
            f"threshold"
        )
        return False, zarr_size_bytes
    emit_event(
        STRIP_COMPRESSED,
        resource={
            "prefect.resource.id": f"strip:{project_name}:strip-{strip_id}",
            "project_name": project_name,
            "slice_id": str(slice_id),
            "strip_id": str(strip_id),
            "camera_id": str(camera_id),
        },
        payload={
            "project_name": project_name,
            "slice_id": slice_id,
            "strip_id": strip_id,
            "camera_id": camera_id,
            "output_path": output_path,
        },
    )
    return True, zarr_size_bytes


def compare_dirs_by_size(dir1, dir2):
    for root, _, files in os.walk(dir1):
        for file in files:
            path1 = os.path.join(root, file)

            # Get relative path to recreate structure in dir2
            relative_path = os.path.relpath(path1, dir1)
            path2 = os.path.join(dir2, relative_path)

            # Check if file exists in dir2
            if not os.path.exists(path2):
                print(f"Missing in dir2: {relative_path}")
                return False

            # Compare file sizes
            if os.path.getsize(path1) != os.path.getsize(path2):
                print(f"Size mismatch: {relative_path}")
                return False

    # Now check if dir2 has extra files
    for root, _, files in os.walk(dir2):
        for file in files:
            path2 = os.path.join(root, file)
            relative_path = os.path.relpath(path2, dir2)
            path1 = os.path.join(dir1, relative_path)

            if not os.path.exists(path1):
                print(f"Extra file in dir2: {relative_path}")
                return False

    return True


@task(
    task_run_name="{project_name}-check-backup-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def check_backup_result(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    backup_path: Optional[str] = None,
    camera_id: Optional[int] = None,
) -> tuple[bool, int]:
    """
    Check if the backup strip is valid.
    """
    logger = get_run_logger()
    logger.info(f"Checking if the backup strip {strip_id} of slice {slice_id} is valid")
    if backup_path is None:
        logger.warning(
            f"Backup path is not set for strip {strip_id} of slice {slice_id}"
        )
        return True, 0

    # check if the backup path exists
    if not os.path.exists(backup_path):
        logger.error(f"Backup strip {strip_id} of slice {slice_id} does not exist")
        return False, 0

    backup_size_bytes = get_directory_size(backup_path)
    # check if the backup path is the same as the strip path, recursively
    if not compare_dirs_by_size(backup_path, strip_path):
        logger.error(
            f"Backup strip {strip_id} of slice {slice_id} is not the same as the "
            f"strip path"
        )
        return False, backup_size_bytes

    return True, backup_size_bytes


@task(
    task_run_name="{project_name}-rename-strip-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def rename_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    rename_strip: bool = False,
    camera_id: Optional[int] = None,
) -> None:
    """
    Rename a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Renaming strip {strip_id} of slice {slice_id}")
    if not rename_strip:
        logger.info(f"Skipping renaming of strip {strip_id} of slice {slice_id}")
        return
    parent_path, basename = os.path.split(strip_path)
    processed_folder = op.join(parent_path, "processed")
    os.makedirs(processed_folder, exist_ok=True)
    new_strip_path = op.join(processed_folder, basename)
    os.rename(strip_path, new_strip_path)
    logger.info(f"Renamed strip {strip_id} of slice {slice_id} to {new_strip_path}")


@task(
    task_run_name="{project_name}-delete-strip-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def delete_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    delete_strip: bool = False,
) -> None:
    """
    Delete a strip of a slice.
    """
    if not delete_strip:
        logger.info(f"Skipping deletion of strip {strip_id} of slice {slice_id}")
        return
    logger = get_run_logger()
    logger.info(f"Deleting strip {strip_id} of slice {slice_id}")
    # delete the strip
    shutil.rmtree(strip_path)


@flow(
    flow_run_name="{project_name}-process-slice-{slice_id}-"
    "strip-{strip_id}-camera-{camera_id}"
)
def process_strip_flow(
    project_name: str,
    strip_path: str,
    slice_id: Optional[int] = None,
    strip_id: Optional[int] = None,
    camera_id: Optional[int] = None,
    scan_config: LSMScanConfigModel = None,
    force_rerun: bool = False,
) -> None:
    """
    Process a strip of a slice.

    This flow compresses a raw strip into a Zarr store (and optional MIP), optionally
    backs up the raw strip folder, validates the compressed output and backup, and
    then performs cleanup of the raw strip folder.

    Cleanup behavior is controlled by the LSM scan configuration:

    - ``delete_strip``: if ``True``, the raw strip folder will be deleted after all
      checks succeed.
    - ``rename_strip``: if ``True`` and ``delete_strip`` is ``False``, the raw strip
      folder will be moved into a ``processed`` subdirectory.
    - If both are ``False``, the raw strip folder is left in place.

    **Precedence rule:** if ``delete_strip`` is ``True``, it overrides ``rename_strip``
    and the strip folder will be deleted instead of renamed.

    After compression and backup checks complete, the flow emits a Slack notification
    summarizing success or failure, including identifiers, the strip folder path, and
    disk-usage information, and then performs the configured cleanup.
    """
    logger = get_run_logger()
    logger.info(f"Processing strip path: {strip_path}")
    if slice_id is None or strip_id is None:
        logger.info(
            f"Parsing slice_id, strip_id, and camera_id from strip path: {strip_path}"
        )
        run_index, strip_index, camera_index = parse_lsm_run_folder_name(
            os.path.basename(strip_path)
        )
        slice_id = run_index if slice_id is None else slice_id
        strip_id = strip_index if strip_id is None else strip_id
        camera_id = camera_index if camera_id is None else camera_id
        logger.info(
            f"Parsed slice_id: {slice_id}, strip_id: {strip_id}, camera_id: {camera_id}"
        )

    # Derive channel_id from camera_id (default to 1).
    channel_id = camera_id if camera_id is not None else 1

    # Skip entire flow if strip already completed and not forcing rerun.
    existing_strip = LSM_STATE_SERVICE.peek_strip(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        channel_id=channel_id,
    )
    if (
        existing_strip is not None
        and existing_strip.processing_state == ProcessingState.COMPLETED
    ):
        if not force_rerun:
            logger.info(
                f"Strip {strip_id} of slice {slice_id} already completed; "
                "skipping processing because force_rerun is False"
            )
            return None
        else:
            logger.info(
                f"Strip {strip_id} of slice {slice_id} already completed; forcing rerun"
            )
        return None

    # Mark strip as running at the start of processing.
    with LSM_STATE_SERVICE.open_strip(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        channel_id=channel_id,
    ) as strip_state:
        strip_state.mark_started()

    acq = f"camera-{camera_id:02d}" if camera_id is not None else None
    logger.info(f"Processing strip {strip_id} of slice {slice_id}")
    # Resolve output and archive settings from scan_config
    output_path = scan_config.output_path or scan_config.project_base_path
    output_format = (
        scan_config.output_format
        or "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{"
        "acq}.ome.zarr"
    )

    # Resolve processing flags and resources
    archive_path = scan_config.archive_path
    delete_strip = scan_config.delete_strip
    rename_strip = scan_config.rename_strip

    zarr_output_path = op.join(
        output_path,
        output_format.format(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            acq=acq,
        ),
    )
    mip_output_path = None
    if scan_config.generate_mip:
        mip_output_path = op.join(
            output_path,
            scan_config.output_mip_format.format(
                project_name=project_name,
                slice_id=slice_id,
                strip_id=strip_id,
                acq=acq,
            ),
        )

    backup_path = None
    if scan_config.generate_archive and archive_path is not None:
        backup_path = op.join(archive_path, os.path.basename(strip_path))

    compress_future = None
    if scan_config.generate_zarr:
        compress_future = compress_strip_task.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            info_file=scan_config.info_file,
            output_path=zarr_output_path,
            zarr_config=scan_config.zarr_config,
            num_workers=scan_config.num_workers,
            cpu_affinity=scan_config.cpu_affinity,
            mip_output_path=mip_output_path,
            camera_id=camera_id,
            force_rerun=force_rerun,
        )

    backup_future = None
    if backup_path:
        backup_future = backup_strip_task.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            output_path=backup_path,
            camera_id=camera_id,
            force_rerun=force_rerun,
        )

    # Submit checks that run asynchronously after compression/backup are done
    check_compressed_future = None
    if compress_future:
        check_compressed_future = check_compressed_result.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            output_path=zarr_output_path,
            camera_id=camera_id,
            wait_for=[compress_future],
        )

    check_backup_future = None
    if backup_future:
        check_backup_future = check_backup_result.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            backup_path=backup_path,
            camera_id=camera_id,
            wait_for=[backup_future],
        )

    # Wait for checks to complete and gather their results
    compress_ok, zarr_size_bytes = (True, 0)
    if check_compressed_future:
        compress_ok, zarr_size_bytes = check_compressed_future.result()

    backup_ok, backup_size_bytes = (True, 0)
    if check_backup_future:
        backup_ok, backup_size_bytes = check_backup_future.result()

    # Update uploaded/backed_up flags based on check results.
    with LSM_STATE_SERVICE.open_strip(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        channel_id=channel_id,
    ) as strip_state:
        if compress_ok and compress_future:
            strip_state.set_uploaded(True)
        if backup_ok and backup_path is not None:
            strip_state.set_backed_up(True)
        if not (compress_ok and backup_ok):
            strip_state.mark_failed()

    # Compute size and disk usage stats synchronously using both strip and backup info
    usage_info = get_strip_usage_stats(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        strip_path=strip_path,
        zarr_output_path=zarr_output_path,
        zarr_size_bytes=zarr_size_bytes,
        backup_path=backup_path,
        backup_size_bytes=backup_size_bytes,
        camera_id=camera_id,
    )

    success = compress_ok and backup_ok
    failure_reasons: List[str] = []
    if not compress_ok:
        failure_reasons.append("compressed output check failed")
    if not backup_ok:
        failure_reasons.append("backup check failed")

    cleanup_note = describe_cleanup(delete_strip, rename_strip)
    status, message = build_status_message(success, failure_reasons, slice_id, strip_id)

    notify_slack(
        status=status,
        message=message,
        details={
            "project_name": project_name,
            "slice_id": slice_id,
            "strip_id": strip_id,
            "camera_id": camera_id,
            "strip_path": strip_path,
            "cleanup_note": cleanup_note,
            **usage_info,
        },
    )

    # Final state transition based on overall success.
    with LSM_STATE_SERVICE.open_strip(
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        channel_id=channel_id,
    ) as strip_state:
        if success:
            strip_state.mark_completed()
        else:
            strip_state.mark_failed()

    if not success:
        raise RuntimeError(message)

    run_cleanup_tasks(
        delete_strip=delete_strip,
        rename_strip=rename_strip,
        project_name=project_name,
        slice_id=slice_id,
        strip_id=strip_id,
        strip_path=strip_path,
        camera_id=camera_id,
    )

    return None


@flow
def process_strip_event(payload: Dict[str, Any]) -> None:
    """
    Process a strip of a slice.
    """
    logger = get_run_logger()
    lsm_scan_config = get_lsm_scan_config(
        payload["project_name"],
        override_config_name=payload.get("override_config_name"),
    )
    process_strip_flow(
        project_name=payload["project_name"],
        strip_path=payload["strip_path"],
        slice_id=payload["slice_id"],
        strip_id=payload["strip_id"],
        camera_id=payload["camera_id"],
        scan_config=lsm_scan_config,
        force_rerun=payload.get("force_rerun", False),
    )


# process_strip_flow_deployment = process_strip_flow.to_deployment(
#     name="process_strip_flow_deployment",
#     tags=["lsm", "process-strip"],
# )
