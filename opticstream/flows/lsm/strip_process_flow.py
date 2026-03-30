import os
import os.path as op
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Sequence

import dask
import psutil
from niizarr.multizarr import ZarrConfig
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture

from opticstream.config.lsm_scan_config import LSMScanConfigModel, StripCleanupAction
from opticstream.data_processing.qc.convert_image import convert_image
from opticstream.events.lsm_events import STRIP_COMPRESSED, STRIP_READY
from opticstream.events.lsm_event_emitters import emit_strip_lsm_event
from opticstream.events.utils import get_event_trigger
from opticstream.state.state_guards import (
    enter_flow_stage,
    force_rerun_from_payload,
    RunDecision,
    enter_milestone_stage,
)
from opticstream.flows.lsm.utils import (
    strip_mip_output_path,
    strip_zarr_output_path,
    load_scan_config_for_payload,
    strip_ident_from_payload,
)
from opticstream.state.lsm_project_state import (
    LSMStripId,
    LSM_STATE_SERVICE,
)
from opticstream.tasks.slack_notification import (
    send_slack_message,
    upload_multiple_files_to_slack,
)
from opticstream.utils.slack_notification_hook import slack_notification_hook
from opticstream.utils.zarr_validation import (
    DirManifest,
    ValidationResult,
    get_dir_manifest,
    validate_zarr_directory,
)


def compare_dir_manifests(
    source_manifest: DirManifest,
    dest_manifest: DirManifest,
    logger=None,
) -> bool:
    if source_manifest.sizes == dest_manifest.sizes:
        return True

    if logger:
        missing = source_manifest.sizes.keys() - dest_manifest.sizes.keys()
        extra = dest_manifest.sizes.keys() - source_manifest.sizes.keys()
        mismatched = {
            k
            for k in (source_manifest.sizes.keys() & dest_manifest.sizes.keys())
            if source_manifest.sizes[k] != dest_manifest.sizes[k]
        }
        if missing:
            logger.error(f"Missing files in destination: {sorted(list(missing))[:10]}")
        if extra:
            logger.error(f"Extra files in destination: {sorted(list(extra))[:10]}")
        if mismatched:
            logger.error(f"Size mismatches: {sorted(list(mismatched))[:10]}")

    return False


def format_bytes(num_bytes: int) -> str:
    """
    Format a byte count into a human-readable string.
    """
    n = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:3.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


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


def get_disk_usage_info(path: Optional[str]) -> Dict[str, Any]:
    """
    Return disk-usage information for the filesystem containing ``path``.

    If path is None or does not exist, return zeroed fields.
    """
    if path is None or not os.path.exists(path):
        total = used = free = 0
    else:
        total, used, free = shutil.disk_usage(path)

    used_percent = (used / total * 100.0) if total else 0.0

    return {
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "total_human": format_bytes(total),
        "used_human": format_bytes(used),
        "free_human": format_bytes(free),
        "used_percent": f"{used_percent:.2f}%",
    }


def build_strip_usage_details(
    strip_size_bytes: int,
    zarr_size_bytes: int,
    backup_size_bytes: int,
    strip_disk: Dict[str, Any],
    backup_disk: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build the notification/report payload for strip storage usage.
    """
    return {
        "strip_size_bytes": strip_size_bytes,
        "strip_size_human": format_bytes(strip_size_bytes),
        "zarr_size_bytes": zarr_size_bytes,
        "zarr_size_human": format_bytes(zarr_size_bytes),
        "backup_size_bytes": backup_size_bytes,
        "backup_size_human": format_bytes(backup_size_bytes),
        "strip_disk_total_bytes": strip_disk["total_bytes"],
        "strip_disk_used_bytes": strip_disk["used_bytes"],
        "strip_disk_free_bytes": strip_disk["free_bytes"],
        "strip_disk_total_human": strip_disk["total_human"],
        "strip_disk_used_human": strip_disk["used_human"],
        "strip_disk_free_human": strip_disk["free_human"],
        "strip_disk_used_percent": strip_disk["used_percent"],
        "backup_disk_total_bytes": backup_disk["total_bytes"],
        "backup_disk_used_bytes": backup_disk["used_bytes"],
        "backup_disk_free_bytes": backup_disk["free_bytes"],
        "backup_disk_total_human": backup_disk["total_human"],
        "backup_disk_used_human": backup_disk["used_human"],
        "backup_disk_free_human": backup_disk["free_human"],
        "backup_disk_used_percent": backup_disk["used_percent"],
    }


@task(task_run_name="compress-{strip_ident}")
def compress_strip(
    strip_ident: LSMStripId,
    strip_path: str,
    info_file: str,
    output_path: str,
    zarr_config: "ZarrConfig",
    num_workers: int = 6,
    cpu_affinity: Optional[List[int]] = None,
    mip_output_path: Optional[str] = None,
    force_rerun: bool = False,
) -> None:
    """
    Compress a strip of a slice.
    """
    from linc_convert.modalities.lsm import strip

    logger = get_run_logger()
    logger.info(f"Compressing {strip_ident} from {strip_path} to {output_path}")

    strip_view = LSM_STATE_SERVICE.peek_strip(
        strip_ident=strip_ident,
    )
    if (
        enter_milestone_stage(
            item_state_view=strip_view,
            item_ident=strip_ident,
            field_name="compressed",
            force_rerun=force_rerun,
        )
        == RunDecision.SKIPPED
    ):
        return
    if cpu_affinity is not None:
        p = psutil.Process(os.getpid())
        p.cpu_affinity(list(range(*cpu_affinity)))
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
    logger.info(f"Compressed {strip_ident} to {output_path}")


@task(task_run_name="backup-{strip_ident}", tags=["lsm-data-archive"])
def archive_strip(
    strip_ident: LSMStripId,
    strip_path: str,
    output_path: str,
    force_rerun: bool = False,
) -> None:
    """
    Backup a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Backing up {strip_ident}")

    strip_view = LSM_STATE_SERVICE.peek_strip(
        strip_ident=strip_ident,
    )
    if (
        enter_milestone_stage(
            item_state_view=strip_view,
            item_ident=strip_ident,
            field_name="archived",
            force_rerun=force_rerun,
        )
        == RunDecision.SKIPPED
    ):
        return
    if invalid_path(output_path):
        raise ValueError(f"Refusing unsafe archive destination: {output_path}")
    rsync_path = shutil.which("rsync")

    if rsync_path is not None:
        subprocess.run(
            [rsync_path, "-a", f"{strip_path}/", f"{output_path}/"],
            check=True,
        )
    else:
        shutil.copytree(
            strip_path,
            output_path,
            dirs_exist_ok=True,
            copy_function=shutil.copy2,
        )
    logger.info(f"Backed up {strip_ident} to {output_path}")


@task(task_run_name="check-compressed-{strip_ident}")
def check_compressed_result(
    strip_ident: LSMStripId,
    output_path: Optional[str] = None,
    mip_output_path: Optional[str] = None,
    generate_mip: bool = True,
    generate_zarr: bool = True,
    mip_size_threshold: int = 10**6,  # 1MB
    zarr_size_threshold: int = 10**9,  # 1GB
) -> ValidationResult:
    """
    Check if the compressed strip is valid and report its size.
    """
    logger = get_run_logger()
    # check mip
    if generate_mip:
        if invalid_path(mip_output_path):
            logger.error(f"MIP output path is not set for {strip_ident}")
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output path is not set",
            )
        if not os.path.exists(mip_output_path):
            logger.error(f"MIP output {mip_output_path} does not exist")
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output does not exist",
            )
        if os.path.getsize(mip_output_path) < mip_size_threshold:
            logger.error(f"MIP output {mip_output_path} is smaller than the threshold")
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output below size threshold",
            )
    zarr_size_bytes = 0
    if generate_zarr:
        logger.info(f"Checking if the compressed strip {strip_ident} is valid")
        zr = validate_zarr_directory(
            logger,
            output_path,
            zarr_size_threshold,
            context=str(strip_ident),
            missing_reason="compressed output missing",
            empty_reason="compressed output empty",
            below_threshold_reason="compressed output below size threshold",
        )
        if not zr.ok:
            return zr
        zarr_size_bytes = zr.size_bytes

    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.set_compressed(True)
    emit_strip_lsm_event(STRIP_COMPRESSED, strip_ident)
    return ValidationResult(ok=True, size_bytes=zarr_size_bytes)


@task(task_run_name="check-backup-{strip_ident}")
def check_backup_result(
    strip_ident: LSMStripId,
    strip_path: str,
    backup_path: Optional[str] = None,
) -> ValidationResult:
    """
    Check if the backup strip is valid.
    """
    logger = get_run_logger()
    logger.info(f"Checking if backup for {strip_ident} is valid")

    if backup_path is None:
        logger.warning(f"Backup path is not set for {strip_ident}")
        return ValidationResult(ok=True, size_bytes=0)

    if not os.path.exists(backup_path):
        logger.error(f"Backup strip {strip_ident} does not exist")
        return ValidationResult(
            ok=False,
            size_bytes=0,
            reason="backup missing",
        )

    source_manifest = get_dir_manifest(strip_path)
    backup_manifest = get_dir_manifest(backup_path)

    if not compare_dir_manifests(source_manifest, backup_manifest, logger=logger):
        logger.error(f"Backup strip {strip_ident} is not the same as the strip path")
        return ValidationResult(
            ok=False,
            size_bytes=backup_manifest.total_bytes,
            reason="backup differs from source",
        )

    return ValidationResult(ok=True, size_bytes=backup_manifest.total_bytes)


@task(on_failure=[slack_notification_hook])
def strip_mip_qc_notification(
    strip_ident: LSMStripId,
    mip_stitched_path: Optional[str] = None,
    output_mip_preview_window: List[float] = [],
) -> None:
    """
    Send a notification for the strip MIP QC.
    """
    logger = get_run_logger()
    logger.info(f"Sending notification for strip MIP QC: {strip_ident}")
    if mip_stitched_path is None:
        logger.warning(f"MIP stitched path is not set for {strip_ident}")
        return
    window_min = None
    window_max = None
    if output_mip_preview_window:
        if len(output_mip_preview_window) != 2:
            raise ValueError(
                f"Output MIP preview window must be a list of 2 values: {output_mip_preview_window}"
            )
        window_min, window_max = output_mip_preview_window
    preview_path = mip_stitched_path.replace(".tiff", ".jpg")
    convert_image(
        input=mip_stitched_path,
        output=preview_path,
        window_min=window_min,
        window_max=window_max,
        split=2,
    )
    files = [preview_path] + [preview_path.with_suffix(f".part{i + 1}.jpg") for i in range(2)]
    upload_multiple_files_to_slack(
        filepaths=files, initial_comment=f"MIP QC for {strip_ident}"
    )


@task(task_run_name="rename-strip-{strip_ident}")
def rename_strip_task(
    strip_ident: LSMStripId,
    strip_path: str,
    do_rename: bool = False,
) -> None:
    """
    Rename a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Renaming {strip_ident}")
    if not do_rename:
        logger.info(f"Skipping renaming of {strip_ident}")
        return
    parent_path, basename = os.path.split(strip_path)
    processed_folder = op.join(parent_path, "processed")
    os.makedirs(processed_folder, exist_ok=True)
    new_strip_path = op.join(processed_folder, basename)
    os.rename(strip_path, new_strip_path)
    logger.info(f"Renamed {strip_ident} to {new_strip_path}")


@task(task_run_name="delete-strip-{strip_ident}")
def delete_strip_task(
    strip_ident: LSMStripId,
    strip_path: str,
    do_delete: bool = False,
) -> None:
    """
    Delete a strip of a slice.
    """
    logger = get_run_logger()
    if not do_delete:
        logger.info(f"Skipping deletion of {strip_ident}")
        return
    logger.info(f"Deleting {strip_ident}")
    shutil.rmtree(strip_path)


def describe_cleanup(cleanup_action: StripCleanupAction) -> str:
    """
    Describe what will happen to the raw strip folder based on configuration.
    """
    if cleanup_action == StripCleanupAction.DELETE:
        return "raw strip folder will be deleted"
    if cleanup_action == StripCleanupAction.RENAME:
        return "raw strip folder will be renamed into processed/"
    return "raw strip folder will be kept in place"


def invalid_path(path: Optional[str]) -> bool:
    return path is None or path in {"/", ".", ""}


def run_cleanup_tasks(
    cleanup_action: StripCleanupAction,
    strip_ident: LSMStripId,
    strip_path: str,
) -> Optional[PrefectFuture]:
    """
    Run cleanup task synchronously according to cleanup action.
    """
    if cleanup_action == StripCleanupAction.DELETE:
        delete_future = delete_strip_task.submit(
            strip_ident=strip_ident,
            strip_path=strip_path,
            do_delete=True,
        )
        return delete_future

    if cleanup_action == StripCleanupAction.RENAME:
        rename_future = rename_strip_task.submit(
            strip_ident=strip_ident,
            strip_path=strip_path,
            do_rename=True,
        )
        return rename_future

    return None


@flow(flow_run_name="process-strip-{strip_ident}")
def process_strip(
    strip_ident: LSMStripId,
    strip_path: str,
    scan_config: LSMScanConfigModel,
    force_rerun: bool = False,
) -> None:
    """
    Process a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Processing strip path: {strip_path} {strip_ident}")

    if (
        enter_flow_stage(
            LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident),
            force_rerun=force_rerun,
            skip_if_running=False,
            item_ident=strip_ident,
        )
        == RunDecision.SKIPPED
    ):
        return None

    initial_strip_manifest = (
        get_dir_manifest(strip_path)
        if os.path.exists(strip_path)
        else DirManifest(file_count=0, total_bytes=0, sizes={})
    )

    acq = f"camera-{strip_ident.channel_id:02d}"
    logger.info(f"Processing {strip_ident} (acq={acq})")

    archive_path = scan_config.archive_path
    strip_cleanup_action = scan_config.strip_cleanup_action

    zarr_output_path = strip_zarr_output_path(strip_ident, scan_config)
    has_zarr_output = scan_config.output_path is not None

    mip_output_path = None
    if scan_config.generate_mip:
        mip_output_path = strip_mip_output_path(strip_ident, scan_config)

    backup_path = None
    if archive_path is not None:
        backup_path = op.join(archive_path, os.path.basename(strip_path))

    compress_future = None
    if scan_config.output_path is not None:
        compress_future = compress_strip.submit(
            strip_ident=strip_ident,
            strip_path=strip_path,
            info_file=scan_config.info_file,
            output_path=zarr_output_path,
            zarr_config=scan_config.zarr_config,
            num_workers=scan_config.num_workers,
            cpu_affinity=scan_config.cpu_affinity,
            mip_output_path=mip_output_path,
            force_rerun=force_rerun,
        )

    backup_future = None
    if backup_path:
        backup_future = archive_strip.submit(
            strip_ident=strip_ident,
            strip_path=strip_path,
            output_path=backup_path,
            force_rerun=force_rerun,
        )

    check_compressed_future = None
    if compress_future:
        check_compressed_future = check_compressed_result.submit(
            strip_ident=strip_ident,
            output_path=zarr_output_path,
            mip_output_path=mip_output_path,
            generate_mip=scan_config.generate_mip,
            generate_zarr=has_zarr_output,
            wait_for=[compress_future],
        )

    check_backup_future = None
    if backup_future:
        check_backup_future = check_backup_result.submit(
            strip_ident=strip_ident,
            strip_path=strip_path,
            backup_path=backup_path,
            wait_for=[backup_future],
        )

    backup_result = ValidationResult(ok=True, size_bytes=0)
    if check_backup_future:
        try:
            backup_result = check_backup_future.result()
        except Exception as e:
            logger.error(f"Error checking backup result: {e}")
            backup_result = ValidationResult(
                ok=False,
                size_bytes=0,
                reason=f"backup check error: {e}",
            )

    compress_result = ValidationResult(ok=True, size_bytes=0)
    if check_compressed_future:
        try:
            compress_result = check_compressed_future.result()
        except Exception as e:
            logger.error(f"Error checking compressed result: {e}")
            compress_result = ValidationResult(
                ok=False,
                size_bytes=0,
                reason=f"compressed check error: {e}",
            )
        if scan_config.generate_mip:
            strip_mip_qc_notification(
                strip_ident=strip_ident,
                mip_stitched_path=mip_output_path,
                output_mip_preview_window=scan_config.output_mip_preview_window,
            )

    success = compress_result.ok and backup_result.ok

    strip_disk = get_disk_usage_info(strip_path)
    backup_disk = get_disk_usage_info(backup_path)

    usage_info = build_strip_usage_details(
        strip_size_bytes=initial_strip_manifest.total_bytes,
        zarr_size_bytes=compress_result.size_bytes,
        backup_size_bytes=backup_result.size_bytes,
        strip_disk=strip_disk,
        backup_disk=backup_disk,
    )

    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        if backup_result.ok and backup_path is not None:
            strip_state.set_archived(True)
        if success:
            strip_state.mark_completed()
        else:
            strip_state.mark_failed()

    failure_reasons: List[str] = []
    if not compress_result.ok and compress_result.reason:
        failure_reasons.append(f"compressed output: {compress_result.reason}")
    if not backup_result.ok and backup_result.reason:
        failure_reasons.append(f"backup: {backup_result.reason}")

    cleanup_note = describe_cleanup(strip_cleanup_action)
    status, message = build_status_message(
        success, failure_reasons, strip_ident.slice_id, strip_ident.strip_id
    )

    send_slack_message(f"{message} {cleanup_note}, {usage_info},")

    if not success:
        raise RuntimeError(message)

    cleanup_future = run_cleanup_tasks(
        cleanup_action=strip_cleanup_action,
        strip_ident=strip_ident,
        strip_path=strip_path,
    )
    if cleanup_future:
        cleanup_future.wait()

    return None


@flow
def process_strip_event(payload: Dict[str, Any]) -> None:
    """
    Process a strip of a slice.

    Payload must include ``strip_ident`` (dict) and ``strip_path``.
    """
    strip_ident = strip_ident_from_payload(payload)
    if "strip_path" not in payload:
        raise KeyError("payload must include strip_path")
    lsm_scan_config = load_scan_config_for_payload(strip_ident.project_name, payload)
    process_strip(
        strip_ident=strip_ident,
        strip_path=payload["strip_path"],
        scan_config=lsm_scan_config,
        force_rerun=force_rerun_from_payload(payload),
    )


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
    concurrent_workers: int = 2,
):
    """
    Create both deployments:
    - manual `process_strip`
    - event-driven `process_strip_event` (triggered by STRIP_READY)
    """
    manual = process_strip.to_deployment(
        name=deployment_name,
        tags=["lsm", "process-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
    )
    event = process_strip_event.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "process-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
        triggers=[get_event_trigger(STRIP_READY, project_name=project_name)],
    )
    return [manual, event]


if __name__ == "__main__":
    import prefect

    prefect.serve(*to_deployment())
