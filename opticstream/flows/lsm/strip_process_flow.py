from asyncio import Future
from datetime import datetime
import os
import os.path as op
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import dask
import psutil
from niizarr.multizarr import ZarrConfig
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.events import emit_event

from opticstream.config.lsm_scan_config import LSMScanConfigModel, get_lsm_scan_config
from opticstream.flows.lsm.event import CHANNEL_READY, STRIP_COMPRESSED
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMProjectStateView,
    LSMStripId,
    LSM_STATE_SERVICE,
    ProcessingState,
)
from opticstream.utils.filename_utils import parse_lsm_run_folder_name
from opticstream.utils.slack_notification import notify_slack


@dataclass
class DirManifest:
    file_count: int
    total_bytes: int
    sizes: Dict[str, int]


@dataclass
class ValidationResult:
    ok: bool
    size_bytes: int
    reason: Optional[str] = None


def get_dir_manifest(path: str) -> DirManifest:
    """
    Compute total size, file count, and relative-path -> size mapping
    in a single walk.
    """
    total_bytes = 0
    file_count = 0
    sizes: Dict[str, int] = {}
    root = Path(path)

    for p in root.rglob("*"):
        if p.is_file():
            st = p.stat()
            rel = str(p.relative_to(root))
            sizes[rel] = st.st_size
            total_bytes += st.st_size
            file_count += 1

    return DirManifest(
        file_count=file_count,
        total_bytes=total_bytes,
        sizes=sizes,
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
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:3.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


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
    if strip_view is not None and strip_view.compressed:
        if not force_rerun:
            logger.info(
                "%s already marked compressed; skipping compression", strip_ident
            )
            return
        else:
            logger.info("%s already marked compressed; forcing rerun", strip_ident)

    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.reset_compressed()
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
    logger.info("Compressed %s to %s", strip_ident, output_path)


@task(task_run_name="backup-{strip_ident}")
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
    logger.info("Backing up %s", strip_ident)

    strip_view = LSM_STATE_SERVICE.peek_strip(
        strip_ident=strip_ident,
    )
    if strip_view is not None and strip_view.archived:
        if not force_rerun:
            logger.info(
                "%s already marked archived; skipping backup because force_rerun is False",
                strip_ident,
            )
            return
        else:
            logger.info("%s already marked archived; forcing rerun", strip_ident)
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.reset_archived()
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
    logger.info("Backed up %s to %s", strip_ident, output_path)


@task(task_run_name="check-compressed-{strip_ident}")
def check_compressed_result(
    strip_ident: LSMStripId,
    output_path: Optional[str] = None,
    mip_output_path: Optional[str] = None,
    generate_mip: bool = True,
    generate_zarr: bool = True,
    mip_size_threshold: int = 10**6, # 1MB
    zarr_size_threshold: int = 10**9, # 1GB
) -> ValidationResult:
    """
    Check if the compressed strip is valid and report its size.
    """
    logger = get_run_logger()
    # check mip 
    if generate_mip:
        if invalid_path(mip_output_path):
            logger.error("MIP output path is not set for %s", strip_ident)
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output path is not set",
            )
        if not os.path.exists(mip_output_path):
            logger.error("MIP output %s does not exist", mip_output_path)
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output does not exist",
            )
        if os.path.getsize(mip_output_path) < mip_size_threshold:
            logger.error("MIP output %s is smaller than the threshold", mip_output_path)
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="mip output below size threshold",
            )
    zarr_size_bytes = 0
    if generate_zarr:
        logger.info("Checking if the compressed strip %s is valid", strip_ident)
        if not os.path.exists(output_path):
            logger.error("Compressed strip %s does not exist", strip_ident)
            return ValidationResult(
                ok=False,
                size_bytes=0,
                reason="compressed output missing",
            )

        manifest = get_dir_manifest(output_path)
        zarr_size_bytes = manifest.total_bytes
        if zarr_size_bytes < zarr_size_threshold:
            logger.error("Compressed strip %s is smaller than the threshold", strip_ident)
            return ValidationResult(
                ok=False,
                size_bytes=zarr_size_bytes,
                reason="compressed output below size threshold",
            )

    emit_event(
        STRIP_COMPRESSED,
        resource={
            "prefect.resource.id": f"{strip_ident}",
        },
        payload={
            "project_name": strip_ident.project_name,
            "strip_ident": strip_ident,
            "output_path": output_path,
        },
    )
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
    logger.info("Checking if backup for %s is valid", strip_ident)

    if backup_path is None:
        logger.warning("Backup path is not set for %s", strip_ident)
        return ValidationResult(ok=True, size_bytes=0)

    if not os.path.exists(backup_path):
        logger.error("Backup strip %s does not exist", strip_ident)
        return ValidationResult(
            ok=False,
            size_bytes=0,
            reason="backup missing",
        )

    source_manifest = get_dir_manifest(strip_path)
    backup_manifest = get_dir_manifest(backup_path)

    if not compare_dir_manifests(source_manifest, backup_manifest, logger=logger):
        logger.error("Backup strip %s is not the same as the strip path", strip_ident)
        return ValidationResult(
            ok=False,
            size_bytes=backup_manifest.total_bytes,
            reason="backup differs from source",
        )

    return ValidationResult(ok=True, size_bytes=backup_manifest.total_bytes)


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
    logger.info("Renaming %s", strip_ident)
    if not do_rename:
        logger.info("Skipping renaming of %s", strip_ident)
        return
    parent_path, basename = os.path.split(strip_path)
    processed_folder = op.join(parent_path, "processed")
    os.makedirs(processed_folder, exist_ok=True)
    new_strip_path = op.join(processed_folder, basename)
    os.rename(strip_path, new_strip_path)
    logger.info("Renamed %s to %s", strip_ident, new_strip_path)


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
        logger.info("Skipping deletion of %s", strip_ident)
        return
    logger.info("Deleting %s", strip_ident)
    shutil.rmtree(strip_path)


def describe_cleanup(delete_strip: bool, rename_strip: bool) -> str:
    """
    Describe what will happen to the raw strip folder based on configuration.
    """
    if delete_strip:
        return "raw strip folder will be deleted"
    if rename_strip:
        return "raw strip folder will be renamed into processed/"
    return "raw strip folder will be kept in place"

def invalid_path(path: Optional[str]) -> bool:
    return path is None or path in {"/", ".", ""}


def strip_ident_from_payload(payload: Dict[str, Any]) -> LSMStripId:
    """Build LSMStripId from an event payload or post-processing callback."""
    raw = payload.get("strip_ident")
    if isinstance(raw, LSMStripId):
        return raw
    if isinstance(raw, dict):
        return LSMStripId(**raw)
    return LSMStripId(
        project_name=payload["project_name"],
        slice_id=int(payload["slice_id"]),
        strip_id=int(payload["strip_id"]),
        channel_id=int(payload.get("camera_id") or payload.get("channel_id") or 1),
    )


@task(task_run_name="{project_name}-lsm-strip-artifact")
def update_lsm_strip_artifact_task(
    project_name: str,
    state: LSMProjectStateView,
    strips_per_slice: int,
) -> str:
    """
    Publish a Prefect table artifact summarizing LSM strip progress from project state.
    """
    logger = get_run_logger()
    artifact_key = f"{project_name.lower().replace('_', '-')}-lsm-strip-progress"
    table_data: List[Dict[str, Any]] = []
    total_completed_strips = 0
    total_strip_slots = 0

    for slice_id in sorted(state.slices.keys()):
        slice_view = state.slices[slice_id]
        for channel_id in sorted(slice_view.channels.keys()):
            channel_view = slice_view.channels[channel_id]
            total = strips_per_slice
            compressed = archived = uploaded = completed = 0
            for strip_id in range(1, total + 1):
                sv = channel_view.strips.get(strip_id)
                if sv is None:
                    continue
                if sv.compressed:
                    compressed += 1
                if sv.archived:
                    archived += 1
                if sv.uploaded:
                    uploaded += 1
                if sv.processing_state == ProcessingState.COMPLETED:
                    completed += 1
            pct = (completed / total * 100.0) if total else 0.0
            table_data.append(
                {
                    "Slice": slice_id,
                    "Channel": channel_id,
                    "Total Strips": total,
                    "Compressed": compressed,
                    "Archived": archived,
                    "Uploaded": uploaded,
                    "Completed": completed,
                    "Progress": f"{pct:.1f}%",
                }
            )
            total_completed_strips += completed
            total_strip_slots += total

    if not table_data:
        logger.warning(
            "No slice/channel rows in LSM state for artifact %s", artifact_key
        )
        return ""

    overall_pct = (
        total_completed_strips / total_strip_slots * 100.0
        if total_strip_slots
        else 0.0
    )
    milestones = [
        "✅" if overall_pct >= 25 else "⏳",
        "✅" if overall_pct >= 50 else "⏳",
        "✅" if overall_pct >= 75 else "⏳",
        "✅" if overall_pct >= 100 else "⏳",
    ]
    description = f"""LSM strip processing progress

Project: {project_name}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Overall completed strip slots: {total_completed_strips}/{total_strip_slots} ({overall_pct:.1f}%)

Milestones:
- {milestones[0]} 25% Complete
- {milestones[1]} 50% Complete
- {milestones[2]} 75% Complete
- {milestones[3]} 100% Complete
"""

    create_table_artifact(key=artifact_key, table=table_data, description=description)
    logger.info("Updated LSM strip artifact %s", artifact_key)
    return artifact_key

def run_cleanup_tasks(
    delete_strip: bool,
    rename_strip: bool,
    strip_ident: LSMStripId,
    strip_path: str,
) -> Optional[Future]:
    """
    Run cleanup tasks synchronously according to delete/rename flags.
    """
    if delete_strip:
        delete_future = delete_strip_task.submit(
            strip_ident=strip_ident,
            strip_path=strip_path,
            do_delete=True,
        )
        return delete_future

    if rename_strip:
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
    logger.info(f"Processing strip path: {strip_path}")

    logger.info("Processing %s", strip_ident)
    project_name = strip_ident.project_name
    slice_id = strip_ident.slice_id
    strip_id = strip_ident.strip_id
    channel_id = strip_ident.channel_id

    existing_strip = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
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

    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.mark_started()

    initial_strip_manifest = (
        get_dir_manifest(strip_path)
        if os.path.exists(strip_path)
        else DirManifest(file_count=0, total_bytes=0, sizes={})
    )

    acq = f"camera-{channel_id:02d}"
    logger.info("Processing %s (acq=%s)", strip_ident, acq)

    archive_path = scan_config.archive_path
    delete_raw_strip = scan_config.delete_strip
    rename_raw_strip = scan_config.rename_strip

    zarr_output_path = op.join(
        scan_config.output_path,
        scan_config.output_format.format(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            acq=acq,
        ),
    )

    mip_output_path = None
    if scan_config.generate_mip:
        mip_output_path = op.join(
            scan_config.output_path,
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
    if scan_config.generate_zarr or scan_config.generate_mip:
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
            generate_zarr=scan_config.generate_zarr,
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
        if compress_result.ok and compress_future:
            strip_state.set_compressed(True)
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

    cleanup_note = describe_cleanup(delete_raw_strip, rename_raw_strip)
    status, message = build_status_message(success, failure_reasons, slice_id, strip_id)

    notify_slack(
        status=status,
        message=message,
        details={
            "project_name": project_name,
            "slice_id": slice_id,
            "strip_id": strip_id,
            "camera_id": channel_id,
            "strip_path": strip_path,
            "cleanup_note": cleanup_note,
            **usage_info,
        },
    )

    if not success:
        raise RuntimeError(message)

    cleanup_future = run_cleanup_tasks(
        delete_strip=delete_raw_strip,
        rename_strip=rename_raw_strip,
        strip_ident=strip_ident,
        strip_path=strip_path,
    )
    if cleanup_future:
        cleanup_future.wait()
    # if cleanup ok, mark cleaned

    return [compress_future, backup_future, check_compressed_future, check_backup_future, cleanup_future]


@flow
def process_strip_event(payload: Dict[str, Any]) -> None:
    """
    Process a strip of a slice.
    """
    lsm_scan_config = get_lsm_scan_config(
        payload["project_name"],
        override_config_name=payload.get("override_config"),
    )
    strip_ident = LSMStripId(
        project_name=payload["project_name"],
        slice_id=payload["slice_id"],
        strip_id=payload["strip_id"],
        channel_id=payload.get("camera_id") or payload.get("channel_id") or 1,
    )
    process_strip(
        strip_ident=strip_ident,
        strip_path=payload["strip_path"],
        scan_config=lsm_scan_config,
        force_rerun=payload.get("force_rerun", False),
    )
    

# process_strip_flow_deployment = process_strip_flow.to_deployment(
#     name="process_strip_flow_deployment",
#     tags=["lsm", "process-strip"],
# )


@task(task_run_name="check-channel-ready-{channel_ident}")
def check_channel_ready(
    channel_ident: LSMChannelId,
    strips_per_slice: int,
) -> None:
    """
    If every strip in the channel has been compressed, emit CHANNEL_READY.
    """
    channel_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    if channel_view is None:
        return
    if channel_view.all_compressed(total_strips=strips_per_slice):
        emit_event(
            CHANNEL_READY,
            resource={
                "prefect.resource.id": f"{channel_ident}",
                "linc.opticstream.project": channel_ident.project_name,
            },
            payload={
                "channel_ident": channel_ident,
            },
        )



@flow
def on_strip_events(event: str, payload: Dict[str, Any]) -> None:
    """
    Refresh the LSM strip progress Prefect artifact from current project state.
    """
    project_name = payload["project_name"]
    channel_ident = channel_ident_from_payload(payload)
    state = LSM_STATE_SERVICE.peek_project_by_parts(project_name)
    cfg = get_lsm_scan_config(
        project_name,
    )
    update_lsm_strip_artifact_task(
        project_name=project_name,
        state=state,
        strips_per_slice=cfg.strips_per_slice,
    )
    if event == STRIP_COMPRESSED:
        check_channel_ready(channel_ident, cfg.strips_per_slice)



def process_channel(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
    *,
    force_rerun: bool = False,
):
    """
    Process a channel.
    """
    logger = get_run_logger()
    logger.info(f"Processing channel: {channel_ident}")
    channel_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    if channel_view is not None:
        if channel_view.processing_state == ProcessingState.COMPLETED:
            if not force_rerun:
                logger.info(
                    f"Channel {channel_ident} already completed; skipping processing because force_rerun is False"
                )
                return

            else:
                logger.info(f"Channel {channel_ident} already completed; forcing rerun")
        elif channel_view.processing_state == ProcessingState.RUNNING:
            if not force_rerun:
                logger.info(
                    f"Channel {channel_ident} already running; skipping processing because force_rerun is False"
                )
                return
            else:
                logger.info(f"Channel {channel_ident} already running; forcing rerun")

    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as channel_state:
        channel_state.mark_started()
    if scan_config.generate_mip:
        # stitch mip
        # submit the task with linc-convert pipeline
        # actually use synchronous version of the task
        
        stitch_mip_future = stitch_mip.submit()
        convert_image_future = convert_image.submit(wait_for=[stitch_mip_future])
        send_to_slack_future = send_to_slack.submit(wait_for=[convert_image_future])
        
        check_result
        if not check_result.ok:
            failed
        # mark mip stitched
    # if not stitched 3d, mark channel finished/failed
    # if sittched 3d, let 3d handles the rest
    # emit 2d stitched
      
    emit_channel_done
    mark_channel_done

def stitch_channel_volume():

    # on channel stitched
    # stitch channel volume
    # check not stitched yet
    # mark channel volume stitched
    # emit channel volume stitched
    pass

def upload_channel_volume():
    # on channel volume stitched
    # upload the volume
    # mark channel volume uploaded
    # emit channel volume uploaded
    pass