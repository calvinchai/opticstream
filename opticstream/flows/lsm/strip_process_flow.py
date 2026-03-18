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
from prefect.events import emit_event
from prefect.futures import PrefectFuture

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.flows.lsm.event import STRIP_COMPRESSED
from opticstream.flows.lsm.paths import strip_mip_output_path, strip_zarr_output_path
from opticstream.flows.lsm.state_guards import (
    force_rerun_from_payload,
    prepare_idempotent_strip_milestone,
    skip_top_level_flow,
)
from opticstream.flows.lsm.utils import load_scan_config_for_payload, strip_ident_from_payload
from opticstream.state.lsm_project_state import (
    LSMStripId,
    LSM_STATE_SERVICE,
    ProcessingState,
)
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


def validate_zarr_directory(
    logger,
    path: str,
    zarr_size_threshold: int,
    *,
    context: str,
    missing_reason: str,
    empty_reason: str,
    below_threshold_reason: str,
) -> ValidationResult:
    """
    Validate a zarr tree: exists, optional min file count when threshold<=0, else min total bytes.
    """
    if not os.path.exists(path):
        logger.error(f"Zarr path does not exist for {context}: {path}")
        return ValidationResult(ok=False, size_bytes=0, reason=missing_reason)
    manifest = get_dir_manifest(path)
    if zarr_size_threshold <= 0:
        if manifest.file_count < 1:
            logger.error(f"Zarr directory empty for {context}: {path}")
            return ValidationResult(ok=False, size_bytes=0, reason=empty_reason)
        logger.info(f"Zarr valid for {context} ({manifest.total_bytes} bytes)")
        return ValidationResult(ok=True, size_bytes=manifest.total_bytes)
    if manifest.total_bytes < zarr_size_threshold:
        logger.error(f"Zarr below size threshold for {context}: {path}")
        return ValidationResult(
            ok=False,
            size_bytes=manifest.total_bytes,
            reason=below_threshold_reason,
        )
    logger.info(f"Zarr valid for {context} ({manifest.total_bytes} bytes)")
    return ValidationResult(ok=True, size_bytes=manifest.total_bytes)


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
    compressed = bool(strip_view and strip_view.compressed)
    if (
        prepare_idempotent_strip_milestone(
            logger,
            milestone_done=compressed,
            force_rerun=force_rerun,
            skip_log=f"{strip_ident} already marked compressed; skipping compression",
            force_log=f"{strip_ident} already marked compressed; forcing rerun",
            strip_ident=strip_ident,
            reset_strip=lambda s: s.reset_compressed(),
        )
        == "skip"
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
    logger.info(f"Backing up {strip_ident}")

    strip_view = LSM_STATE_SERVICE.peek_strip(
        strip_ident=strip_ident,
    )
    archived = bool(strip_view and strip_view.archived)
    if (
        prepare_idempotent_strip_milestone(
            logger,
            milestone_done=archived,
            force_rerun=force_rerun,
            skip_log=(
                f"{strip_ident} already marked archived; skipping backup because force_rerun is False"
            ),
            force_log=f"{strip_ident} already marked archived; forcing rerun",
            strip_ident=strip_ident,
            reset_strip=lambda s: s.reset_archived(),
        )
        == "skip"
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
    emit_event(
        STRIP_COMPRESSED,
        resource={
            "prefect.resource.id": f"{strip_ident}",
        },
        payload={
            "strip_ident": strip_ident.model_dump(mode="json"),
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


def run_cleanup_tasks(
    delete_strip: bool,
    rename_strip: bool,
    strip_ident: LSMStripId,
    strip_path: str,
) -> Optional[PrefectFuture]:
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

    logger.info(f"Processing {strip_ident}")
    project_name = strip_ident.project_name
    slice_id = strip_ident.slice_id
    strip_id = strip_ident.strip_id
    channel_id = strip_ident.channel_id

    existing_strip = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
    pst = existing_strip.processing_state if existing_strip else None
    if skip_top_level_flow(pst, force_rerun=force_rerun, skip_if_running=False):
        logger.info(
            f"Strip {strip_id} of slice {slice_id} already completed; "
            f"skipping processing because force_rerun is False"
        )
        return None
    if pst == ProcessingState.COMPLETED and force_rerun:
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
    logger.info(f"Processing {strip_ident} (acq={acq})")

    archive_path = scan_config.archive_path
    delete_raw_strip = scan_config.delete_strip
    rename_raw_strip = scan_config.rename_strip

    zarr_output_path = strip_zarr_output_path(strip_ident, scan_config)

    mip_output_path = None
    if scan_config.generate_mip:
        mip_output_path = strip_mip_output_path(strip_ident, scan_config)

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

