import os
import shutil
import subprocess
from pathlib import Path
import time
from typing import Any, Dict, Optional, Sequence

from prefect import flow, get_run_logger, task

from opticstream.events import get_event_trigger
from opticstream.events.lsm_events import STRIP_ARCHIVED, STRIP_READY
from opticstream.hooks.publish_hooks import (
    publish_lsm_project_hook,
    publish_lsm_slice_hook,
)
from opticstream.hooks.slack_notification_hook import slack_notification_hook
from opticstream.flows.lsm.utils import (
    host_lsm_fs_path,
    load_scan_config_for_payload,
    strip_ident_from_payload,
)
from opticstream.flows.lsm.strip_cleanup_flow import run_cleanup_tasks
from opticstream.state.lsm_project_state import LSMStripId, LSM_STATE_SERVICE
from opticstream.state.milestone_wrappers_lsm import strip_processing_milestone
from opticstream.state.state_guards import RunDecision, enter_milestone_stage, force_rerun_from_payload
from opticstream.utils.zarr_validation import (
    ValidationResult,
    compare_dir_manifests,
    get_dir_manifest,
)


def invalid_path(path: Optional[str]) -> bool:
    return path is None or path in {"/", ".", ""}


def _make_throttled_copy(rate_limit_mb: int):
    """
    Return a copy_function for shutil.copytree that paces transfers to
    ``rate_limit_mb`` MB/s by sleeping between files.

    Because individual files are small relative to the rate limit the
    sleep granularity is per-file, which is sufficient.
    """
    rate_limit_bytes = rate_limit_mb * 10**6
    start = time.monotonic()
    total_copied: list[int] = [0]

    def _copy(src: str, dst: str) -> None:
        shutil.copy2(src, dst)
        total_copied[0] += os.path.getsize(dst)
        expected_elapsed = total_copied[0] / rate_limit_bytes
        deficit = expected_elapsed - (time.monotonic() - start)
        if deficit > 0:
            time.sleep(deficit)

    return _copy


@task(task_run_name="archive-{strip_ident}", tags=["lsm-data-archive"])
def archive_strip(
    strip_ident: LSMStripId,
    strip_path: str,
    output_path: str,
    rate_limit_mb: int = 500,
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
        bwlimit_kb = max(1, rate_limit_mb * 1000)
        command = [rsync_path, "-a", f"--bwlimit={bwlimit_kb}", f"{strip_path}/", f"{output_path}/"]
        
        subprocess.run(command, check=True)
        logger.info(f"Backed up {strip_ident} to {output_path} with command: {command}")
    else:
        shutil.copytree(
            strip_path,
            output_path,
            dirs_exist_ok=True,
            copy_function=_make_throttled_copy(rate_limit_mb),
        )
    logger.info(f"Backed up {strip_ident} to {output_path}")


@task(task_run_name="check-backup-{strip_ident}")
def check_archive_result(
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
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.set_archived(True)
    return ValidationResult(ok=True, size_bytes=backup_manifest.total_bytes)

@task
def check_disbributed_archive_result(
    strip_ident: LSMStripId,
    timeout: int,
    strip_path: Optional[str] = None,
    backup_path: Optional[str] = None,
) -> ValidationResult:
    """
    Check if the backup strip is valid.
    """
    logger = get_run_logger()
    logger.info(f"Checking if backup for {strip_ident} is valid")
    start_time = time.time()
    while time.time() - start_time < timeout:
        strip_view = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
        if strip_view.archived:
            size_bytes = 0
            if backup_path and os.path.exists(backup_path):
                size_bytes = get_dir_manifest(backup_path).total_bytes
            return ValidationResult(ok=True, size_bytes=size_bytes)
        time.sleep(1)

    return ValidationResult(ok=False, size_bytes=0, reason="timeout")

@flow(
    flow_run_name="archive-strip-{strip_ident}",
    on_completion=[publish_lsm_slice_hook, publish_lsm_project_hook],
    on_failure=[slack_notification_hook],
)
@strip_processing_milestone(field_name="archived", success_event=STRIP_ARCHIVED)
def archive_strip_flow(
    strip_ident: LSMStripId,
    strip_path: str,
    archive_path: str,
    rate_limit_mb: int = 500,
    force_rerun: bool = False,
) -> None:
    """
    Archive a raw strip to the configured archive path, then validate the copy.

    Skips automatically if the strip is already marked as archived unless
    ``force_rerun`` is set.  On success, emits ``STRIP_ARCHIVED``.
    """
    logger = get_run_logger()
    resolved_strip = os.fspath(host_lsm_fs_path(strip_path))
    resolved_archive = os.fspath(host_lsm_fs_path(archive_path))

    backup_path = os.path.join(resolved_archive, Path(resolved_strip).name)
    Path(backup_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Archiving {strip_ident}: {resolved_strip} -> {backup_path}")

    backup_future = archive_strip.submit(
        strip_ident=strip_ident,
        strip_path=resolved_strip,
        output_path=backup_path,
        rate_limit_mb=rate_limit_mb,
        force_rerun=force_rerun,
    )
    check_archive_result.submit(
        strip_ident=strip_ident,
        strip_path=resolved_strip,
        backup_path=backup_path,
        wait_for=[backup_future],
    ).result(raise_on_failure=True)


@flow
def archive_strip_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event-driven wrapper for ``archive_strip_flow``, triggered by ``STRIP_READY``.

    Payload must include ``strip_ident`` (dict) and ``strip_path``.
    The archive destination is read from the project's scan config
    ``archive_path``; the flow is a no-op when that field is unset.
    """
    strip_ident = strip_ident_from_payload(payload)
    if "strip_path" not in payload:
        raise KeyError("payload must include strip_path")
    cfg = load_scan_config_for_payload(strip_ident.project_name, payload)
    if cfg.archive_path is None:
        get_run_logger().warning(
            f"archive_path not configured for {strip_ident.project_name}, skipping archive"
        )
        return
    archive_strip_flow(
        strip_ident=strip_ident,
        strip_path=payload["strip_path"],
        archive_path=os.fspath(cfg.archive_path),
        rate_limit_mb=cfg.archive_rate_limit,
        force_rerun=force_rerun_from_payload(payload),
    )

    if cfg.distribute_archive:
        # Locked read: archived is already True (set inside archive_strip_flow).
        # If compressed is also True, the process flow already finished and
        # handed cleanup responsibility to us. Otherwise, process will handle
        # cleanup when it completes (it will see archived=True in its open_strip block).
        strip_view = LSM_STATE_SERVICE.read_strip(strip_ident=strip_ident)
        if strip_view and strip_view.compressed:
            cleanup_future = run_cleanup_tasks(
                cleanup_action=cfg.strip_cleanup_action,
                strip_ident=strip_ident,
                strip_path=payload["strip_path"],
            )
            if cleanup_future:
                cleanup_future.wait()


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
    concurrent_workers: int = 1,
) -> list:
    """
    Create both deployments:
    - manual ``archive_strip_flow``
    - event-driven ``archive_strip_event_flow`` (triggered by ``STRIP_READY``)
    """
    manual = archive_strip_flow.to_deployment(
        name=deployment_name,
        tags=["lsm", "archive-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
    )
    event = archive_strip_event_flow.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "archive-strip", *list(extra_tags)],
        concurrency_limit=concurrent_workers,
        triggers=[get_event_trigger(STRIP_READY, project_name=project_name)],
    )
    return [manual, event]


if __name__ == "__main__":
    import prefect

    prefect.serve(*to_deployment())
