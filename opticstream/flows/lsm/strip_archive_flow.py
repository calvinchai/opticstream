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


@task(task_run_name="archive-{strip_ident}", tags=["lsm-data-archive"])
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
    strip_path: str,
    backup_path: str,
    timeout: int,
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
            return ValidationResult(ok=True, size_bytes=0)
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
        force_rerun=force_rerun_from_payload(payload),
    )


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
) -> list:
    """
    Create both deployments:
    - manual ``archive_strip_flow``
    - event-driven ``archive_strip_event_flow`` (triggered by ``STRIP_READY``)
    """
    manual = archive_strip_flow.to_deployment(
        name=deployment_name,
        tags=["lsm", "archive-strip", *list(extra_tags)],
    )
    event = archive_strip_event_flow.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "archive-strip", *list(extra_tags)],
        triggers=[get_event_trigger(STRIP_READY, project_name=project_name)],
    )
    return [manual, event]


if __name__ == "__main__":
    import prefect

    prefect.serve(*to_deployment())
