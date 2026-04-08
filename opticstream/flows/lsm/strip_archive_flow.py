import os
import shutil
import subprocess
from typing import Optional

from prefect import get_run_logger, task

from opticstream.state.lsm_project_state import LSMStripId, LSM_STATE_SERVICE
from opticstream.state.state_guards import RunDecision, enter_milestone_stage
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


