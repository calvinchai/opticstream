import os
import os.path as op
import shutil
from typing import Optional

from prefect import get_run_logger, task
from prefect.futures import PrefectFuture

from opticstream.config.lsm_scan_config import StripCleanupAction
from opticstream.state.lsm_project_state import LSMStripId


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
    os.makedirs(strip_path, exist_ok=True)
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
    os.makedirs(strip_path, exist_ok=True)


def describe_cleanup(cleanup_action: StripCleanupAction) -> str:
    """
    Describe what will happen to the raw strip folder based on configuration.
    """
    if cleanup_action == StripCleanupAction.DELETE:
        return "raw strip folder will be deleted"
    if cleanup_action == StripCleanupAction.RENAME:
        return "raw strip folder will be renamed into processed/"
    return "raw strip folder will be kept in place"


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

