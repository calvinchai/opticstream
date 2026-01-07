"""
Batch state flag utilities.

Abstracts batch state flag operations so flows express intent
rather than filesystem mechanics.
"""

from workflow.state.flags import (
    ARCHIVED,
    PROCESSED,
    STARTED,
    get_batch_flag_path_from_project,
)


def mark_batch_started(project_base_path: str, mosaic_id: int, batch_id: int) -> None:
    """
    Mark a batch as started by creating the started flag file.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier
    """
    batch_started_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, STARTED
    )
    batch_started_path.touch()


def mark_batch_archived(project_base_path: str, mosaic_id: int, batch_id: int) -> None:
    """
    Mark a batch as archived by creating the archived flag file.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier
    """
    batch_archived_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, ARCHIVED
    )
    batch_archived_path.touch()


def mark_batch_processed(project_base_path: str, mosaic_id: int, batch_id: int) -> None:
    """
    Mark a batch as processed by creating the processed flag file.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier
    """
    batch_processed_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, PROCESSED
    )
    batch_processed_path.touch()


def is_batch_started(project_base_path: str, mosaic_id: int, batch_id: int) -> bool:
    """
    Check if a batch has been started.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier

    Returns
    -------
    bool
        True if batch has been started
    """
    batch_started_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, STARTED
    )
    return batch_started_path.exists()


def is_batch_archived(project_base_path: str, mosaic_id: int, batch_id: int) -> bool:
    """
    Check if a batch has been archived.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier

    Returns
    -------
    bool
        True if batch has been archived
    """
    batch_archived_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, ARCHIVED
    )
    return batch_archived_path.exists()


def is_batch_processed(project_base_path: str, mosaic_id: int, batch_id: int) -> bool:
    """
    Check if a batch has been processed.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier

    Returns
    -------
    bool
        True if batch has been processed
    """
    batch_processed_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, PROCESSED
    )
    return batch_processed_path.exists()

