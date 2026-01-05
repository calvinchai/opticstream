"""
Flag file definitions and helper functions for state management.

This module provides centralized flag naming conventions and helper functions
for constructing flag file paths. Flag files serve as the authoritative source
of truth for processing state at batch, mosaic, and slice levels.
"""

from pathlib import Path
from typing import Union

from workflow.tasks.utils import get_mosaic_paths, get_slice_paths


# Flag name constants
# Batch-level flags
STARTED = "started"
ARCHIVED = "archived"
PROCESSED = "processed"
UPLOADED = "uploaded"

# Mosaic-level flags
STITCHED = "stitched"
VOLUME_STITCHED = "volume_stitched"
VOLUME_UPLOADED = "volume_uploaded"

# Slice-level flags
REGISTERED = "registered"


def get_batch_flag_path(state_path: Path, batch_id: int, flag_name: str) -> Path:
    """
    Get the path to a batch-level flag file.

    Parameters
    ----------
    state_path : Path
        Path to the state directory (e.g., mosaic-001/state/)
    batch_id : int
        Batch identifier
    flag_name : str
        Flag name (e.g., "started", "archived", "processed", "uploaded")

    Returns
    -------
    Path
        Path to the flag file (e.g., state_path / "batch-001.started")
    """
    return state_path / f"batch-{batch_id:03d}.{flag_name}"


def get_mosaic_flag_path(state_path: Path, mosaic_id: int, flag_name: str) -> Path:
    """
    Get the path to a mosaic-level flag file.

    Parameters
    ----------
    state_path : Path
        Path to the state directory (e.g., mosaic-001/state/)
    mosaic_id : int
        Mosaic identifier
    flag_name : str
        Flag name (e.g., "started", "stitched", "volume_stitched", "volume_uploaded")

    Returns
    -------
    Path
        Path to the flag file (e.g., state_path / "mosaic-001.started")
    """
    return state_path / f"mosaic-{mosaic_id:03d}.{flag_name}"


def get_slice_flag_path(state_path: Path, slice_number: int, flag_name: str) -> Path:
    """
    Get the path to a slice-level flag file.

    Parameters
    ----------
    state_path : Path
        Path to the state directory (e.g., slice-01/state/)
    slice_number : int
        Slice number (1-indexed)
    flag_name : str
        Flag name (e.g., "started", "registered", "uploaded")

    Returns
    -------
    Path
        Path to the flag file (e.g., state_path / "slice-01.started")
    """
    return state_path / f"slice-{slice_number:02d}.{flag_name}"


def get_batch_flag_path_from_project(
    project_base_path: Union[str, Path], mosaic_id: int, batch_id: int, flag_name: str
) -> Path:
    """
    Get the path to a batch-level flag file from project parameters.

    Convenience function that constructs the state path and flag path in one call.

    Parameters
    ----------
    project_base_path : str or Path
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier
    flag_name : str
        Flag name (e.g., "started", "archived", "processed", "uploaded")

    Returns
    -------
    Path
        Path to the flag file (e.g., {project_base_path}/mosaic-001/state/batch-001.started)
    """
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    return get_batch_flag_path(state_path, batch_id, flag_name)


def get_mosaic_flag_path_from_project(
    project_base_path: Union[str, Path], mosaic_id: int, flag_name: str
) -> Path:
    """
    Get the path to a mosaic-level flag file from project parameters.

    Convenience function that constructs the state path and flag path in one call.

    Parameters
    ----------
    project_base_path : str or Path
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    flag_name : str
        Flag name (e.g., "started", "stitched", "volume_stitched", "volume_uploaded")

    Returns
    -------
    Path
        Path to the flag file (e.g., {project_base_path}/mosaic-001/state/mosaic-001.started)
    """
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    return get_mosaic_flag_path(state_path, mosaic_id, flag_name)


def get_slice_flag_path_from_project(
    project_base_path: Union[str, Path], slice_number: int, flag_name: str
) -> Path:
    """
    Get the path to a slice-level flag file from project parameters.

    Convenience function that constructs the state path and flag path in one call.

    Parameters
    ----------
    project_base_path : str or Path
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
    flag_name : str
        Flag name (e.g., "started", "registered", "uploaded")

    Returns
    -------
    Path
        Path to the flag file (e.g., {project_base_path}/slice-01/state/slice-01.started)
    """
    _, _, _, state_path = get_slice_paths(project_base_path, slice_number)
    return get_slice_flag_path(state_path, slice_number, flag_name)
