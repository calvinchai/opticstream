"""
Utility functions and tasks used across multiple flows.
"""

from pathlib import Path
from typing import List, Optional, Tuple

from prefect import get_run_logger, task


def mosaic_id_to_slice_number(mosaic_id: int) -> int:
    """
    Convert mosaic ID to slice number.
    
    Mapping:
    - Mosaic 1 (normal) → Slice 1
    - Mosaic 2 (tilted) → Slice 1
    - Mosaic 3 (normal) → Slice 2
    - Mosaic 4 (tilted) → Slice 2
    - etc.
    
    Parameters
    ----------
    mosaic_id : int
        Mosaic identifier
        
    Returns
    -------
    int
        Slice number (1-indexed)
    """
    # Normal mosaics: 1, 3, 5, ... → slices 1, 2, 3, ...
    # Tilted mosaics: 2, 4, 6, ... → slices 1, 2, 3, ...
    if mosaic_id % 2 == 0:
        # Tilted (even): 2→1, 4→2, 6→3, ...
        return mosaic_id // 2
    else:
        # Normal (odd): 1→1, 3→2, 5→3, ...
        return (mosaic_id + 1) // 2


def get_slice_paths(project_base_path: str, slice_number: int) -> Tuple[
    Path, Path, Path, Path]:
    """
    Get standard paths for a slice directory structure.
    
    Structure:
    - {project_base_path}/slice-{slice_number:02d}/processed/
    - {project_base_path}/slice-{slice_number:02d}/stitched/
    - {project_base_path}/slice-{slice_number:02d}/complex/
    - {project_base_path}/slice-{slice_number:02d}/state/
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
        
    Returns
    -------
    Tuple[Path, Path, Path, Path]
        Tuple of (processed_path, stitched_path, complex_path, state_path)
    """
    slice_path = Path(project_base_path) / f"slice-{slice_number:02d}"
    processed_path = slice_path / "processed/"
    stitched_path = slice_path / "stitched/"
    complex_path = slice_path / "complex/"
    state_path = slice_path / "state/"

    return processed_path, stitched_path, complex_path, state_path


@task(name="discover_slices")
def discover_slices_task(
    data_root_path: str,
    slice_numbers: Optional[List[int]] = None
) -> List[int]:
    """
    Discover available slices from data directory.
    
    Parameters
    ----------
    data_root_path : str
        Root path to data directory
    slice_numbers : List[int], optional
        Optional list of slice numbers (if None, auto-discover)
    
    Returns
    -------
    List[int]
        List of slice numbers
    """
    logger = get_run_logger()
    if slice_numbers is not None:
        return slice_numbers

    logger.info(f"Discovering slices in {data_root_path}")

    data_path = Path(data_root_path)
    slices = []

    # TODO: Implement actual discovery logic
    # This would scan the directory structure to find available slices
    # For now, return empty list as placeholder

    return slices


def get_mosaic_paths(project_base_path: str, mosaic_id: int) -> Tuple[
    Path, Path, Path, Path]:
    """
    Get standard paths for a mosaic using slice-based structure.
    
    This function converts mosaic_id to slice_number and returns the
    appropriate paths in the slice-based directory structure.
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
        
    Returns
    -------
    Tuple[Path, Path, Path, Path]
        Tuple of (processed_path, stitched_path, complex_path, state_path)
    """
    slice_number = mosaic_id_to_slice_number(mosaic_id)
    return get_slice_paths(project_base_path, slice_number)


def get_illumination(mosaic_id: int) -> str:
    """
    Get illumination type for a mosaic.
    
    Parameters
    ----------
    mosaic_id : int
        Mosaic identifier
        
    Returns
    -------
    str
        Illumination type ("normal" or "tilted")
    """
    return "tilted" if mosaic_id % 2 == 0 else "normal"