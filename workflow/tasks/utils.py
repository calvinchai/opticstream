"""
Utility functions and tasks used across multiple flows.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from prefect import task
from prefect.results import MaterializationResult

logger = logging.getLogger(__name__)


def extract_path_from_asset(asset: MaterializationResult) -> str:
    """
    Extract file path from MaterializationResult asset.
    
    Parameters
    ----------
    asset : MaterializationResult
        Asset containing file metadata
    
    Returns
    -------
    str
        File path from asset metadata
    """
    return asset.metadata.get("path", "")


def extract_paths_from_assets(assets: Dict[str, MaterializationResult]) -> Dict[str, str]:
    """
    Extract file paths from dictionary of MaterializationResult assets.
    
    Parameters
    ----------
    assets : Dict[str, MaterializationResult]
        Dictionary of assets
    
    Returns
    -------
    Dict[str, str]
        Dictionary of file paths
    """
    return {k: extract_path_from_asset(v) for k, v in assets.items()}


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
    if slice_numbers is not None:
        return slice_numbers
    
    logger.info(f"Discovering slices in {data_root_path}")
    
    data_path = Path(data_root_path)
    slices = []
    
    # TODO: Implement actual discovery logic
    # This would scan the directory structure to find available slices
    # For now, return empty list as placeholder
    
    return slices

