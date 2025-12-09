"""
Main experiment flow and stacking flow.
"""

import logging
from typing import Dict, List, Optional, Any, Union
from prefect import flow

from ..tasks.stacking import (
    collect_slice_data_task,
    stack_2d_images_task,
    stack_3d_volumes_task,
    save_stacked_data_task,
)
from ..tasks.utils import (
    discover_slices_task,
)
from .slice_flow import process_slice_flow

logger = logging.getLogger(__name__)


@flow(name="stack_all_slices_flow")
def stack_all_slices_flow(
    slice_numbers: List[int],
    output_base_path: str
) -> Dict[str, Any]:
    """
    Stack all processed slices together.
    
    Parameters
    ----------
    slice_numbers : List[int]
        List of all slice numbers
    output_base_path : str
        Base path for outputs
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with MaterializationResult assets for stacked files
    """
    logger.info(f"Stacking {len(slice_numbers)} slices")
    
    # Collect all slice data paths
    slice_data = collect_slice_data_task(slice_numbers, output_base_path)
    
    # Stack 2D images
    stacked_2d = stack_2d_images_task(slice_data)
    
    # Stack 3D volumes
    stacked_3d = stack_3d_volumes_task(slice_data)
    
    # Save stacked outputs - returns MaterializationResult assets
    stacked_assets = save_stacked_data_task(stacked_2d, stacked_3d, output_base_path)
    
    return stacked_assets


@flow(name="process_experiment_flow")
def process_experiment_flow(
    data_root_path: str,
    output_base_path: str,
    compressed_base_path: str,
    slice_numbers: Optional[List[int]] = None,
    surface_method: Union[str, int] = "find",
    depth: int = 80,
    mask_threshold: int = 55,
    overlap: int = 50,
    gamma: float = 0.0,
    ideal_coord_file: Optional[str] = None,
    upload_queue: Optional[Any] = None,
    slack_config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Orchestrate entire experiment processing.
    
    Parameters
    ----------
    data_root_path : str
        Root path to data directory
    output_base_path : str
        Base path for outputs
    compressed_base_path : str
        Base path for compressed files
    slice_numbers : List[int], optional
        Optional list of slice numbers (if None, auto-discover)
    surface_method : Union[str, int]
        Surface finding method
    depth : int
        Depth below surface for enface window
    mask_threshold : int
        Threshold for AIP mask creation
    overlap : int
        Overlap between tiles in pixels
    gamma : float
        Tilt angle parameter for registration
    ideal_coord_file : str, optional
        Path to ideal coordinate file
    upload_queue : Any, optional
        Upload queue manager instance
    slack_config : dict, optional
        Slack notification configuration
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with processing results
    """
    logger.info(f"Processing experiment from {data_root_path}")
    
    # Discover available slices
    slices = discover_slices_task(data_root_path, slice_numbers)
    
    if not slices:
        logger.warning("No slices found to process")
        return {"slices": [], "stacked": {}}
    
    logger.info(f"Found {len(slices)} slices to process")
    
    # Process all slices in parallel
    slice_results = []
    for slice_num in slices:
        # Calculate mosaic IDs
        normal_mosaic_id = f"mosaic_{2*slice_num - 1:03d}"
        tilted_mosaic_id = f"mosaic_{2*slice_num:03d}"
        
        # TODO: Discover tile paths for each mosaic
        # For now, using placeholder paths
        normal_tile_paths = []  # TODO: Discover from data_root_path
        tilted_tile_paths = []  # TODO: Discover from data_root_path
        
        result = process_slice_flow(
            slice_number=slice_num,
            normal_mosaic_id=normal_mosaic_id,
            tilted_mosaic_id=tilted_mosaic_id,
            normal_tile_paths=normal_tile_paths,
            tilted_tile_paths=tilted_tile_paths,
            output_base_path=output_base_path,
            compressed_base_path=compressed_base_path,
            surface_method=surface_method,
            depth=depth,
            mask_threshold=mask_threshold,
            overlap=overlap,
            gamma=gamma,
            ideal_coord_file=ideal_coord_file,
            upload_queue=upload_queue,
            slack_config=slack_config
        )
        slice_results.append(result)
    
    # Stack all slices (after all slices complete)
    stacked_paths = stack_all_slices_flow(slices, output_base_path)
    
    return {
        "slices": slice_results,
        "stacked": stacked_paths
    }

