"""
Flows for processing a slice (two mosaics: normal and tilted).
"""

import logging
from typing import Dict, List, Optional, Any, Union
from prefect import flow

from ..tasks.slice_processing import (
    load_normal_mosaic_task,
    load_tilted_mosaic_task,
    register_orientations_task,
    compute_3d_orientation_task,
    save_registered_data_task,
)
from .mosaic_flow import process_mosaic_flow

logger = logging.getLogger(__name__)


@flow(name="register_slice_flow")
def register_slice_flow(
    slice_number: int,
    normal_mosaic_id: str,
    tilted_mosaic_id: str,
    output_base_path: str,
    gamma: float,
    mask_file: Optional[str] = None,
    mask_threshold: int = 55
) -> Dict[str, Any]:
    """
    Register normal and tilted illumination mosaics to combine orientations.
    
    Parameters
    ----------
    slice_number : int
        Slice number (n)
    normal_mosaic_id : str
        Normal mosaic ID (2n-1)
    tilted_mosaic_id : str
        Tilted mosaic ID (2n)
    output_base_path : str
        Base path for output files
    gamma : float
        Tilt angle parameter
    mask_file : str, optional
        Optional mask file for registration
    mask_threshold : int
        Threshold for mask
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with MaterializationResult assets for registered files
    """
    logger.info(f"Registering slice {slice_number}")
    
    # Load mosaics
    normal_mosaic = load_normal_mosaic_task(normal_mosaic_id, output_base_path)
    tilted_mosaic = load_tilted_mosaic_task(tilted_mosaic_id, output_base_path)
    
    # Register orientations
    registered_data = register_orientations_task(
        normal_mosaic,
        tilted_mosaic,
        gamma,
        mask_file,
        mask_threshold
    )
    
    # Compute 3D orientation
    orientation_data = compute_3d_orientation_task(registered_data)
    
    # Save registered data - returns MaterializationResult assets
    registered_assets = save_registered_data_task(
        registered_data,
        orientation_data,
        output_base_path,
        slice_number
    )
    
    return registered_assets


@flow(name="process_slice_flow")
def process_slice_flow(
    slice_number: int,
    normal_mosaic_id: str,
    tilted_mosaic_id: str,
    normal_tile_paths: List[str],
    tilted_tile_paths: List[str],
    output_base_path: str,
    compressed_base_path: str,
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
    Orchestrate processing of a single slice (both mosaics).
    
    Parameters
    ----------
    slice_number : int
        Slice number
    normal_mosaic_id : str
        Normal mosaic ID
    tilted_mosaic_id : str
        Tilted mosaic ID
    normal_tile_paths : List[str]
        List of normal mosaic tile paths
    tilted_tile_paths : List[str]
        List of tilted mosaic tile paths
    output_base_path : str
        Base path for output files
    compressed_base_path : str
        Base path for compressed files
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
        Dictionary with processed slice data
    """
    logger.info(f"Processing slice {slice_number}")
    
    # Process normal and tilted mosaics in parallel
    normal_result = process_mosaic_flow(
        mosaic_id=normal_mosaic_id,
        tile_paths=normal_tile_paths,
        output_base_path=output_base_path,
        compressed_base_path=compressed_base_path,
        surface_method=surface_method,
        depth=depth,
        mask_threshold=mask_threshold,
        overlap=overlap,
        ideal_coord_file=ideal_coord_file,
        upload_queue=upload_queue,
        slack_config=slack_config
    )
    
    tilted_result = process_mosaic_flow(
        mosaic_id=tilted_mosaic_id,
        tile_paths=tilted_tile_paths,
        output_base_path=output_base_path,
        compressed_base_path=compressed_base_path,
        surface_method=surface_method,
        depth=depth,
        mask_threshold=mask_threshold,
        overlap=overlap,
        ideal_coord_file=ideal_coord_file,
        upload_queue=upload_queue,
        slack_config=slack_config
    )
    
    # Register slice (after both mosaics complete)
    registered_paths = register_slice_flow(
        slice_number=slice_number,
        normal_mosaic_id=normal_mosaic_id,
        tilted_mosaic_id=tilted_mosaic_id,
        output_base_path=output_base_path,
        gamma=gamma,
        mask_threshold=mask_threshold
    )
    
    return {
        "normal_mosaic": normal_result,
        "tilted_mosaic": tilted_result,
        "registered": registered_paths
    }

