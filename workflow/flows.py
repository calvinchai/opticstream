"""
Prefect flow definitions for OCT pipeline workflow.

This module contains all flow definitions following the hierarchy:
- process_experiment_flow (main)
  - process_slice_flow (per slice)
    - process_mosaic_flow (per mosaic)
      - process_tile_flow (per tile)
      - determine_mosaic_coordinates_flow
      - stitch_mosaic_flow
    - register_slice_flow
  - stack_all_slices_flow
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from prefect import flow, task
from prefect.tasks import task_input_hash

from .tasks import (
    # Tile processing tasks
    load_spectral_raw_task,
    spectral_to_complex_task,
    complex_to_volumes_task,
    find_surface_task,
    volumes_to_enface_task,
    save_volumes_task,
    save_enface_task,
    compress_spectral_task,
    queue_upload_spectral_task,
    notify_tile_complete_task,
    # Mosaic coordinate tasks
    collect_tile_aip_images_task,
    determine_tile_coordinates_task,
    save_coordinates_task,
    load_coordinates_task,
    # Mosaic stitching tasks
    create_mask_from_aip_task,
    stitch_enface_images_task,
    stitch_3d_volumes_task,
    apply_mask_task,
    save_stitched_enface_task,
    save_stitched_volumes_task,
    queue_upload_stitched_volumes_task,
    notify_stitched_complete_task,
    monitor_tile_progress_task,
    # Slice registration tasks
    load_normal_mosaic_task,
    load_tilted_mosaic_task,
    register_orientations_task,
    compute_3d_orientation_task,
    save_registered_data_task,
    # Stacking tasks
    collect_slice_data_task,
    stack_2d_images_task,
    stack_3d_volumes_task,
    save_stacked_data_task,
    # Utility tasks
    discover_slices_task,
    notify_slack_task,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Tile Processing Flow
# ============================================================================

@flow(name="process_tile_flow")
def process_tile_flow(
    tile_path: str,
    mosaic_id: str,
    tile_index: int,
    output_base_path: str,
    compressed_base_path: str,
    surface_method: Union[str, int] = "find",
    depth: int = 80,
    upload_queue: Optional[Any] = None,
    slack_config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Process a single tile from spectral raw data to 3D volumes and enface images.
    
    Parameters
    ----------
    tile_path : str
        Path to spectral raw tile file
    mosaic_id : str
        Mosaic identifier (e.g., "mosaic_001")
    tile_index : int
        Tile index within mosaic
    output_base_path : str
        Base path for output files
    compressed_base_path : str
        Base path for compressed files (separate directory/disk)
    surface_method : Union[str, int]
        Surface finding method ("find", constant, or file path)
    depth : int
        Depth below surface for enface window
    upload_queue : Any, optional
        Upload queue manager instance
    slack_config : dict, optional
        Slack notification configuration
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with processed volumes and enface images
    """
    logger.info(f"Processing tile {tile_index} in {mosaic_id}")
    
    # Load spectral raw (synchronous)
    spectral_data = load_spectral_raw_task(tile_path)
    
    # Convert to complex (in-memory, synchronous)
    complex_data = spectral_to_complex_task(spectral_data)
    
    # Convert to volumes (synchronous)
    volumes = complex_to_volumes_task(complex_data)
    
    # Find surface (synchronous)
    surface = find_surface_task(volumes, method=surface_method)
    
    # Generate enface images (synchronous)
    enface_images = volumes_to_enface_task(volumes, surface, depth)
    
    # Save outputs (synchronous, blocking)
    volume_paths = save_volumes_task(volumes, output_base_path, mosaic_id, tile_index)
    enface_paths = save_enface_task(enface_images, output_base_path, mosaic_id, tile_index)
    
    # Async tasks (fire-and-forget, non-blocking)
    # Compress to separate directory
    compressed_future = compress_spectral_task.submit(
        tile_path,
        compressed_base_path,
        mosaic_id,
        tile_index
    )
    
    # Queue for upload (after compression completes)
    if upload_queue:
        queue_upload_spectral_task.submit(
            compressed_future.result(),  # Wait for compression
            f"s3://bucket/compressed/{mosaic_id}/",  # TODO: Get from config
            upload_queue
        )
    
    # Send notification (async, non-blocking)
    if slack_config:
        notify_tile_complete_task.submit(
            mosaic_id,
            tile_index,
            slack_config
        )
    
    return {
        "volumes": volumes,
        "enface": enface_images,
        "surface": surface,
        "volume_paths": volume_paths,
        "enface_paths": enface_paths
    }


# ============================================================================
# Mosaic Coordinate Determination Flow
# ============================================================================

@flow(name="determine_mosaic_coordinates_flow")
def determine_mosaic_coordinates_flow(
    mosaic_id: str,
    tile_paths: List[str],
    output_base_path: str,
    ideal_coord_file: Optional[str] = None
) -> str:
    """
    Determine stitching coordinates for a mosaic after all tiles are processed.
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    tile_paths : List[str]
        List of processed tile paths
    output_base_path : str
        Base path for output files
    ideal_coord_file : str, optional
        Path to ideal coordinate file (if available)
    
    Returns
    -------
    str
        Path to saved coordinate file
    """
    logger.info(f"Determining coordinates for {mosaic_id}")
    
    # Collect all AIP enface images for the mosaic
    aip_paths = collect_tile_aip_images_task(mosaic_id, tile_paths, output_base_path)
    
    # Determine tile coordinates
    coordinates = determine_tile_coordinates_task(mosaic_id, aip_paths, ideal_coord_file)
    
    # Save coordinates
    output_coord_file = Path(output_base_path) / "coordinates" / f"{mosaic_id}_coordinates.yaml"
    coordinate_file = save_coordinates_task(coordinates, str(output_coord_file))
    
    return coordinate_file


# ============================================================================
# Mosaic Stitching Flow
# ============================================================================

@flow(name="stitch_mosaic_flow")
def stitch_mosaic_flow(
    mosaic_id: str,
    coordinate_file: str,
    tile_paths: List[str],
    output_base_path: str,
    mask_threshold: int = 55,
    overlap: int = 50,
    upload_queue: Optional[Any] = None,
    slack_config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Stitch all tiles in a mosaic together using determined coordinates.
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    coordinate_file : str
        Path to coordinate file
    tile_paths : List[str]
        List of processed tile paths
    output_base_path : str
        Base path for output files
    mask_threshold : int
        Threshold for AIP mask creation
    overlap : int
        Overlap between tiles in pixels
    upload_queue : Any, optional
        Upload queue manager instance
    slack_config : dict, optional
        Slack notification configuration
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with stitched enface images and volumes
    """
    logger.info(f"Stitching mosaic {mosaic_id}")
    
    # Load coordinates (synchronous)
    coordinates = load_coordinates_task(coordinate_file)
    
    # Stitch enface images (synchronous)
    stitched_enface = stitch_enface_images_task(tile_paths, coordinates, overlap)
    
    # Stitch 3D volumes (synchronous)
    stitched_volumes = stitch_3d_volumes_task(tile_paths, coordinates, overlap)
    
    # Create mask from stitched AIP (synchronous)
    mask = create_mask_from_aip_task(stitched_enface.get("aip"), mask_threshold)
    
    # Apply mask to all stitched outputs (synchronous)
    masked_enface = apply_mask_task(stitched_enface, mask)
    masked_volumes = apply_mask_task(stitched_volumes, mask)
    
    # Save stitched outputs (synchronous)
    enface_paths = save_stitched_enface_task(masked_enface, output_base_path, mosaic_id)
    volume_paths = save_stitched_volumes_task(masked_volumes, output_base_path, mosaic_id)
    
    # Queue uploads (async, non-blocking)
    if upload_queue:
        queue_upload_stitched_volumes_task.submit(
            volume_paths,
            f"s3://bucket/stitched/{mosaic_id}/",  # TODO: Get from config
            upload_queue
        )
    
    # Send stitched image to Slack (async, non-blocking)
    if slack_config:
        aip_path = enface_paths.get("aip")
        if aip_path:
            notify_stitched_complete_task.submit(
                mosaic_id,
                aip_path,
                slack_config
            )
    
    return {
        "enface": masked_enface,
        "volumes": masked_volumes,
        "enface_paths": enface_paths,
        "volume_paths": volume_paths
    }


# ============================================================================
# Mosaic Processing Flow
# ============================================================================

@flow(name="process_mosaic_flow")
def process_mosaic_flow(
    mosaic_id: str,
    tile_paths: List[str],
    output_base_path: str,
    compressed_base_path: str,
    surface_method: Union[str, int] = "find",
    depth: int = 80,
    mask_threshold: int = 55,
    overlap: int = 50,
    ideal_coord_file: Optional[str] = None,
    upload_queue: Optional[Any] = None,
    slack_config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Process all tiles in a mosaic and stitch them together.
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    tile_paths : List[str]
        List of spectral raw tile paths
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
    ideal_coord_file : str, optional
        Path to ideal coordinate file
    upload_queue : Any, optional
        Upload queue manager instance
    slack_config : dict, optional
        Slack notification configuration
    
    Returns
    -------
    Dict[str, Any]
        Dictionary with stitched mosaic data
    """
    logger.info(f"Processing mosaic {mosaic_id} with {len(tile_paths)} tiles")
    
    total_tiles = len(tile_paths)
    completed_tiles = []
    
    # Process all tiles in parallel
    tile_results = []
    for tile_index, tile_path in enumerate(tile_paths):
        result = process_tile_flow(
            tile_path=tile_path,
            mosaic_id=mosaic_id,
            tile_index=tile_index,
            output_base_path=output_base_path,
            compressed_base_path=compressed_base_path,
            surface_method=surface_method,
            depth=depth,
            upload_queue=upload_queue,
            slack_config=slack_config
        )
        tile_results.append(result)
        completed_tiles.append(str(tile_index))
        
        # Monitor progress and send milestone notifications
        monitor_tile_progress_task(
            mosaic_id=mosaic_id,
            total_tiles=total_tiles,
            completed_tiles=completed_tiles,
            slack_config=slack_config
        )
    
    # Determine coordinates
    coordinate_file = determine_mosaic_coordinates_flow(
        mosaic_id=mosaic_id,
        tile_paths=tile_paths,
        output_base_path=output_base_path,
        ideal_coord_file=ideal_coord_file
    )
    
    # Stitch mosaic
    stitched = stitch_mosaic_flow(
        mosaic_id=mosaic_id,
        coordinate_file=coordinate_file,
        tile_paths=tile_paths,
        output_base_path=output_base_path,
        mask_threshold=mask_threshold,
        overlap=overlap,
        upload_queue=upload_queue,
        slack_config=slack_config
    )
    
    return stitched


# ============================================================================
# Slice Registration Flow
# ============================================================================

@flow(name="register_slice_flow")
def register_slice_flow(
    slice_number: int,
    normal_mosaic_id: str,
    tilted_mosaic_id: str,
    output_base_path: str,
    gamma: float,
    mask_file: Optional[str] = None,
    mask_threshold: int = 55
) -> Dict[str, str]:
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
    Dict[str, str]
        Dictionary with paths to saved registered files
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
    
    # Save registered data
    registered_paths = save_registered_data_task(
        registered_data,
        orientation_data,
        output_base_path,
        slice_number
    )
    
    return registered_paths


# ============================================================================
# Slice Processing Flow
# ============================================================================

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


# ============================================================================
# Final Stacking Flow
# ============================================================================

@flow(name="stack_all_slices_flow")
def stack_all_slices_flow(
    slice_numbers: List[int],
    output_base_path: str
) -> Dict[str, str]:
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
    Dict[str, str]
        Dictionary with paths to saved stacked files
    """
    logger.info(f"Stacking {len(slice_numbers)} slices")
    
    # Collect all slice data paths
    slice_data = collect_slice_data_task(slice_numbers, output_base_path)
    
    # Stack 2D images
    stacked_2d = stack_2d_images_task(slice_data)
    
    # Stack 3D volumes
    stacked_3d = stack_3d_volumes_task(slice_data)
    
    # Save stacked outputs
    stacked_paths = save_stacked_data_task(stacked_2d, stacked_3d, output_base_path)
    
    return stacked_paths


# ============================================================================
# Main Experiment Flow
# ============================================================================

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

