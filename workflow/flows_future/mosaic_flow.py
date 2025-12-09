"""
Flows for processing a mosaic (coordinate determination and stitching).
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from prefect import flow

from ..tasks.mosaic_processing import (
    collect_tile_aip_images_task,
    determine_tile_coordinates_task,
    save_coordinates_task,
    load_coordinates_task,
    create_mask_from_aip_task,
    stitch_enface_images_task,
    stitch_3d_volumes_task,
    apply_mask_task,
    save_stitched_enface_task,
    save_stitched_volumes_task,
)
from ..tasks.upload import (
    queue_upload_stitched_volumes_task,
)
from ..tasks.notifications import (
    notify_stitched_complete_task,
    monitor_tile_progress_task,
)
from ..tasks.utils import (
    extract_path_from_asset,
    extract_paths_from_assets,
)
from .tile_flow import process_tile_flow

logger = logging.getLogger(__name__)


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
    coordinate_asset = save_coordinates_task(coordinates, str(output_coord_file), mosaic_id)
    coordinate_file = extract_path_from_asset(coordinate_asset)
    
    return coordinate_file


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
    
    # Save stitched outputs (synchronous) - returns MaterializationResult assets
    enface_assets = save_stitched_enface_task(masked_enface, output_base_path, mosaic_id)
    volume_assets = save_stitched_volumes_task(masked_volumes, output_base_path, mosaic_id)
    
    # Extract paths from assets
    enface_paths = extract_paths_from_assets(enface_assets)
    volume_paths = extract_paths_from_assets(volume_assets)
    
    # Queue uploads (async, non-blocking)
    if upload_queue:
        queue_upload_stitched_volumes_task.submit(
            volume_paths,
            f"s3://bucket/stitched/{mosaic_id}/",  # TODO: Get from config
            upload_queue
        )
    
    # Send stitched image to Slack (async, non-blocking)
    if slack_config:
        aip_asset = enface_assets.get("aip")
        if aip_asset:
            aip_path = extract_path_from_asset(aip_asset)
            notify_stitched_complete_task.submit(
                mosaic_id,
                aip_path,
                slack_config
            )
    
    return {
        "enface": masked_enface,
        "volumes": masked_volumes,
        "enface_assets": enface_assets,
        "volume_assets": volume_assets,
        "enface_paths": enface_paths,
        "volume_paths": volume_paths
    }


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

