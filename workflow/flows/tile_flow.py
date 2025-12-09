"""
Flow for processing a single tile.
"""

import logging
import os.path as op
from typing import Dict, List, Optional, Any, Union
from prefect import flow
import asyncio

from data_processing.enface.vol2enface import EnfaceVolume

from ..tasks.tile_processing import (
    load_spectral_file_raw_task,
    load_spectral_data_raw_task,
    save_file_with_gz_task,
    save_nifti_task,
    spectral_to_complex_task,
    complex_to_volumes_task,
    find_surface_task,
    volumes_to_enface_task,
    # save_volumes_task,
    # save_enface_task,
)
from ..tasks.upload import (
    upload_to_dandi_task,
    upload_to_linc_task,
    # compress_spectral_task,
    # queue_upload_spectral_task,
)
# from ..tasks.notifications import (
#     notify_tile_complete_task,
# )
# from ..tasks.utils import (
#     extract_path_from_asset,
#     extract_paths_from_assets,
# )

logger = logging.getLogger(__name__)

@flow(name="archive_tile_flow", flow_run_name="archive_mosaic_{mosaic_id}_tile_{tile_index}")
async def archive_tile_flow(
    tile_data: Any,
    mosaic_id: int,
    tile_index: int,
    output_base_path: str,
    compressed_base_path: str,
) -> Dict[str, Any]:
    """
    Archive a single tile.
    """
    slice_id = (mosaic_id+1) // 2
    await save_file_with_gz_task(tile_data, output_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}.nii.gz")
    await upload_to_linc_task(f"{output_base_path}/mosaic_{mosaic_id}_tile_{tile_index}.nii.gz")

@flow(name="process_tile_flow")
def process_tile_flow(
    project_name: str,
    tile_path: str,
    mosaic_id: str,
    tile_index: int,
    output_base_path: str,
    intermediate_base_path: str,
    compressed_base_path: str,
    surface_method: Union[str, int] = "find",
    depth: int = 80,
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
    spectral_data = load_spectral_file_raw_task(tile_path)
    archive_future=save_file_with_gz_task.submit(spectral_data, output_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}.nii")
    upload_to_linc_task.submit(f"{output_base_path}/mosaic_{mosaic_id}_tile_{tile_index}.nii.gz", wait_for=[archive_future])
    # archive_future = archive_tile_flow(spectral_data, mosaic_id, tile_index, output_base_path, compressed_base_path)
    spectral_data = load_spectral_data_raw_task(spectral_data, 350, 350, 352)
    # Convert to complex (in-memory, synchronous)
    complex_data = spectral_to_complex_task(spectral_data)

    # Convert to volumes (synchronous)
    volumes = complex_to_volumes_task(complex_data)
    save_nifti_task.submit(volumes["dBI"], op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_dBI.nii"))
    save_nifti_task.submit(volumes["O3D"], op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_O3D.nii"))
    save_nifti_task.submit(volumes["R3D"], op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_R3D.nii"))
    volume = EnfaceVolume(dBI3D=volumes["dBI"], O3D=volumes["O3D"], R3D=volumes["R3D"], surface=surface_method, depth=depth)
    save_nifti_task.submit(volume.aip, op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_aip.nii"))
    save_nifti_task.submit(volume.mip, op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_mip.nii"))
    save_nifti_task.submit(volume.ret, op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_ret.nii"))
    save_nifti_task.submit(volume.ori, op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_ori.nii"))
    save_nifti_task.submit(volume.biref, op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_biref.nii"))
    save_nifti_task.submit(volume.surface, op.join(intermediate_base_path, f"mosaic_{mosaic_id}_tile_{tile_index}_surface.nii"))
    # wait for all tasks to complete
    # asyncio.gather(archive_future)
    # # Save outputs (synchronous, blocking) - returns MaterializationResult assets
    # volume_assets = save_volumes_task(volumes, output_base_path, mosaic_id, tile_index)
    # enface_assets = save_enface_task(enface_images, output_base_path, mosaic_id, tile_index)

    # # Extract paths from assets for return value
    # volume_paths = extract_paths_from_assets(volume_assets)
    # enface_paths = extract_paths_from_assets(enface_assets)
    
    # # Async tasks (fire-and-forget, non-blocking)
    # # Compress to separate directory
    # compressed_future = compress_spectral_task.submit(
    #     tile_path,
    #     compressed_base_path,
    #     mosaic_id,
    #     tile_index
    # )
    
    
    
    # return {
    #     "volumes": volumes,
    #     "enface": enface_images,
    #     "surface": surface,
    #     "volume_assets": volume_assets,
    #     "enface_assets": enface_assets,
    #     "volume_paths": volume_paths,
    #     "enface_paths": enface_paths
    # }
from prefect.artifacts import Artifact


