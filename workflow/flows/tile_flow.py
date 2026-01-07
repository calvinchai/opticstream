"""
Flow for processing a single tile.
"""

import os.path as op
from typing import Any, Dict, Union

from prefect import flow
from prefect.logging import get_run_logger

from workflow.tasks.tile_processing import archive_tile_task
from workflow.tasks.upload import (
    submit_upload_to_linc_task,
)

"sub-I80_voi-slab1_sample-slice{slice_num:03d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii"


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-tile-{tile_index}")
def process_tile_flow(
    project_name: str,
    project_base_path: str,
    tile_path: str,
    mosaic_id: int,
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
    logger = get_run_logger()
    titled_illumination = mosaic_id % 2 == 0
    acq = "tilted" if titled_illumination else "normal"
    slice_id = (mosaic_id + 1) // 2
    archived_tile_name = (
        f"{project_name}_sample-slice-{slice_id:03d}_chunk-"
        f"{tile_index:04d}_acq-{acq}_OCT.nii.gz"
    )
    archived_tile_path = op.join(compressed_base_path, archived_tile_name)

    logger.info(f"Processing tile {tile_index:04d} in mosaic {mosaic_id:03d}")
    archive_tile_future = archive_tile_task.submit(tile_path, archived_tile_path)
    upload_to_linc_future = submit_upload_to_linc_task.submit(
        archived_tile_path, wait_for=[archive_tile_future]
    )

    # intermediate_tile_path_prefix = op.join(intermediate_base_path, f"mosaic_{
    # mosaic_id:03d}_tile_{tile_index:04d}")
    # intermediate_complex_tile_path = f"{intermediate_tile_path_prefix}_complex.nii"

    # spectral_to_complex_task(tile_path, intermediate_complex_tile_path, aline, 350)
    # complex_to_processed_task(intermediate_complex_tile_path,
    # intermediate_tile_path_prefix, surface_method=surface_method, depth=depth)
    upload_to_linc_future.wait()

    # # Load spectral raw (synchronous)
    # spectral_data = load_spectral_file_raw_task(tile_path)
    # archive_future=save_file_with_gz_task.submit(spectral_data, output_base_path,
    # f"mosaic_{mosaic_id}_tile_{tile_index}.nii")
    # upload_to_linc_task.submit(f"{output_base_path}/mosaic_{mosaic_id}_tile_{
    # tile_index}.nii.gz", wait_for=[archive_future])
    # # archive_future = archive_tile_flow(spectral_data, mosaic_id, tile_index,
    # output_base_path, compressed_base_path)
    # spectral_data = load_spectral_data_raw_task(spectral_data, 350, 350, 352)
    # # Convert to complex (in-memory, synchronous)
    # complex_data = spectral_to_complex_task(spectral_data)

    # # Convert to volumes (synchronous)
    # volumes = complex_to_volumes_task(complex_data)
    # save_nifti_task.submit(volumes["dBI"], op.join(intermediate_base_path,
    # f"mosaic_{mosaic_id}_tile_{tile_index}_dBI.nii"))
    # save_nifti_task.submit(volumes["O3D"], op.join(intermediate_base_path,
    # f"mosaic_{mosaic_id}_tile_{tile_index}_O3D.nii"))
    # save_nifti_task.submit(volumes["R3D"], op.join(intermediate_base_path,
    # f"mosaic_{mosaic_id}_tile_{tile_index}_R3D.nii"))
    # volume = EnfaceVolume(dBI3D=volumes["dBI"], O3D=volumes["O3D"], R3D=volumes[
    # "R3D"], surface=surface_method, depth=depth)
    # save_nifti_task.submit(volume.aip, op.join(intermediate_base_path, f"mosaic_{
    # mosaic_id}_tile_{tile_index}_aip.nii"))
    # save_nifti_task.submit(volume.mip, op.join(intermediate_base_path, f"mosaic_{
    # mosaic_id}_tile_{tile_index}_mip.nii"))
    # save_nifti_task.submit(volume.ret, op.join(intermediate_base_path, f"mosaic_{
    # mosaic_id}_tile_{tile_index}_ret.nii"))
    # save_nifti_task.submit(volume.ori, op.join(intermediate_base_path, f"mosaic_{
    # mosaic_id}_tile_{tile_index}_ori.nii"))
    # save_nifti_task.submit(volume.biref, op.join(intermediate_base_path, f"mosaic_{
    # mosaic_id}_tile_{tile_index}_biref.nii"))
    # save_nifti_task.submit(volume.surface, op.join(intermediate_base_path,
    # f"mosaic_{mosaic_id}_tile_{tile_index}_surface.nii"))
    # wait for all tasks to complete
    # asyncio.gather(archive_future)
    # # Save outputs (synchronous, blocking) - returns MaterializationResult assets
    # volume_assets = save_volumes_task(volumes, output_base_path, mosaic_id,
    # tile_index)
    # enface_assets = save_enface_task(enface_images, output_base_path, mosaic_id,
    # tile_index)

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


# def start_project():
# project_name
# project_base_path
# tile_configuration_normal
# tile_configuration_tilted
# scan_resolution = [10,10,2.5]
# wavelength_um = 0.013
# intermediate_path
# archive_path
# dandiset_id
# dandiset_path
