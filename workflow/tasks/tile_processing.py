"""
Tasks for processing individual tiles.
"""

import gzip
import logging
import shutil

from prefect import task
from prefect_shell import ShellOperation

logger = logging.getLogger(__name__)


@task(tags=["psoct-data-conversion"])
def spectral_to_complex_task(
    spectral_data_path: str, output_path: str, align_length: int = 200,
    bline_length: int = 350
):
    # /space/megaera/1/users/kchai/code/psoct-renew/spectral2complex
    # /for_redistribution_files_only/run_spectral2complex.sh
    # /autofs/cluster/matlab/R2024b /mnt/sas/I55_spectralraw_slice1zircon[
    # 0]:psoct-renew$ time spectral2complex/for_redistribution_files_only
    # /run_spectral2complex.sh /autofs/cluster/matlab/R2024b
    # /mnt/sas/I55_spectralraw_slice12_20251125/mosaic_015_image_200_spectral_0200
    # .nii 350 350 /space/megaera/3/users/kchai/
    with ShellOperation(
        commands=[
            f"/space/megaera/1/users/kchai/code/psoct-renew/spectral2complex"
            f"/for_redistribution_files_only/run_spectral2complex.sh "
            f"/autofs/cluster/matlab/R2024b '{spectral_data_path}' {align_length} "
            f"{bline_length} '{output_path}'"]
    ) as spectral_to_complex_operation:
        spectral_to_complex_process = spectral_to_complex_operation.trigger()
        spectral_to_complex_process.wait_for_completion()
        output = spectral_to_complex_process.fetch_result()
        logger.info(f"Spectral to complex output: {output}")
    return output


# complex2processed/for_redistribution_files_only/run_complex2processed.sh
# /autofs/cluster/matlab/R2024b /run/media/kc1708/New\
# Volume/oct-archive/000053/rawdata/sub-I55/mosaic_015_image_200_processed_cropped
# .nii /local_mount/space/zircon/5/users/kchai/I55_mosaic_001_tile_200 find 100 2.5
# 0.013 new ""
@task(tags=["psoct-data-conversion"])
def complex_to_processed_task(
    complex_data_path: str, output_path: str, surface_method: str = "find",
    depth: int = 80, wavelength_um: float = 0.013, voxel_size_z_um: float = 2.5
):
    with ShellOperation(
        commands=[
            f"/space/megaera/1/users/kchai/code/psoct-renew/complex2processed"
            f"/for_redistribution_files_only/run_complex2processed.sh "
            f"/autofs/cluster/matlab/R2024b '{complex_data_path}' '{output_path}' "
            f"{surface_method} {depth} {wavelength_um} {voxel_size_z_um} new ''"]
    ) as complex_to_processed_operation:
        complex_to_processed_process = complex_to_processed_operation.trigger()
        complex_to_processed_process.wait_for_completion()
        output = complex_to_processed_process.fetch_result()
        logger.info(f"Complex to processed output: {output}")
    return output


@task(tags=["psoct-data-archive"])
def archive_tile_task(input_path: str, output_path: str):
    """gzip the file"""
    if not output_path.endswith('.gz'):
        output_path += '.gz'
    with gzip.open(output_path, 'wb', compresslevel=3) as f:
        with open(input_path, 'rb') as f_in:
            shutil.copyfileobj(f_in, f)
    logger.info(f"Archived tile {input_path} to {output_path}")

# @task(name="load_spectral_file_raw", cache_key_fn=task_input_hash)
# def load_spectral_file_raw_task(
#     file_path: str
# ):
#     with open(file_path, 'rb') as f:
#         raw = f.read()
#     return raw


# @task(name="load_spectral_data_raw")
# def load_spectral_data_raw_task(
#     raw: bytes,
#     aline_length: int = 200,
#     bline_length: int = 350,
#     nifti_header_size: int = 352
# ):
#     data = np.frombuffer(raw[nifti_header_size:], dtype=np.uint16)
#     data = data.reshape((2048, aline_length, 2, bline_length), order='F')
#     return data


# @task(name="spectral_to_complex")
# def spectral_to_complex_task(spectral_data: Any) -> Any:
#     """
#     Convert spectral raw to complex data (in-memory).

#     Parameters
#     ----------
#     spectral_data : Any
#         Spectral raw data

#     Returns
#     -------
#     Any
#         Complex data (placeholder - implement with actual conversion)
#     """
#     logger.info("Converting spectral raw to complex data")

#     return spectral2complex(spectral_data)


# @task(name="complex_to_volumes")
# def complex_to_volumes_task(complex_data: Any, flip_orientation: bool = False,
# offset: float = 100) -> Dict[str, Any]:
#     """
#     Convert complex to 3D volumes (dBI, O3D, R3D).

#     Parameters
#     ----------
#     complex_data : Any
#         Complex data

#     Returns
#     -------
#     Dict[str, Any]
#         Dictionary with keys: dBI, O3D, R3D
#     """
#     logger.info("Converting complex to 3D volumes")
#     dBI, O3D, R3D = complex2volume(complex_data, flip_orientation=flip_orientation,
#     offset=offset)
#     return {"dBI": dBI, "O3D": O3D, "R3D": R3D}


# @task(name="find_surface")
# def find_surface_task(
#     volumes: Dict[str, Any],
#     method: Union[str, int] = "find"
# ) -> Any:
#     """
#     Surface finding algorithm for enface conversion.

#     Parameters
#     ----------
#     volumes : Dict[str, Any]
#         Dictionary with volume data (dBI, O3D, R3D)
#     method : Union[str, int]
#         Surface finding method ("find", constant, or file path)

#     Returns
#     -------
#     Any
#         Surface data (placeholder - implement with actual algorithm)
#     """
#     logger.info(f"Finding surface using method: {method}")
#     # TODO: Implement actual surface finding
#     # from oct_pipe.enface import find_surface
#     # return find_surface(volumes["dBI"], method=method)
#     return {"surface": None}  # Placeholder


# @task(name="volumes_to_enface")
# def volumes_to_enface_task(
#     volumes: Dict[str, Any],
#     surface: Any,
#     depth: int = 80
# ) -> Dict[str, Any]:
#     """
#     Generate enface images from volumes.

#     Parameters
#     ----------
#     volumes : Dict[str, Any]
#         Dictionary with volume data (dBI, O3D, R3D)
#     surface : Any
#         Surface data
#     depth : int
#         Depth below surface for enface window

#     Returns
#     -------
#     Dict[str, Any]
#         Dictionary with keys: aip, mip, orientation, retardance, birefringence
#     """
#     logger.info(f"Generating enface images with depth={depth}")
#     # TODO: Implement actual enface generation
#     # from oct_pipe.enface.vol2enface import EnfaceVolume
#     # enface = EnfaceVolume(
#     #     volumes["dBI"],
#     #     volumes["R3D"],
#     #     volumes["O3D"],
#     #     surface=surface,
#     #     depth=depth
#     # )
#     # return {
#     #     "aip": enface.aip,
#     #     "mip": enface.mip,
#     #     "orientation": enface.orientation,
#     #     "retardance": enface.retardance,
#     #     "birefringence": enface.birefringence
#     # }
#     return {
#         "aip": None,
#         "mip": None,
#         "orientation": None,
#         "retardance": None,
#         "birefringence": None
#     }  # Placeholder

#
# @task(name="save_volumes")
# def save_volumes_task(
#     volumes: Dict[str, Any],
#     output_base_path: str,
#     mosaic_id: str,
#     tile_index: int
# ) -> Dict[str, Any]:
#     """
#     Save 3D volumes to disk as Prefect assets.
#
#     Parameters
#     ----------
#     volumes : Dict[str, Any]
#         Dictionary with volume data (dBI, O3D, R3D)
#     output_base_path : str
#         Base path for output files
#     mosaic_id : str
#         Mosaic identifier
#     tile_index : int
#         Tile index within mosaic
#
#     Returns
#     -------
#     Dict[str, MaterializationResult]
#         Dictionary with MaterializationResult assets for each volume file
#     """
#     logger.info(f"Saving volumes for {mosaic_id} tile {tile_index}")
#
#     output_dir = Path(output_base_path) / "processed"
#     output_dir.mkdir(parents=True, exist_ok=True)
#
#     volume_assets = {}
#     for modality in ["dBI", "O3D", "R3D"]:
#         output_path = output_dir / f"{mosaic_id}_tile_{tile_index}_{modality}.nii"
#         # TODO: Implement actual saving
#         # import nibabel as nib
#         # nib.save(nib.Nifti1Image(volumes[modality], affine), output_path)
#
#         # Create asset with unique key
#         asset_key = f"{mosaic_id}_tile_{tile_index}_{modality.lower()}"
#         asset = MaterializationResult(
#             asset_key=asset_key,
#             description=f"{modality} volume for {mosaic_id} tile {tile_index}",
#             metadata={"path": str(output_path), "modality": modality, "mosaic_id":
#             mosaic_id, "tile_index": tile_index}
#         )
#         volume_assets[modality] = asset
#         logger.debug(f"Created asset {asset_key} for {output_path}")
#
#     return volume_assets
#
#
# @task(name="save_enface")
# def save_enface_task(
#     enface_images: Dict[str, Any],
#     output_base_path: str,
#     mosaic_id: str,
#     tile_index: int
# ) -> Dict[str, MaterializationResult]:
#     """
#     Save enface images to disk as Prefect assets.
#
#     Parameters
#     ----------
#     enface_images : Dict[str, Any]
#         Dictionary with enface images (aip, mip, orientation, retardance,
#         birefringence)
#     output_base_path : str
#         Base path for output files
#     mosaic_id : str
#         Mosaic identifier
#     tile_index : int
#         Tile index within mosaic
#
#     Returns
#     -------
#     Dict[str, MaterializationResult]
#         Dictionary with MaterializationResult assets for each enface file
#     """
#     logger.info(f"Saving enface images for {mosaic_id} tile {tile_index}")
#
#     output_dir = Path(output_base_path) / "processed"
#     output_dir.mkdir(parents=True, exist_ok=True)
#
#     enface_assets = {}
#     for modality in ["aip", "mip", "orientation", "retardance", "birefringence"]:
#         output_path = output_dir / f"{mosaic_id}_tile_{tile_index}_{modality}.nii"
#         # TODO: Implement actual saving
#         # import nibabel as nib
#         # nib.save(nib.Nifti1Image(enface_images[modality], affine), output_path)
#
#         # Create asset with unique key
#         asset_key = f"{mosaic_id}_tile_{tile_index}_{modality}"
#         asset = MaterializationResult(
#             asset_key=asset_key,
#             description=f"{modality.upper()} enface image for {mosaic_id} tile {
#             tile_index}",
#             metadata={"path": str(output_path), "modality": modality, "mosaic_id":
#             mosaic_id, "tile_index": tile_index}
#         )
#         enface_assets[modality] = asset
#         logger.debug(f"Created asset {asset_key} for {output_path}")
#
#     return enface_assets


# @task(name="save_file_with_gz")
# def save_file_with_gz_task(file_content: bytes, output_dir: str, output_file_name:
# str):
#     """
#     Save the file to another directory with .gz postfix using gzip directly.

#     Parameters
#     ----------
#     file_content : bytes
#         Original file content to compress
#     output_dir : str
#         Output directory path
#     output_file_name : str
#         Output file name
#     """
#     task_logger = get_run_logger()
#     # Add .gz extension
#     output_filename = output_file_name + '.gz'
#     output_path = os.path.join(output_dir, output_filename)

#     task_logger.info(f"Compressing {output_file_name} to {output_path}")

#     # Compress file using gzip directly
#     with gzip.open(output_path, 'wb') as f:
#         f.write(file_content)

#     task_logger.info(f"Successfully saved compressed file: {output_path}")
#     return output_path


# @task
# def dandi_upload_task(file_path: str):
#     """
#     Upload the file to DANDI.

#     Parameters
#     ----------
#     file_path : str
#         Path to the file to upload
#     """
#     task_logger = get_run_logger()
#     task_logger.info(f"Uploading {file_path} to DANDI")

# @task
# def save_nifti_task(data: Any, output_path: str):
#     """
#     Save the data to a NIfTI file.

#     Parameters
#     ----------
#     data : Any
#         Data to save
#     output_path : str
#         Path to the output file
#     """
#     task_logger = get_run_logger()
#     task_logger.info(f"Saving {output_path}")
#     nib.save(nib.Nifti1Image(data, np.eye(4)), output_path)
#     task_logger.info(f"Successfully saved {output_path}")
#     return output_path
