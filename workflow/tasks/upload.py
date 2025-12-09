"""
Tasks for uploading files to cloud storage.
"""

import os
import gzip
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from prefect import task
# from prefect.results import MaterializationResult

logger = logging.getLogger(__name__)

from prefect_shell import ShellOperation, shell_run_command

@task(name="upload_to_dandi", tags=["dandi-upload"], retries=1)
def upload_to_dandi_task(file_path: str) -> None:
    """
    Upload the file to DANDI.
    """
    shell_run_command(f"conda run -n dandi dandi upload {file_path} -J 10:10")

@task(name="upload_to_linc", tags=["dandi-upload"], retries=1)
def upload_to_linc_task(file_path: str) -> None:
    """
    Upload the file to LINC.
    """
    with ShellOperation(
        commands=[f"/autofs/space/aspasia_002/users/code/miniforge3/envs/dandi-linc/bin/dandi upload -i linc '{file_path}' -J 10:10 --allow-any-path"],
        env={
            "LINC_API_KEY": "6703dac4c9afc50c9c7c4ca5544546fa5193bbaa",
            "DANDI_DEVEL": "1"
        },
        working_dir=os.path.dirname(file_path)
    ) as upload_operation:
        upload_process = upload_operation.trigger()
        upload_process.wait_for_completion()
        output = upload_process.fetch_result()
        logger.info(f"Upload output: {output}")
    # shell_run_command(f"/autofs/space/aspasia_002/users/code/miniforge3/envs/dandi-linc/bin/dandi upload -i linc {file_path} -J 10:10")


#
# @task(name="compress_spectral", allow_failure=True)
# def compress_spectral_task(
#     tile_path: str,
#     compressed_base_path: str,
#     mosaic_id: str,
#     tile_index: int
# ) -> MaterializationResult:
#     """
#     Compress spectral raw file to separate directory as Prefect asset (async, fire-and-forget).
#
#     Parameters
#     ----------
#     tile_path : str
#         Path to spectral raw tile file
#     compressed_base_path : str
#         Base path for compressed files (separate directory/disk)
#     mosaic_id : str
#         Mosaic identifier
#     tile_index : int
#         Tile index within mosaic
#
#     Returns
#     -------
#     MaterializationResult
#         Asset for the compressed file
#     """
#     logger.info(f"Compressing spectral raw for {mosaic_id} tile {tile_index}")
#
#     # Create output directory if it doesn't exist
#     os.makedirs(compressed_base_path, exist_ok=True)
#
#     # Output path
#     output_filename = f"{mosaic_id}_tile_{tile_index}_spectral.nii.gz"
#     output_path = os.path.join(compressed_base_path, output_filename)
#
#     # Compress file
#     try:
#         with open(tile_path, 'rb') as f_in:
#             with gzip.open(output_path, 'wb') as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         logger.info(f"Compressed to {output_path}")
#
#         # Create asset
#         asset_key = f"{mosaic_id}_tile_{tile_index}_spectral_compressed"
#         asset = MaterializationResult(
#             asset_key=asset_key,
#             description=f"Compressed spectral raw for {mosaic_id} tile {tile_index}",
#             metadata={"path": output_path, "mosaic_id": mosaic_id, "tile_index": tile_index, "type": "compressed"}
#         )
#         return asset
#     except Exception as e:
#         logger.error(f"Compression failed: {e}")
#         raise

#
# @task(name="queue_upload_spectral", allow_failure=True)
# def queue_upload_spectral_task(
#     compressed_file_path: str,
#     destination: str,
#     upload_queue: Any
# ) -> None:
#     """
#     Queue compressed file for cloud upload (async, non-blocking).
#
#     Parameters
#     ----------
#     compressed_file_path : str
#         Path to compressed file
#     destination : str
#         Destination path/URL for upload
#     upload_queue : Any
#         Upload queue manager instance
#     """
#     logger.info(f"Queueing upload: {compressed_file_path} -> {destination}")
#     upload_queue.enqueue(compressed_file_path, destination)
#
#
# @task(name="queue_upload_stitched_volumes", allow_failure=True)
# def queue_upload_stitched_volumes_task(
#     volume_paths: Dict[str, str],
#     destination_base: str,
#     upload_queue: Any
# ) -> None:
#     """
#     Queue stitched volumes for cloud upload (async, non-blocking).
#
#     Parameters
#     ----------
#     volume_paths : Dict[str, str]
#         Dictionary with paths to stitched volume files
#     destination_base : str
#         Base destination path/URL
#     upload_queue : Any
#         Upload queue manager instance
#     """
#     logger.info(f"Queueing uploads for {len(volume_paths)} volumes")
#     for modality, path in volume_paths.items():
#         destination = f"{destination_base}/{os.path.basename(path)}"
#         upload_queue.enqueue(path, destination)

