"""
Flow for handling uploads to cloud storage.

This flow can be used to orchestrate upload operations if needed.
Currently, uploads are primarily handled as async tasks within other flows,
but this flow provides a centralized way to manage upload workflows.
"""

import logging
from typing import Dict, List, Optional, Any
from prefect import flow

from ..tasks.upload import (
    queue_upload_spectral_task,
    queue_upload_stitched_volumes_task,
)

logger = logging.getLogger(__name__)


@flow(name="upload_files_flow")
def upload_files_flow(
    file_paths: List[str],
    destinations: List[str],
    upload_queue: Any
) -> Dict[str, bool]:
    """
    Upload multiple files to cloud storage.
    
    Parameters
    ----------
    file_paths : List[str]
        List of file paths to upload
    destinations : List[str]
        List of destination paths/URLs
    upload_queue : Any
        Upload queue manager instance
    
    Returns
    -------
    Dict[str, bool]
        Dictionary mapping file paths to upload success status
    """
    logger.info(f"Uploading {len(file_paths)} files")
    
    if len(file_paths) != len(destinations):
        raise ValueError("file_paths and destinations must have the same length")
    
    results = {}
    for file_path, destination in zip(file_paths, destinations):
        try:
            queue_upload_spectral_task.submit(
                file_path,
                destination,
                upload_queue
            )
            results[file_path] = True
        except Exception as e:
            logger.error(f"Failed to queue upload for {file_path}: {e}")
            results[file_path] = False
    
    return results

