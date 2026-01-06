"""
Batch Detection Control Flow

This flow runs on an interval schedule to detect spectral files, organize them into batches,
and emit BATCH_READY events for downstream processing.

The flow:
1. Scans configured folders for spectral files
2. Groups files by mosaic_id and organizes into batches
3. Emits BATCH_READY events for complete batches
4. Tracks emitted batches to avoid duplicates
5. Uses PSOCTScanConfig block for project configuration
"""

import os
import glob
import json
import subprocess
import asyncio
from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List, Set, Tuple

from prefect import flow, get_client
from prefect.events import emit_event
from prefect.logging import get_run_logger
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterState
from prefect.states import StateType

from workflow.config.project_config import get_project_config_block
from workflow.events import BATCH_READY


async def wait_for_tile_batch_flow_runs(flow_name: str = "process_tile_batch_flow", check_interval: int = 10):
    """
    Wait for any running flow runs of the specified flow to finish.
    
    Args:
        flow_name: Name of the flow to check for running runs
        check_interval: Number of seconds to wait between checks
    
    Returns:
        Number of flow runs that were waited for
    """
    logger = get_run_logger()
    
    async with get_client() as client:
        # Filter for running flow runs
        filters = FlowRunFilter(
            flow={"name": {"any_": [flow_name]}},
            state=FlowRunFilterState(
                type={"any_": [StateType.RUNNING, StateType.PENDING, StateType.SCHEDULED]}
            )
        )
        
        runs = await client.read_flow_runs(flow_run_filter=filters)
        
        if not runs:
            logger.info(f"No running flow runs found for {flow_name}")
            return 0
        
        logger.info(f"Found {len(runs)} running flow run(s) for {flow_name}. Waiting for them to finish...")
        
        # Wait until all runs are finished
        while True:
            running_runs = []
            for run in runs:
                # Refresh the run state
                updated_run = await client.read_flow_run(run.id)
                if updated_run.state_type in [StateType.RUNNING, StateType.PENDING, StateType.SCHEDULED]:
                    running_runs.append(updated_run)
            
            if not running_runs:
                logger.info(f"All flow runs for {flow_name} have finished")
                break
            
            logger.info(f"Still waiting for {len(running_runs)} flow run(s) to finish...")
            await asyncio.sleep(check_interval)
        
        return len(runs)


def wait_for_tile_batch_flow_runs_sync(flow_name: str = "process_tile_batch_flow", check_interval: int = 10):
    """
    Synchronous wrapper for wait_for_tile_batch_flow_runs.
    
    Args:
        flow_name: Name of the flow to check for running runs
        check_interval: Number of seconds to wait between checks
    
    Returns:
        Number of flow runs that were waited for
    """
    return asyncio.run(wait_for_tile_batch_flow_runs(flow_name, check_interval))


def refresh_disk():
    """
    Refresh disk mount by unmounting and remounting sas2.
    This ensures files are accessible before scanning.
    """
    try:
        subprocess.run(['bash', '/usr/etc/sas2_unmount'], check=True)
        subprocess.run(['bash', '/usr/etc/sas2_mount'], check=True)
    except subprocess.CalledProcessError as e:
        logger = get_run_logger()
        logger.warning(f"Error refreshing disk: {e}. Continuing anyway.")


def is_file_readable(file_path: str) -> bool:
    """
    Check if a file exists and is readable.
    
    Args:
        file_path: Path to the file
    
    Returns:
        True if file exists and is readable, False otherwise
    """
    if not os.path.exists(file_path):
        return False
    return os.access(file_path, os.R_OK)


def load_emitted_batches(state_file_path: str) -> Set[Tuple[int, int]]:
    """
    Load previously emitted batches from persistent storage.
    
    Args:
        state_file_path: Path to the state JSON file
    
    Returns:
        Set of (mosaic_id, batch_id_for_tile) tuples
    """
    if not os.path.exists(state_file_path):
        return set()
    
    try:
        with open(state_file_path, 'r') as f:
            data = json.load(f)
            # Convert list of lists to set of tuples
            return {tuple(item) for item in data.get('emitted_batches', [])}
    except (json.JSONDecodeError, IOError) as e:
        logger = get_run_logger()
        logger.warning(f"Error loading state file {state_file_path}: {e}. Starting with empty state.")
        return set()


def save_emitted_batches(state_file_path: str, emitted_batches: Set[Tuple[int, int]]):
    """
    Save emitted batches to persistent storage.
    
    Args:
        state_file_path: Path to the state JSON file
        emitted_batches: Set of (mosaic_id, batch_id_for_tile) tuples
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
    
    # Convert set of tuples to list of lists for JSON serialization
    data = {
        'emitted_batches': [list(item) for item in emitted_batches]
    }
    
    try:
        with open(state_file_path, 'w') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        logger = get_run_logger()
        logger.error(f"Error saving state file {state_file_path}: {e}")


def process_folder(
    folder_path: str,
    project_name: str,
    project_base_path: str,
    mosaic_ranges: List[Tuple[int, int, int]],
    batch_id_counter: int,
    emitted_batches: Set[Tuple[int, int]],
) -> Tuple[int, Set[Tuple[int, int]]]:
    """
    Process a folder for specified mosaic ranges with their batch sizes.
    
    Args:
        folder_path: Path to the folder containing spectral files
        project_name: Project name
        project_base_path: Base path for the project
        mosaic_ranges: List of tuples (min_mosaic_id, max_mosaic_id, batch_size)
        batch_id_counter: Starting batch_id counter
        emitted_batches: Set of (mosaic_id, batch_id_for_tile) tuples that have been emitted
    
    Returns:
        Tuple of (updated batch_id_counter, newly_emitted batches set)
    """
    logger = get_run_logger()
    
    # Get all spectral files
    all_spectral_files = glob.glob(os.path.join(folder_path, "*spectral*.nii"))
    
    # Filter to only include readable files (treat unreadable as non-existent)
    spectral_files = [f for f in all_spectral_files if is_file_readable(f)]
    
    if not spectral_files:
        logger.info(f"No readable spectral files found in {folder_path}")
        return batch_id_counter, set()
    
    # Group files by mosaic_id
    files_by_mosaic = defaultdict(list)
    for file_path in spectral_files:
        basename = os.path.basename(file_path)
        try:
            mosaic_id = int(basename.split("_")[1])
            files_by_mosaic[mosaic_id].append(file_path)
        except (ValueError, IndexError):
            logger.warning(f"Could not parse mosaic_id from {basename}, skipping")
            continue
    
    # Process each mosaic range
    newly_emitted = set()
    
    for min_mosaic_id, max_mosaic_id, batch_size in mosaic_ranges:
        for mosaic_id in range(min_mosaic_id, max_mosaic_id + 1):
            if mosaic_id not in files_by_mosaic:
                logger.debug(f"No files found for mosaic {mosaic_id}")
                continue
            
            file_list = files_by_mosaic[mosaic_id]
            
            # Sort files by tile_index for consistent ordering
            file_list.sort(key=lambda f: int(os.path.basename(f).split("_")[3]))
            
            # Build batches keyed by batch_id derived from tile index
            batches = {}
            for fpath in file_list:
                basename = os.path.basename(fpath)
                try:
                    tile_index = int(basename.split("_")[3])
                except (ValueError, IndexError):
                    logger.warning(f"Could not parse tile_index from {basename}, skipping")
                    continue
                
                # Derive batch_id from tile_index: 1 -> 001-027, 2 -> 028-054, etc.
                batch_id_for_tile = (tile_index - 1) // batch_size + 1
                batches.setdefault(batch_id_for_tile, []).append(fpath)
            
            for batch_id_for_tile, batch_files in sorted(batches.items()):
                # Only process full batches
                batch_key = (mosaic_id, batch_id_for_tile)
                
                if len(batch_files) < batch_size:
                    logger.warning(
                        f"Incomplete batch for mosaic {mosaic_id}, logical_batch_id {batch_id_for_tile} "
                        f"(got {len(batch_files)} files, expected {batch_size})"
                    )
                    continue
                
                if batch_key in emitted_batches:
                    # This batch for this mosaic has already been emitted
                    continue
                
                # Map the stable (mosaic_id, batch_id_for_tile) into a unique running batch_id
                batch_id = batch_id_for_tile
                
                try:
                    logger.info(
                        f"Emitting BATCH_READY event for mosaic {mosaic_id} with {len(batch_files)} files "
                        f"(logical_batch_id {batch_id_for_tile}, batch_id {batch_id})"
                    )
                    
                    emit_event(
                        event=BATCH_READY,
                        resource={
                            "prefect.resource.id": f"batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}",
                            "project_name": project_name,
                            "mosaic_id": str(mosaic_id),
                            "batch_id": str(batch_id),
                        },
                        payload={
                            "project_name": project_name,
                            "project_base_path": project_base_path,
                            "mosaic_id": mosaic_id,
                            "batch_id": batch_id,
                            "file_list": batch_files,
                            "archive": False,
                        },
                    )
                    
                    newly_emitted.add(batch_key)
                    logger.info(
                        f"Successfully emitted BATCH_READY event for mosaic {mosaic_id}, "
                        f"logical_batch_id {batch_id_for_tile}, batch_id {batch_id}"
                    )
                    
                    # Emit at most 1 batch per flow run - return immediately after first emission
                    logger.info(f"Emitted 1 batch from {folder_path}. Stopping to limit to 1 batch per run.")
                    return batch_id_counter, newly_emitted
                    
                except Exception as e:
                    logger.error(
                        f"Failed to emit BATCH_READY event for mosaic {mosaic_id}, "
                        f"logical_batch_id {batch_id_for_tile}, batch_id {batch_id}: {e}"
                    )
    
    logger.info(f"Emitted {len(newly_emitted)} new batches from {folder_path}")
    return batch_id_counter, newly_emitted


@flow
def batch_detection_flow(
    project_name: str,
    folder_configs: List[Dict],
) -> Dict[str, Any]:
    """
    Main batch detection flow that scans folders and emits BATCH_READY events.
    
    Args:
        project_name: Project identifier (used to load PSOCTScanConfig block)
        folder_configs: List of dicts with keys:
            - folder_path: str - Path to folder containing spectral files
            - mosaic_ranges: List[Tuple[int, int, int]] - List of (min_id, max_id, batch_size) tuples
    
    Returns:
        Dict with summary of processing results
    """
    logger = get_run_logger()
    
    # # Wait for any running process_tile_batch_flow runs to finish
    # logger.info("Checking for running process_tile_batch_flow runs...")
    # try:
    #     num_waited = wait_for_tile_batch_flow_runs_sync(flow_name="process_tile_batch_flow")
    #     if num_waited > 0:
    #         logger.info(f"Waited for {num_waited} process_tile_batch_flow run(s) to finish")
    # except Exception as e:
    #     logger.warning(f"Error checking for running flow runs: {e}. Continuing anyway.")
    
    # Refresh disk mount at the start of each flow run
    logger.info("Refreshing disk mount...")
    refresh_disk()
    
    # Load project configuration block
    project_config = get_project_config_block(project_name)
    if project_config is None:
        error_msg = (
            f"Project config block for '{project_name}' not found. "
            f"Please create config block '{project_name}-config' before running this flow."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    project_base_path = project_config.project_base_path
    
    # State file path for tracking emitted batches
    state_file_path = os.path.join(project_base_path, ".batch_detection_state.json")
    
    # Load previously emitted batches
    emitted_batches = load_emitted_batches(state_file_path)
    logger.info(f"Loaded {len(emitted_batches)} previously emitted batches from state file")
    
    # Process each folder
    batch_id_counter = 0 #max(emitted_batches, key=lambda x: x[1])[1] + 1
    total_new_batches = 0
    
    for folder_config in folder_configs:
        folder_path = folder_config["folder_path"]
        mosaic_ranges = folder_config["mosaic_ranges"]
        
        logger.info(f"Processing folder: {folder_path}")
        
        try:
            batch_id_counter, newly_emitted = process_folder(
                folder_path=folder_path,
                project_name=project_name,
                project_base_path=project_base_path,
                mosaic_ranges=mosaic_ranges,
                batch_id_counter=batch_id_counter,
                emitted_batches=emitted_batches,
            )
            
            # Update emitted_batches set
            emitted_batches.update(newly_emitted)
            total_new_batches += len(newly_emitted)
            
            # Save state after each folder to persist progress
            save_emitted_batches(state_file_path, emitted_batches)
            
            # Emit at most 1 batch per flow run - stop after first batch is emitted
            if total_new_batches > 0:
                logger.info(f"Emitted 1 batch in this run. Stopping to limit to 1 batch per run.")
                break
            
        except Exception as e:
            logger.error(f"Error processing folder {folder_path}: {e}", exc_info=True)
            continue
    
    logger.info(f"Total batches processed in this run: {total_new_batches}")
    logger.info(f"Total unique batches tracked: {len(emitted_batches)}")
    
    return {
        "project_name": project_name,
        "total_new_batches": total_new_batches,
        "total_tracked_batches": len(emitted_batches),
        "batch_id_counter": batch_id_counter,
    }


# Deployment configuration
if __name__ == "__main__":
    from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
    from workflow.config.blocks import PSOCTScanConfig
    from linc_convert.utils.zarr_config import ZarrConfig
    
    # Configure and save the block
    project_name = "sub-I55"
    block_name = f"{project_name.lower()}-config"
    
    # Create ZarrConfig (you may need to adjust these parameters based on your actual ZarrConfig requirements)
    zarr_config = ZarrConfig(shard=(1024,))
    
    # Create and save PSOCTScanConfig block
    project_config = PSOCTScanConfig(
        zarr_config=zarr_config,
        project_base_path="/space/zircon/5/users/data/sub-I55/",
        grid_size_x_normal=14,  # Adjust based on your project
        grid_size_x_tilted=23,  # Adjust based on your project
        grid_size_y=31,  # Adjust based on your project
        dandiset_path = "/space/zircon/5/users/data/dandi/000053/derivatives/sub-I55/micr/",
        mask_threshold = 60
    )
    
    # Save the block
    project_config.save(block_name, overwrite=True)
    print(f"Saved PSOCTScanConfig block as '{block_name}'")
    
    batch_detection_deployment = batch_detection_flow.to_deployment(
        name="batch_detection_flow",
        interval=timedelta(minutes=4),
        parameters={
            "project_name": project_name,
            "folder_configs": [
                {
                    "folder_path": "/mnt/sas/I55_spectralraw_slice5-28/",
                    "mosaic_ranges": [(1, 2, 31)],
                },
            ],
        },
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
            grace_period_seconds=120,  # 2 minutes
        ),
        tags=["batch-detection", "control-flow"],
    )
    
    from prefect import serve
    
    serve(batch_detection_deployment)

