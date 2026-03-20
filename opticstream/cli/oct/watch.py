"""
OCT watch: continuous batch detection for spectral files.

Watches folders for spectral files, groups them into batches, and calls
process_tile_batch_event_flow directly. Provides setup-config (PSOCTScanConfig
block) and run (continuous loop) commands.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import signal
import subprocess
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from cyclopts import App
from niizarr import ZarrConfig

from opticstream.cli.oct import oct_cli
from opticstream.config.psoct_scan_config import PSOCTScanConfig
from opticstream.config.project_config import get_project_config_block
from opticstream.flows.psoct.tile_batch_process_flow import process_tile_batch_event_flow

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)

shutdown_requested = False


def _signal_handler(signum: int, frame: Any) -> None:
    global shutdown_requested
    logger.info("Received signal %s. Initiating graceful shutdown...", signum)
    shutdown_requested = True


def _refresh_disk() -> None:
    """Refresh disk mount by unmounting and remounting sas2."""
    return # TODO: Skip in test
    try:
        subprocess.run(["bash", "/usr/etc/sas2_unmount"], check=True)
        subprocess.run(["bash", "/usr/etc/sas2_mount"], check=True)
    except subprocess.CalledProcessError as e:
        logger.warning("Error refreshing disk: %s. Continuing anyway.", e)


def _is_file_readable(file_path: str) -> bool:
    if not os.path.exists(file_path):
        return False
    return os.access(file_path, os.R_OK)


def _load_processed_batches(state_file_path: str) -> Set[Tuple[int, int]]:
    if not os.path.exists(state_file_path):
        return set()
    try:
        with open(state_file_path) as f:
            data = json.load(f)
            return {tuple(item) for item in data.get("processed_batches", [])}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Error loading state file %s: %s. Starting with empty state.", state_file_path, e)
        return set()


def _save_processed_batches(state_file_path: str, processed_batches: Set[Tuple[int, int]]) -> None:
    os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
    data = {"processed_batches": [list(item) for item in processed_batches]}
    try:
        with open(state_file_path, "w") as f:
            json.dump(data, f, indent=2)
    except OSError as e:
        logger.error("Error saving state file %s: %s", state_file_path, e)


def _process_folder(
    folder_path: str,
    project_name: str,
    project_base_path: str,
    mosaic_ranges: List[Tuple[int, int]],
    batch_size: int,
    batch_id_counter: int,
    processed_batches: Set[Tuple[int, int]],
) -> Tuple[int, Set[Tuple[int, int]]]:
    all_spectral_files = glob.glob(os.path.join(folder_path, "*cropped_focus*.nii"))
    spectral_files = [f for f in all_spectral_files if _is_file_readable(f)]

    if not spectral_files:
        logger.info("No readable spectral files found in %s", folder_path)
        return batch_id_counter, set()

    files_by_mosaic: Dict[int, List[str]] = defaultdict(list)
    for file_path in spectral_files:
        basename = os.path.basename(file_path)
        try:
            mosaic_id = int(basename.split("_")[1])
            files_by_mosaic[mosaic_id].append(file_path)
        except (ValueError, IndexError):
            logger.warning("Could not parse mosaic_id from %s, skipping", basename)
            continue

    newly_processed: Set[Tuple[int, int]] = set()

    for min_mosaic_id, max_mosaic_id in mosaic_ranges:
        for mosaic_id in range(min_mosaic_id, max_mosaic_id + 1):
            if mosaic_id not in files_by_mosaic:
                logger.debug("No files found for mosaic %s", mosaic_id)
                continue

            file_list = files_by_mosaic[mosaic_id]
            file_list.sort(key=lambda f: int(os.path.basename(f).split("_")[3]))

            batches: Dict[int, List[str]] = {}
            for fpath in file_list:
                basename = os.path.basename(fpath)
                try:
                    tile_index = int(basename.split("_")[3])
                except (ValueError, IndexError):
                    logger.warning("Could not parse tile_index from %s, skipping", basename)
                    continue
                batch_id_for_tile = (tile_index - 1) // batch_size + 1
                batches.setdefault(batch_id_for_tile, []).append(fpath)

            for batch_id_for_tile, batch_files in sorted(batches.items()):
                batch_key = (mosaic_id, batch_id_for_tile)
                if len(batch_files) < batch_size:
                    logger.warning(
                        "Incomplete batch for mosaic %s, logical_batch_id %s (got %s files, expected %s)",
                        mosaic_id,
                        batch_id_for_tile,
                        len(batch_files),
                        batch_size,
                    )
                    continue
                if batch_key in processed_batches:
                    continue

                batch_id = batch_id_for_tile
                try:
                    logger.info(
                        "Calling process_tile_batch_event_flow for mosaic %s with %s files (logical_batch_id %s, batch_id %s)",
                        mosaic_id,
                        len(batch_files),
                        batch_id_for_tile,
                        batch_id,
                    )
                    payload = {
                        "project_name": project_name,
                        "project_base_path": project_base_path,
                        "mosaic_id": mosaic_id,
                        "batch_id": batch_id,
                        "file_list": batch_files,
                        "archive": True,
                    }
                    process_tile_batch_event_flow(payload)
                    newly_processed.add(batch_key)
                    logger.info(
                        "Successfully called process_tile_batch_event_flow for mosaic %s, logical_batch_id %s, batch_id %s",
                        mosaic_id,
                        batch_id_for_tile,
                        batch_id,
                    )
                    logger.info(
                        "Processed 1 batch from %s. Stopping to limit to 1 batch per iteration.",
                        folder_path,
                    )
                    return batch_id_counter, newly_processed
                except Exception as e:
                    logger.error(
                        "Failed to call process_tile_batch_event_flow for mosaic %s, logical_batch_id %s, batch_id %s: %s",
                        mosaic_id,
                        batch_id_for_tile,
                        batch_id,
                        e,
                        exc_info=True,
                    )

    logger.info("Processed %s new batches from %s", len(newly_processed), folder_path)
    return batch_id_counter, newly_processed


def _run_batch_detection_iteration(
    project_name: str,
    folder_configs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    logger.info("Refreshing disk mount...")
    _refresh_disk()

    project_config = get_project_config_block(project_name)
    if project_config is None:
        raise ValueError(
            f"Project config block for '{project_name}' not found. "
            f"Please run 'opticstream oct watch setup-config' first to create the config block."
        )

    project_base_path = project_config.project_base_path
    batch_size = project_config.acquisition.grid_size_y
    state_file_path = os.path.join(project_base_path, ".batch_detection_direct_state.json")
    processed_batches = _load_processed_batches(state_file_path)
    logger.info("Loaded %s previously processed batches from state file", len(processed_batches))
    logger.info("Using batch_size (grid_size_y): %s", batch_size)

    batch_id_counter = 0
    total_new_batches = 0

    for folder_config in folder_configs:
        folder_path = folder_config["folder_path"]
        mosaic_ranges = folder_config["mosaic_ranges"]
        logger.info("Processing folder: %s", folder_path)
        try:
            batch_id_counter, newly_processed = _process_folder(
                folder_path=folder_path,
                project_name=project_name,
                project_base_path=project_base_path,
                mosaic_ranges=mosaic_ranges,
                batch_size=batch_size,
                batch_id_counter=batch_id_counter,
                processed_batches=processed_batches,
            )
            processed_batches.update(newly_processed)
            total_new_batches += len(newly_processed)
            _save_processed_batches(state_file_path, processed_batches)
            if total_new_batches > 0:
                logger.info("Processed 1 batch in this iteration. Stopping to limit to 1 batch per iteration.")
                break
        except Exception as e:
            logger.error("Error processing folder %s: %s", folder_path, e, exc_info=True)
            continue

    logger.info("Total batches processed in this iteration: %s", total_new_batches)
    logger.info("Total unique batches tracked: %s", len(processed_batches))
    return {
        "project_name": project_name,
        "total_new_batches": total_new_batches,
        "total_tracked_batches": len(processed_batches),
        "batch_id_counter": batch_id_counter,
    }


def _configure_project_block(
    project_name: str,
    project_base_path: str,
    grid_size_x_normal: int = 14,
    grid_size_x_tilted: int = 23,
    grid_size_y: int = 31,
    *,
    dandiset_path: Optional[str] = None,
    mask_threshold_normal: float = 60.0,
    mask_threshold_tilted: float = 55.0,
    zarr_shard: Tuple[int, ...] = (1024,),
    overwrite: bool = True,
) -> PSOCTScanConfig:
    block_name = f"{project_name.lower().replace('_', '-')}-config"
    zarr_config = ZarrConfig(shard=zarr_shard)
    config_dict: Dict[str, Any] = {
        "project_name": project_name,
        "zarr_config": zarr_config,
        "project_base_path": project_base_path,
        "acquisition": {
            "grid_size_x_normal": grid_size_x_normal,
            "grid_size_x_tilted": grid_size_x_tilted,
            "grid_size_y": grid_size_y,
        },
        "mask_threshold_normal": mask_threshold_normal,
        "mask_threshold_tilted": mask_threshold_tilted,
    }
    if dandiset_path is not None:
        config_dict["dandiset_path"] = dandiset_path
    project_config = PSOCTScanConfig(**config_dict)
    project_config.save(block_name, overwrite=overwrite)
    logger.info("Saved PSOCTScanConfig block as '%s'", block_name)
    return project_config


def _run_continuous_loop(
    project_name: str,
    folder_configs: List[Dict[str, Any]],
    interval_seconds: int = 240,
) -> None:
    global shutdown_requested
    shutdown_requested = False
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("=" * 80)
    logger.info("Starting batch detection (opticstream oct watch run)")
    logger.info("Project: %s", project_name)
    logger.info("Interval: %s seconds (%.1f minutes)", interval_seconds, interval_seconds / 60.0)
    logger.info("Number of folders to monitor: %s", len(folder_configs))
    logger.info("=" * 80)

    iteration_count = 0
    try:
        while not shutdown_requested:
            iteration_count += 1
            logger.info("--- Starting iteration %s ---", iteration_count)
            try:
                result = _run_batch_detection_iteration(
                    project_name=project_name,
                    folder_configs=folder_configs,
                )
                logger.info("Iteration %s completed: %s", iteration_count, result)
            except Exception as e:
                logger.error("Error in iteration %s: %s", iteration_count, e, exc_info=True)

            if shutdown_requested:
                logger.info("Shutdown requested. Exiting loop.")
                break

            logger.info("Waiting %s seconds until next iteration...", interval_seconds)
            sleep_chunk = min(interval_seconds, 10)
            elapsed = 0
            while elapsed < interval_seconds and not shutdown_requested:
                time.sleep(sleep_chunk)
                elapsed += sleep_chunk
                if elapsed < interval_seconds:
                    sleep_chunk = min(interval_seconds - elapsed, 10)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error("Unexpected error in main loop: %s", e, exc_info=True)
    finally:
        logger.info("=" * 80)
        logger.info("Batch detection stopped after %s iterations", iteration_count)
        logger.info("=" * 80)


# ---------------------------------------------------------------------------
# Cyclopts CLI
# ---------------------------------------------------------------------------

watch_cli = oct_cli.command(App(name="watch"))


@watch_cli.command
def setup(
    project_name: str,
    project_base_path: str,
    grid_size_x_normal: int,
    grid_size_x_tilted: int,
    grid_size_y: int,
    *,
    dandiset_path: Optional[str] = None,
    mask_threshold_normal: float = 60.0,
    mask_threshold_tilted: float = 55.0,
    zarr_shard: Tuple[int, ...] = (1024,),
    overwrite: bool = True,
) -> None:
    """
    Create or update the PSOCTScanConfig block for a project.

    Run this before 'opticstream oct watch run'. The block name is
    derived from project_name (e.g. 'myproject' -> 'myproject-config').
    """
    _configure_project_block(
        project_name=project_name,
        project_base_path=project_base_path,
        grid_size_x_normal=grid_size_x_normal,
        grid_size_x_tilted=grid_size_x_tilted,
        grid_size_y=grid_size_y,
        dandiset_path=dandiset_path,
        mask_threshold_normal=mask_threshold_normal,
        mask_threshold_tilted=mask_threshold_tilted,
        zarr_shard=zarr_shard,
        overwrite=overwrite,
    )
    print(f"PSOCTScanConfig block for project '{project_name}' saved successfully.")


def _parse_mosaic_ranges(mosaic_ranges_str: str) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    for range_str in mosaic_ranges_str.split(","):
        parts = range_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid mosaic range format: {range_str!r}. Expected 'min:max'")
        min_id, max_id = int(parts[0]), int(parts[1])
        out.append((min_id, max_id))
    return out


@watch_cli.command
def run(
    project_name: str,
    folder_path: str,
    mosaic_ranges: str,
    interval: int = 240,
) -> None:
    """
    Run continuous batch detection: scan folders for spectral files, group into
    batches, and call process_tile_batch_event_flow for each complete batch.

    Requires the project's PSOCTScanConfig block to exist (run
    'opticstream oct watch setup-config' first).

    mosaic_ranges format: 'min1:max1,min2:max2' (e.g. '1:2,3:4').
    """
    mosaic_ranges_list = _parse_mosaic_ranges(mosaic_ranges)
    folder_configs = [
        {"folder_path": folder_path, "mosaic_ranges": mosaic_ranges_list},
    ]
    _run_continuous_loop(
        project_name=project_name,
        folder_configs=folder_configs,
        interval_seconds=interval,
    )
