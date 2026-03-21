"""
OCT watch: continuous batch detection for spectral files.

Runs ``process_tile_batch`` in-process (default) or emits ``BATCH_READY`` (``--no-direct``).
In direct mode, after each batch the sas2 disk is refreshed (unmount/mount) before later scans.

Requires a PSOCTScanConfig block (``opticstream oct setup``).

Dedupes via ``OCT_STATE_SERVICE.peek_batch`` (same idea as LSM ``peek_strip``).
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any

from cyclopts import App

from opticstream.cli.oct import oct_cli
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.config.project_config import get_project_config_block
from opticstream.events import BATCH_READY
from opticstream.events.psoct_event_emitters import emit_batch_psoct_event
from opticstream.flows.psoct.tile_batch_process_flow import process_tile_batch
from opticstream.flows.psoct.utils import oct_batch_ident
from opticstream.state.oct_project_state import OCT_STATE_SERVICE
from opticstream.utils.directory_watch import wait_until_stable

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
    """Unmount/remount sas2 so the next folder scan sees an up-to-date view of the disk."""
    try:
        subprocess.run(["bash", "/usr/etc/sas2_unmount"], check=True)
        subprocess.run(["bash", "/usr/etc/sas2_mount"], check=True)
        logger.info("Disk refresh (sas2 unmount/mount) completed")
    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as e:
        logger.warning("Disk refresh failed: %s. Continuing.", e)


def _process_folder(
    folder_path: str,
    project_name: str,
    project_base_path: str,
    mosaic_ranges: list[tuple[int, int]],
    batch_size: int,
    scan_config: PSOCTScanConfigModel,
    *,
    direct: bool = True,
    force_resend: bool = False,
) -> int:
    """Dispatch every complete batch found in one pass; return how many were processed."""
    root = Path(folder_path)
    spectral_files = [
        p for p in root.glob("*cropped_focus*.nii")
        if p.is_file() and os.access(p, os.R_OK)
    ]

    if not spectral_files:
        logger.info("No readable spectral files in %s", folder_path)
        return 0

    batches_dispatched = 0

    files_by_mosaic: dict[int, list[Path]] = defaultdict(list)
    for file_path in spectral_files:
        basename = file_path.name
        try:
            mosaic_id = int(basename.split("_")[1])
            files_by_mosaic[mosaic_id].append(file_path)
        except (ValueError, IndexError):
            logger.warning("Could not parse mosaic_id from %s, skipping", basename)
            continue

    for min_mosaic_id, max_mosaic_id in mosaic_ranges:
        for mosaic_id in range(min_mosaic_id, max_mosaic_id + 1):
            if mosaic_id not in files_by_mosaic:
                continue

            file_list = files_by_mosaic[mosaic_id]
            file_list.sort(key=lambda f: int(f.name.split("_")[3]))

            batches: dict[int, list[Path]] = {}
            for fpath in file_list:
                try:
                    tile_index = int(fpath.name.split("_")[3])
                except (ValueError, IndexError):
                    logger.warning("Could not parse tile_index from %s, skipping", fpath.name)
                    continue
                logical_batch = (tile_index - 1) // batch_size + 1
                batches.setdefault(logical_batch, []).append(fpath)

            for logical_batch, batch_files in sorted(batches.items()):
                if len(batch_files) < batch_size:
                    logger.warning(
                        "Incomplete batch mosaic=%s logical_batch=%s (%s files, need %s)",
                        mosaic_id,
                        logical_batch,
                        len(batch_files),
                        batch_size,
                    )
                    continue

                batch_ident = oct_batch_ident(project_name, mosaic_id, logical_batch)
                existing = OCT_STATE_SERVICE.peek_batch(batch_ident=batch_ident)
                if existing is not None and not force_resend:
                    logger.info(
                        "[SKIP] batch state exists for %s (use --force-resend to override)",
                        batch_ident,
                    )
                    continue

                try:
                    if direct:
                        logger.info(
                            "process_tile_batch mosaic=%s files=%s logical_batch=%s",
                            mosaic_id,
                            len(batch_files),
                            logical_batch,
                        )
                        process_tile_batch(
                            batch_id=batch_ident,
                            config=scan_config,
                            file_list=batch_files,
                            force_rerun=force_resend,
                        )
                        _refresh_disk()
                    else:
                        logger.info(
                            "emit BATCH_READY mosaic=%s files=%s logical_batch=%s",
                            mosaic_id,
                            len(batch_files),
                            logical_batch,
                        )
                        extra: dict[str, Any] = {
                            "file_list": [str(p) for p in batch_files],
                            "project_base_path": project_base_path,
                        }
                        if force_resend:
                            extra["force_rerun"] = True
                        emit_batch_psoct_event(BATCH_READY, batch_ident, extra_payload=extra)
                        with OCT_STATE_SERVICE.open_batch(batch_ident=batch_ident):
                            pass

                    batches_dispatched += 1
                except Exception as e:
                    logger.error(
                        "batch failed mosaic=%s logical_batch=%s: %s",
                        mosaic_id,
                        logical_batch,
                        e,
                        exc_info=True,
                    )

    if batches_dispatched == 0:
        logger.info("No dispatchable batch in %s", folder_path)
    else:
        logger.info("Dispatched %s batch(es) from %s", batches_dispatched, folder_path)
    return batches_dispatched


def _run_batch_detection_iteration(
    project_name: str,
    folder_path: str,
    mosaic_ranges: list[tuple[int, int]],
    *,
    scan_config: PSOCTScanConfigModel,
    project_base_path: str,
    batch_size: int,
    stability_seconds: int,
    direct: bool,
    force_resend: bool,
) -> int:
    logger.info("Stability check (%s s): %s", stability_seconds, folder_path)
    wait_until_stable(Path(folder_path), stability_seconds)

    return _process_folder(
        folder_path=folder_path,
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_ranges=mosaic_ranges,
        batch_size=batch_size,
        scan_config=scan_config,
        direct=direct,
        force_resend=force_resend,
    )


def _run_continuous_loop(
    project_name: str,
    folder_path: str,
    mosaic_ranges: list[tuple[int, int]],
    *,
    scan_config: PSOCTScanConfigModel,
    project_base_path: str,
    batch_size: int,
    stability_seconds: int = 15,
    direct: bool = True,
    force_resend: bool = False,
) -> None:
    global shutdown_requested
    shutdown_requested = False
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    mode_label = "direct (process_tile_batch in-process)" if direct else "event (emit BATCH_READY)"
    logger.info(
        "Starting batch detection (opticstream oct watch run); project=%s; mode=%s",
        project_name,
        mode_label,
    )
    logger.info("folder=%s; stability=%ss (no delay between iterations)", folder_path, stability_seconds)

    iteration_count = 0
    try:
        while not shutdown_requested:
            iteration_count += 1
            logger.info("--- iteration %s ---", iteration_count)
            try:
                n_batches = _run_batch_detection_iteration(
                    project_name=project_name,
                    folder_path=folder_path,
                    mosaic_ranges=mosaic_ranges,
                    scan_config=scan_config,
                    project_base_path=project_base_path,
                    batch_size=batch_size,
                    stability_seconds=stability_seconds,
                    direct=direct,
                    force_resend=force_resend,
                )
                logger.info("iteration %s: batches_dispatched=%s", iteration_count, n_batches)
            except Exception as e:
                logger.error("iteration %s failed: %s", iteration_count, e, exc_info=True)

            if shutdown_requested:
                break
    except KeyboardInterrupt:
        logger.info("keyboard interrupt, shutting down")
    except Exception as e:
        logger.error("main loop error: %s", e, exc_info=True)
    finally:
        logger.info("batch detection stopped after %s iterations", iteration_count)


def _parse_mosaic_ranges(mosaic_ranges_str: str) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for range_str in mosaic_ranges_str.split(","):
        parts = range_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid mosaic range format: {range_str!r}. Expected 'min:max'")
        min_id, max_id = int(parts[0]), int(parts[1])
        out.append((min_id, max_id))
    return out


@oct_cli.command
def watch(
    project_name: str,
    folder_path: str,
    mosaic_ranges: str,
    *,
    stability_seconds: int = 15,
    direct: bool = True,
    force_resend: bool = False,
) -> None:
    """
    Scan for spectral batches: run ``process_tile_batch`` in-process by default, or
    emit ``BATCH_READY`` when ``--no-direct``. Each iteration processes all complete
    batches found after a stability wait; there is no sleep between iterations.

    mosaic_ranges: comma-separated min:max ranges (e.g. ``1:2,3:4``).
    """
    project_config = get_project_config_block(project_name)
    if project_config is None:
        raise ValueError(
            f"Project config block for '{project_name}' not found. "
            "Run 'opticstream oct setup' first to create the config block."
        )

    scan_config = PSOCTScanConfigModel.model_validate(project_config.model_dump())
    project_base_path = str(project_config.project_base_path)
    batch_size = project_config.acquisition.grid_size_y
    logger.info("Using batch_size (grid_size_y): %s", batch_size)

    _run_continuous_loop(
        project_name=project_name,
        folder_path=folder_path,
        mosaic_ranges=_parse_mosaic_ranges(mosaic_ranges),
        scan_config=scan_config,
        project_base_path=project_base_path,
        batch_size=batch_size,
        stability_seconds=stability_seconds,
        direct=direct,
        force_resend=force_resend,
    )

