from __future__ import annotations

import os
import os.path as op
from pathlib import Path
from typing import Any, Dict

from prefect import flow, get_run_logger, task

from opticstream.config.psoct_scan_config import PSOCTScanConfigModel, TileSavingType
from opticstream.events import BATCH_ARCHIVED, BATCH_COMPLEXED
from opticstream.flows.psoct.utils import (
    batch_ident_from_payload,
    load_scan_config_for_payload,
    path_list_from_payload,
)
from opticstream.state.milestone_wrappers_psoct import oct_batch_processing_milestone
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTBatchId
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.tasks.common_tasks import archive_tile_task
from opticstream.utils.filename_utils import (
    complex_to_complex_filename,
    extract_tile_index_from_filename,
    spectral_to_complex_filename,
)
from opticstream.utils.matlab_execution import run_matlab_batch_command
from opticstream.utils.utils import get_mosaic_paths


def _determine_processing_mode(
    *,
    convert: bool,
    tile_saving_type: TileSavingType,
    mosaic_id: int,
    tile_size_x_tilted: int,
    tile_size_x_normal: int,
    tile_size_y: int,
) -> Dict[str, Any]:
    if not convert:
        return {"mode": None}
    if tile_saving_type == TileSavingType.SPECTRAL:
        return {
            "mode": "spectral",
            "aline_length": tile_size_x_tilted if mosaic_id % 2 == 0 else tile_size_x_normal,
            "bline_length": tile_size_y,
        }
    if tile_saving_type in (TileSavingType.COMPLEX, TileSavingType.COMPLEX_WITH_SPECTRAL):
        return {"mode": "complex"}
    return {"mode": None}


@task(task_run_name="spectral-to-complex-{batch_id}")
def process_spectral_tile_batch(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    project_base_path: Path,
    aline_length: int,
    bline_length: int,
) -> list[Path]:
    logger = get_run_logger()
    _, _, complex_path, _ = get_mosaic_paths(str(project_base_path), batch_id.mosaic_id)
    complex_path.mkdir(parents=True, exist_ok=True)
    file_list_str = ",".join(f"'{file}'" for file in file_list)
    cmd = (
        f"spectral2complex_batch({{{file_list_str}}}, '{complex_path}/', "
        f"{aline_length}, {bline_length})"
    )
    logger.info("Running MATLAB command: %s", cmd)
    run_matlab_batch_command(cmd)
    complex_files = [Path(spectral_to_complex_filename(str(f), complex_path)) for f in file_list]
    for complex_file in complex_files:
        if not complex_file.is_file():
            raise FileNotFoundError(f"complex file missing after processing: {complex_file}")
    return complex_files


@task(task_run_name="complex-link-{batch_id}")
def link_complex_inputs_to_mosaic_complex_dir(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    project_base_path: Path,
) -> list[Path]:
    _, _, complex_path, _ = get_mosaic_paths(str(project_base_path), batch_id.mosaic_id)
    complex_path.mkdir(parents=True, exist_ok=True)
    output_paths: list[Path] = []
    for source_file in file_list:
        output_file = Path(complex_to_complex_filename(str(source_file), complex_path))
        if output_file.exists() or output_file.is_symlink():
            output_file.unlink()
        os.symlink(source_file, output_file)
        output_paths.append(output_file)
    return output_paths


@flow(flow_run_name="archive-tile-batch-{batch_id}")
@oct_batch_processing_milestone(field_name="archived", success_event=BATCH_ARCHIVED)
def archive_tile_batch(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    archive_path: Path,
    archive_tile_name_format: str,
    force_rerun: bool = False,
) -> list[str]:
    logger = get_run_logger()
    archive_path.mkdir(parents=True, exist_ok=True)
    acq = "tilted" if batch_id.mosaic_id % 2 == 0 else "normal"
    archived_file_paths: list[str] = []
    futures = []
    for source_file in file_list:
        tile_id = extract_tile_index_from_filename(str(source_file))
        output_name = archive_tile_name_format.format(
            project_name=batch_id.project_name,
            slice_id=batch_id.slice_number,
            tile_id=tile_id,
            acq=acq,
        )
        output_path = op.join(str(archive_path), output_name)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        futures.append(archive_tile_task.submit(str(source_file), output_path))
        archived_file_paths.append(output_path)
    for future in futures:
        future.wait()
    logger.info("Archived %d files for %s", len(archived_file_paths), batch_id)
    return archived_file_paths


@flow(flow_run_name="process-tile-batch-{batch_id}")
@oct_batch_processing_milestone(field_name="complexed", success_event=BATCH_COMPLEXED)
def process_tile_batch(
    batch_id: OCTBatchId,
    config: PSOCTScanConfigModel,
    file_list: list[Path],
    *,
    archive: bool = True,
    convert: bool = True,
    force_rerun: bool = False,
) -> dict[str, Any]:
    logger = get_run_logger()
    with OCT_STATE_SERVICE.open_batch(batch_ident=batch_id) as batch_state:
        batch_state.mark_started()
    archived: list[str] = []
    if archive:
        archive_root = Path(config.archive_path or (config.project_base_path / "archived"))
        archived = archive_tile_batch(
            batch_id=batch_id,
            file_list=file_list,
            archive_path=archive_root,
            archive_tile_name_format=config.archive_tile_name_format,
            force_rerun=force_rerun,
        )

    mode = _determine_processing_mode(
        convert=convert,
        tile_saving_type=config.acquisition.tile_saving_type,
        mosaic_id=batch_id.mosaic_id,
        tile_size_x_tilted=config.acquisition.tile_size_x_tilted,
        tile_size_x_normal=config.acquisition.tile_size_x_normal,
        tile_size_y=config.acquisition.tile_size_y,
    )
    complex_files: list[Path] = []
    if mode["mode"] == "spectral":
        complex_files = process_spectral_tile_batch(
            batch_id=batch_id,
            file_list=file_list,
            project_base_path=config.project_base_path,
            aline_length=mode["aline_length"],
            bline_length=mode["bline_length"],
        )
    elif mode["mode"] == "complex":
        complex_files = link_complex_inputs_to_mosaic_complex_dir(
            batch_id=batch_id,
            file_list=file_list,
            project_base_path=config.project_base_path,
        )
    logger.info("Produced %d complex files for %s", len(complex_files), batch_id)
    return {"complex_files": [str(p) for p in complex_files], "archived_files": archived}


@flow
def process_tile_batch_event_flow(payload: Dict[str, Any]) -> dict[str, Any]:
    batch_ident = batch_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    return process_tile_batch(
        batch_id=batch_ident,
        config=cfg,
        file_list=path_list_from_payload(payload),
        archive=bool(payload.get("archive", True)),
        convert=bool(payload.get("convert", True)),
        force_rerun=force_rerun_from_payload(payload),
    )


process_tile_batch_flow = process_tile_batch