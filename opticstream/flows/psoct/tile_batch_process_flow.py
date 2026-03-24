from __future__ import annotations

import os
import os.path as op
from pathlib import Path
from typing import Any, Dict

from prefect import flow, get_run_logger, task

from opticstream.config.psoct_scan_config import PSOCTScanConfigModel, TileSavingType
from opticstream.events.psoct_event_emitters import emit_batch_psoct_event
from opticstream.events.psoct_events import BATCH_ARCHIVED
from opticstream.flows.psoct.utils import (
    batch_ident_from_payload,
    load_scan_config_for_payload,
    mosaic_context_from_ids,
    path_list_from_payload,
)
from opticstream.state.milestone_wrappers_psoct import oct_batch_processing_milestone
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTBatchId
from opticstream.state.state_guards import enter_flow_stage, enter_milestone_stage, force_rerun_from_payload, should_skip_run
from opticstream.tasks.common_tasks import archive_tile_task
from opticstream.utils.filename_utils import (
    complex_to_complex_filename,
    extract_tile_number_from_filename,
    spectral_to_complex_filename,
)
from opticstream.utils.matlab_execution import run_matlab_batch_command
from opticstream.flows.psoct.utils import get_slice_paths


def _determine_processing_mode(
    *,
    tile_saving_type: TileSavingType,
) -> Dict[str, Any]:
    if tile_saving_type in (TileSavingType.SPECTRAL, TileSavingType.SPECTRAL_12bit):
        return {"mode": "spectral"}
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
    _, _, complex_path = get_slice_paths(str(project_base_path), batch_id.slice_id)
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
    _, _, complex_path = get_slice_paths(str(project_base_path), batch_id.slice_id)
    complex_path.mkdir(parents=True, exist_ok=True)
    output_paths: list[Path] = []
    for source_file in file_list:
        output_file = Path(complex_to_complex_filename(str(source_file), complex_path))
        if output_file.exists() or output_file.is_symlink():
            output_file.unlink()
        os.symlink(source_file, output_file)
        output_paths.append(output_file)
    return output_paths


@task
@oct_batch_processing_milestone(field_name="archived")
def archive_tile_batch(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    acquisition_label: str,
    archive_path: Path,
    archive_tile_name_format: str,
    force_rerun: bool = False,
) -> None:
    logger = get_run_logger()
    archive_path.mkdir(parents=True, exist_ok=True)
    with OCT_STATE_SERVICE.open_batch(batch_ident=batch_id) as batch:
        batch.reset_archived()
    archived_file_paths: list[str] = []
    futures = []
    for source_file in file_list:
        tile_id = extract_tile_number_from_filename(str(source_file))
        output_name = archive_tile_name_format.format(
            project_name=batch_id.project_name,
            slice_id=batch_id.slice_id,
            tile_id=tile_id,
            acq=acquisition_label,
        )
        output_path = archive_path / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        futures.append(archive_tile_task.submit(str(source_file), output_path))
        archived_file_paths.append(str(output_path))
    for future in futures:
        future.wait()
    logger.info("Archived %d files for %s", len(archived_file_paths), batch_id)
    files_with_issue = check_archive_result(batch_id, archived_file_paths)
    if files_with_issue:
        raise RuntimeError(
            "Archive validation failed for "
            f"{len(files_with_issue)} file(s): " + " | ".join(files_with_issue)
        )
    emit_batch_psoct_event(BATCH_ARCHIVED, batch_id, extra_payload={"archived_file_paths": archived_file_paths})


def check_archive_result(
    batch_id: OCTBatchId,
    archived_file_paths: list[str],
    min_file_size_bytes: int = 200 * 1024 * 1024,
) -> list[str]:
    logger = get_run_logger()
    logger.info("Checking if the archived files are valid for %s", batch_id)
    files_with_issue: list[str] = []
    for archived_file_path in archived_file_paths:
        if not os.path.exists(archived_file_path):
            logger.error("Archived file %s does not exist", archived_file_path)
            files_with_issue.append(f"{archived_file_path} (missing)")
            continue

        file_size = os.path.getsize(archived_file_path)
        if file_size <= min_file_size_bytes:
            logger.error(
                "Archived file %s is too small: %d bytes (threshold: %d bytes)",
                archived_file_path,
                file_size,
                min_file_size_bytes,
            )
            files_with_issue.append(
                f"{archived_file_path} (size={file_size}B, min={min_file_size_bytes}B)"
            )
    return files_with_issue

@flow(flow_run_name="process-tile-batch-{batch_id}")
def process_tile_batch(
    batch_id: OCTBatchId,
    config: PSOCTScanConfigModel,
    file_list: list[Path],
    *,
    force_rerun: bool = False,
) -> dict[str, Any]:
    logger = get_run_logger()
    if should_skip_run(enter_flow_stage(OCT_STATE_SERVICE.peek_batch(batch_ident=batch_id), force_rerun=force_rerun, skip_if_running=False, item_ident=batch_id)):
        return None
    mosaic_context = mosaic_context_from_ids(
        slice_id=batch_id.slice_id,
        mosaic_id=batch_id.mosaic_id,
        mosaics_per_slice=config.mosaics_per_slice,
    )
    archive_future = None
    if config.archive_path:
        archive_future = archive_tile_batch.submit(
            batch_id=batch_id,
            file_list=file_list,
            acquisition_label=mosaic_context.acquisition_label,
            archive_path=config.archive_path,
            archive_tile_name_format=config.archive_tile_name_format,
            force_rerun=force_rerun,
        )
    
    mode = _determine_processing_mode(
        tile_saving_type=config.acquisition.tile_saving_type,
    )
    complex_files: list[Path] = []
    if mode["mode"] == "spectral":
        aline_length = mosaic_context.tile_size_x(config)
        bline_length = mosaic_context.tile_size_y(config)
        complex_files = process_spectral_tile_batch(
            batch_id=batch_id,
            file_list=file_list,
            project_base_path=config.project_base_path,
            aline_length=aline_length,
            bline_length=bline_length,
        )
    elif mode["mode"] == "complex":
        complex_files = link_complex_inputs_to_mosaic_complex_dir(
            batch_id=batch_id,
            file_list=file_list,
            project_base_path=config.project_base_path,
        )

    
    
    if archive_future:
        archive_future.wait()


    logger.info("Produced %d complex files for %s", len(complex_files), batch_id)
    return {"complex_files": [str(p) for p in complex_files], "archived_files": []}


@flow
def process_tile_batch_event_flow(payload: Dict[str, Any]) -> dict[str, Any]:
    batch_ident = batch_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    return process_tile_batch(
        batch_id=batch_ident,
        config=cfg,
        file_list=path_list_from_payload(payload),
        force_rerun=force_rerun_from_payload(payload),
    )


process_tile_batch_flow = process_tile_batch