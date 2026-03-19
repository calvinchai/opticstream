from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from prefect import flow, get_run_logger, task

from opticstream.config.pipeline_opts_builder import build_pipeline_opts
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.events import BATCH_PROCESSED
from opticstream.flows.psoct.utils import (
    batch_ident_from_payload,
    load_scan_config_for_payload,
    path_list_from_payload,
)
from opticstream.state.milestone_wrappers_psoct import oct_batch_processing_milestone
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTBatchId
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.utils.matlab_execution import run_matlab_batch_command
from opticstream.utils.utils import get_mosaic_paths


@task(task_run_name="complex-to-processed-{batch_id}")
def complex_to_processed_batch(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    config: PSOCTScanConfigModel,
) -> None:
    logger = get_run_logger()
    processed_path, _, _, _ = get_mosaic_paths(str(config.project_base_path), batch_id.mosaic_id)
    processed_path.mkdir(parents=True, exist_ok=True)
    file_list_str = ",".join(f"'{f}'" for f in file_list)
    pipeline_opts = build_pipeline_opts(config)
    cmd = (
        f"complex2processed_batch({{{file_list_str}}}, '{processed_path}/', "
        f"'{config.processing.surface_spec}', {config.processing.enface_depth}, "
        f"pipelineOpts={pipeline_opts.to_matlab_literal()})"
    )
    logger.info("Running MATLAB command: %s", cmd)
    run_matlab_batch_command(cmd)


@flow(flow_run_name="process-complex-tile-batch-{batch_id}")
@oct_batch_processing_milestone(field_name="enface_processed", success_event=BATCH_PROCESSED)
def process_complex_tile_batch(
    batch_id: OCTBatchId,
    config: PSOCTScanConfigModel,
    file_list: list[Path],
    *,
    force_rerun: bool = False,
) -> dict[str, Any]:
    with OCT_STATE_SERVICE.open_batch(batch_ident=batch_id) as batch_state:
        batch_state.mark_started()
    complex_to_processed_batch(
        batch_id=batch_id,
        file_list=file_list,
        config=config,
    )
    return {"processed": True}


@flow
def process_complex_tile_batch_event(payload: Dict[str, Any]) -> dict[str, Any]:
    batch_ident = batch_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    return process_complex_tile_batch(
        batch_id=batch_ident,
        config=cfg,
        file_list=path_list_from_payload(payload),
        force_rerun=force_rerun_from_payload(payload),
    )
