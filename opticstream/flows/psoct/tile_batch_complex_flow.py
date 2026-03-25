from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from prefect import flow, get_run_logger, task

from opticstream.artifacts.publish_hooks import (
    publish_oct_mosaic_hook,
    publish_oct_project_hook,
)
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.events import BATCH_PROCESSED
from opticstream.flows.psoct.tile_batch_matlab import build_complex_to_processed_command
from opticstream.flows.psoct.tile_file_reference import build_tile_file_reference_list
from opticstream.flows.psoct.utils import (
    batch_ident_from_payload,
    load_scan_config_for_payload,
    mosaic_context_from_ids,
    path_list_from_payload,
)
from opticstream.state.milestone_wrappers_psoct import oct_batch_processing_milestone
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTBatchId
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.utils.matlab_execution import run_matlab_batch_command_or_cli


@task(task_run_name="complex-to-processed-{batch_id}")
def complex_to_processed_batch(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    config: PSOCTScanConfigModel,
) -> None:
    logger = get_run_logger()
    mosaic_context = mosaic_context_from_ids(
        slice_id=batch_id.slice_id,
        mosaic_id=batch_id.mosaic_id,
        mosaics_per_slice=config.mosaics_per_slice,
    )
    file_reference_list = build_tile_file_reference_list(
        file_list,
        config=config,
        mosaic_context=mosaic_context,
    )
    _, cmd = build_complex_to_processed_command(
        batch_id,
        file_reference_list,
        config=config,
        mosaic_context=mosaic_context,
    )
    logger.info("Running MATLAB command: %s", cmd)
    run_matlab_batch_command_or_cli(
        cmd,
        matlab_script_path=str(config.matlab_root) if config.matlab_root else None,
    )


@flow(
    flow_run_name="process-complex-tile-batch-{batch_id}",
    on_completion=[publish_oct_mosaic_hook, publish_oct_project_hook],
)
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
