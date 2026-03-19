from __future__ import annotations

from typing import Any, Dict

from prefect import flow, task

from opticstream.events import BATCH_PROCESSED, MOSAIC_READY
from opticstream.events.psoct_event_emitters import emit_mosaic_psoct_event
from opticstream.flows.psoct.update_artifacts import publish_mosaic_progress_artifact
from opticstream.flows.psoct.utils import (
    load_scan_config_for_payload,
    mosaic_ident_from_payload,
)
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTMosaicId


@task(task_run_name="check-mosaic-ready-{mosaic_ident}")
def check_mosaic_ready(mosaic_ident: OCTMosaicId, total_batches: int) -> bool:
    mosaic_view = OCT_STATE_SERVICE.peek_mosaic(mosaic_ident=mosaic_ident)
    if mosaic_view is None:
        return False
    if mosaic_view.all_batches_done(total_batches=total_batches):
        emit_mosaic_psoct_event(MOSAIC_READY, mosaic_ident)

@flow
def on_batch_events(event: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    mosaic_ident = mosaic_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    artifact_key = publish_mosaic_progress_artifact(mosaic_ident)
    total_batches = (
        cfg.acquisition.grid_size_x_tilted
        if mosaic_ident.mosaic_id % 2 == 0
        else cfg.acquisition.grid_size_x_normal
    )
    if event == BATCH_PROCESSED:
        check_mosaic_ready(mosaic_ident, total_batches)

