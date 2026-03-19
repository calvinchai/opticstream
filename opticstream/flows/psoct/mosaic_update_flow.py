from __future__ import annotations

from typing import Any, Dict

from prefect import flow, task

from opticstream.events import MOSAIC_STITCHED, SLICE_READY
from opticstream.events.psoct_event_emitters import emit_slice_psoct_event
from opticstream.events.psoct_events import MOSAIC_READY, MOSAIC_VOLUME_STITCHED, MOSAIC_VOLUME_UPLOADED
from opticstream.events.utils import get_event_trigger
from opticstream.flows.psoct.utils import (
    load_scan_config_for_payload,
    slice_ident_from_payload,
)
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTSliceId


@task(task_run_name="check-slice-ready-{slice_ident}")
def check_slice_ready(slice_ident: OCTSliceId, mosaics_per_slice: int) -> None:
    slice_view = OCT_STATE_SERVICE.peek_slice(slice_ident=slice_ident)
    if slice_view is None:
        return
    if slice_view.all_mosaics_enface_stitched(total_mosaics=mosaics_per_slice):
        emit_slice_psoct_event(SLICE_READY, slice_ident)


@flow
def on_mosaic_events(event: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg = load_scan_config_for_payload(payload)
    slice_ident = slice_ident_from_payload(payload)
    if event == MOSAIC_STITCHED:
        check_slice_ready(slice_ident=slice_ident, mosaics_per_slice=cfg.mosaics_per_slice)
    
if __name__ == "__main__":
    mosaic_update_event_deployment = on_mosaic_events.to_deployment(
        name="direct",
        triggers=[get_event_trigger(MOSAIC_STITCHED),
        get_event_trigger(MOSAIC_READY),
        get_event_trigger(MOSAIC_VOLUME_STITCHED),
        get_event_trigger(MOSAIC_VOLUME_UPLOADED)
        ],
    )