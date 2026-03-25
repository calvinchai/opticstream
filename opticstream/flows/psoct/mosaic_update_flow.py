from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from prefect import flow, task

from opticstream.events import MOSAIC_ENFACE_STITCHED, SLICE_READY
from opticstream.events.psoct_event_emitters import emit_slice_psoct_event
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
    check_slice_ready(slice_ident=slice_ident, mosaics_per_slice=cfg.mosaics_per_slice)


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
):
    """
    Event-only deployment for `on_mosaic_events`.

    Triggered by MOSAIC_ENFACE_STITCHED to emit SLICE_READY once all mosaics in the
    slice are stitched.
    """
    return [
        on_mosaic_events.to_deployment(
            name=deployment_name,
            tags=["event-driven", "slice-ready", *list(extra_tags)],
            triggers=[
                get_event_trigger(MOSAIC_ENFACE_STITCHED, project_name=project_name)
            ],
        )
    ]
