"""Hook to emit SLICE_READY when all mosaics in a slice are enface-stitched."""

from __future__ import annotations

from typing import Any

from prefect.logging.loggers import flow_run_logger

from opticstream.config.psoct_scan_config import get_psoct_scan_config
from opticstream.events.psoct_event_emitters import emit_slice_psoct_event
from opticstream.events.psoct_events import SLICE_READY
from opticstream.state.oct_project_state import OCTSliceId, OCT_STATE_SERVICE
from opticstream.utils.flow_run_name_parse import (
    missing_required_fields,
    parse_flow_run_name_fields,
)

_REQUIRED_FIELDS = ("project_name", "slice_id")


def check_slice_ready_hook(flow: Any, flow_run: Any, state: Any) -> None:
    """
    Prefect on_completion hook: emit SLICE_READY when every mosaic in the
    slice has been enface-stitched.

    Parses ``project_name`` and ``slice_id`` from the flow run name, loads
    the scan config to obtain ``mosaics_per_slice``, and checks the slice
    state.
    """
    logger = flow_run_logger(flow_run, flow)
    parsed = parse_flow_run_name_fields(flow_run.name or "")
    missing = missing_required_fields(parsed, _REQUIRED_FIELDS)
    if missing:
        logger.info(
            "check_slice_ready_hook skipped for run %s; missing fields: %s",
            flow_run.name,
            ", ".join(missing),
        )
        return

    slice_ident = OCTSliceId(
        project_name=str(parsed["project_name"]),
        slice_id=int(parsed["slice_id"]),
    )

    scan_config = get_psoct_scan_config(str(parsed["project_name"]))
    slice_view = OCT_STATE_SERVICE.peek_slice(slice_ident=slice_ident)
    if slice_view is None:
        logger.info(
            "check_slice_ready_hook: no slice state for %s", slice_ident
        )
        return

    if slice_view.all_mosaics_enface_stitched(total_mosaics=scan_config.mosaics_per_slice):
        emit_slice_psoct_event(SLICE_READY, slice_ident)
        logger.info(
            "check_slice_ready_hook: emitted SLICE_READY for %s", slice_ident
        )
