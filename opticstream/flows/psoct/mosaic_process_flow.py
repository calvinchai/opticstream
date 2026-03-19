from __future__ import annotations

from typing import Any, Dict

from prefect import flow

from opticstream.flows.mosaic_processing_flow import process_mosaic_flow as _process_mosaic_flow
from opticstream.flows.psoct.utils import (
    kwargs_for_process_mosaic_event,
    load_scan_config_for_payload,
    mosaic_ident_from_payload,
)
from opticstream.state.state_guards import force_rerun_from_payload


@flow(flow_run_name="process-mosaic-{mosaic_ident}")
def process_mosaic(
    mosaic_ident,
    *,
    force_refresh_coords: bool = False,
    force_rerun: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    # Keep heavy stitching logic in one implementation while psoct wrappers
    # provide the new, typed event-entry pattern.
    return _process_mosaic_flow(
        project_name=mosaic_ident.project_name,
        mosaic_id=mosaic_ident.mosaic_id,
        force_refresh_coords=force_refresh_coords,
        **kwargs,
    )


@flow
def process_mosaic_event_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    mosaic_ident = mosaic_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    return process_mosaic(
        mosaic_ident=mosaic_ident,
        force_refresh_coords=bool(payload.get("force_refresh_coords", False)),
        **kwargs_for_process_mosaic_event(mosaic_ident, cfg, payload),
        force_rerun=force_rerun_from_payload(payload),
    )
