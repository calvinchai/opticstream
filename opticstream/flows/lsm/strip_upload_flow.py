from typing import Any, Dict, List

from prefect import flow, get_run_logger

from opticstream.config.lsm_scan_config import LSMScanConfig, get_lsm_scan_config
from opticstream.events import get_event_trigger
from opticstream.events.lsm_events import STRIP_COMPRESSED
from opticstream.flows.lsm.paths import strip_zarr_output_path
from opticstream.state.state_guards import (
    RunDecision,
    force_rerun_from_payload,
    enter_milestone_stage,
)
from opticstream.flows.lsm.utils import strip_ident_from_payload
from opticstream.state.lsm_project_state import LSM_STATE_SERVICE, LSMStripId
from opticstream.tasks.dandi_upload import upload_to_dandi


@flow(flow_run_name="upload-to-dandi-{strip_ident}")
def upload_strip_to_dandi_flow(
    strip_ident: LSMStripId,
    output_path: str,
    dandi_instance: str = "linc",
    dandi_bin: str = "dandi",
    force_rerun: bool = False,
) -> None:
    """
    Upload the strip to DANDI.
    """
    logger = get_run_logger()
    logger.info(f"Uploading {strip_ident} to DANDI")
    if (
        enter_milestone_stage(
            item_state_view=LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident),
            item_ident=strip_ident,
            field_name="uploaded",
            force_rerun=force_rerun,
        )
        == RunDecision.SKIPPED
    ):
        return
    upload_to_dandi(output_path, dandi_instance, dandi_bin)
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.set_uploaded(True)
    logger.info(f"Successfully uploaded {strip_ident} to DANDI")


def resolve_config(
    payload: Dict[str, Any], keys: List[str]
) -> tuple[Dict[str, Any], LSMScanConfig]:
    """
    Resolve configuration values from payload and project config.

    Priority: payload[key] → project_config.key → omit key

    Returns resolved dict and the loaded scan config (single load per call).
    """
    strip_ident = strip_ident_from_payload(payload)
    project_name = strip_ident.project_name
    cfg = get_lsm_scan_config(
        project_name, override_config_name=payload.get("override_config")
    )
    resolved: Dict[str, Any] = {}
    for key in keys:
        if key in payload:
            resolved[key] = payload[key]
        elif hasattr(cfg, key):
            resolved[key] = getattr(cfg, key)
    return resolved, cfg


@flow
def upload_strip_to_dandi_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event wrapper flow for uploading the strip to DANDI.

    Payload requires ``strip_ident`` (dict); zarr path is derived from config.
    """
    strip_ident = strip_ident_from_payload(payload)
    config, cfg = resolve_config(payload, ["dandi_instance", "dandi_bin"])
    output_path = strip_zarr_output_path(strip_ident, cfg)
    return upload_strip_to_dandi_flow(
        strip_ident=strip_ident,
        output_path=output_path,
        force_rerun=force_rerun_from_payload(payload),
        **config,
    )

def to_deployment():
    upload_strip_to_dandi_flow_deployment = upload_strip_to_dandi_flow.to_deployment(
        name="upload_strip_to_dandi_flow"
    )
    upload_strip_to_dandi_event_flow_deployment = (
        upload_strip_to_dandi_event_flow.to_deployment(
            name="upload_strip_to_dandi_event_flow",
            triggers=[
                get_event_trigger(STRIP_COMPRESSED),
            ],
        )
    )
    return upload_strip_to_dandi_flow_deployment, upload_strip_to_dandi_event_flow_deployment