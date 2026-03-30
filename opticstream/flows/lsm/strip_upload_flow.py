from typing import Any, Dict, List, Optional, Sequence

from prefect import flow, get_run_logger

from opticstream.hooks.publish_hooks import (
    publish_lsm_project_hook,
    publish_lsm_slice_hook,
)
from opticstream.config.lsm_scan_config import LSMScanConfig, get_lsm_scan_config
from opticstream.events import get_event_trigger
from opticstream.events.lsm_events import STRIP_COMPRESSED, STRIP_UPLOADED
from opticstream.state.milestone_wrappers_lsm import strip_processing_milestone
from opticstream.state.state_guards import (
    force_rerun_from_payload,
)
from opticstream.flows.lsm.utils import strip_ident_from_payload, strip_zarr_output_path
from opticstream.state.lsm_project_state import LSMStripId
from opticstream.tasks.dandi_upload import upload_to_dandi
from opticstream.hooks.slack_notification_hook import slack_notification_hook


@flow(
    flow_run_name="upload-to-dandi-{strip_ident}",
    on_completion=[publish_lsm_slice_hook, publish_lsm_project_hook],
    on_failure=[slack_notification_hook],
)
@strip_processing_milestone(field_name="uploaded", success_event=STRIP_UPLOADED)
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
    upload_to_dandi(output_path, dandi_instance=dandi_instance, dandi_bin=dandi_bin)
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


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
):
    manual = upload_strip_to_dandi_flow.to_deployment(
        name=deployment_name,
        tags=["lsm", "strip", "upload", *list(extra_tags)],
    )
    event = upload_strip_to_dandi_event_flow.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "strip", "upload", *list(extra_tags)],
        triggers=[get_event_trigger(STRIP_COMPRESSED, project_name=project_name)],
    )
    return [manual, event]
