from typing import Any, Dict

from prefect import flow, get_run_logger, task
from opticstream.events.lsm_events import CHANNEL_READY, STRIP_COMPRESSED
from opticstream.events.lsm_event_emitters import emit_channel_lsm_event
from opticstream.artifacts.lsm import (
    publish_all_slice_matrix_artifacts_task,
    publish_project_artifact_task,
)
from opticstream.flows.lsm.utils import (
    channel_ident_from_strip,
    load_scan_config_for_payload,
    strip_ident_from_payload,
)
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMProjectId,
    LSM_STATE_SERVICE,
)


@task(task_run_name="check-channel-ready-{channel_ident}")
def check_channel_ready(
    channel_ident: LSMChannelId,
    strips_per_slice: int,
) -> None:
    """
    If every strip in the channel has been compressed, emit CHANNEL_READY.
    """
    channel_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    if channel_view is None:
        return
    if channel_view.all_compressed(total_strips=strips_per_slice):
        emit_channel_lsm_event(CHANNEL_READY, channel_ident)


@flow
def on_strip_events(event: str, payload: Dict[str, Any]) -> None:
    """
    Refresh the LSM strip progress Prefect artifact from current project state.

    Deployed as ``lsm_strip_update_event_flow`` on ``STRIP_COMPRESSED`` (see
    ``strip_update_to_deployment``).
    """
    strip_ident = strip_ident_from_payload(payload)
    project_name = strip_ident.project_name
    channel_ident = channel_ident_from_strip(strip_ident)
    cfg = load_scan_config_for_payload(project_name, payload)
    project_ident = LSMProjectId(project_name=project_name)
    override_config = payload.get("override_config")
    artifact_key = publish_project_artifact_task(
        project_ident,
        override_config_name=override_config,
    )
    if artifact_key:
        slice_keys = publish_all_slice_matrix_artifacts_task(
            project_ident,
            override_config_name=override_config,
        )
        get_run_logger().info(
            "Updated LSM strip artifacts: project=%s, slice_artifact_keys=%s",
            artifact_key,
            slice_keys,
        )
    if event == STRIP_COMPRESSED:
        check_channel_ready(channel_ident, cfg.strips_per_slice)


@flow
def lsm_strip_update_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event entrypoint for STRIP_COMPRESSED: artifact table + optional CHANNEL_READY.
    """
    on_strip_events(STRIP_COMPRESSED, payload)


def strip_update_to_deployment():
    """Deployment with STRIP_COMPRESSED trigger (register alongside strip upload)."""
    from opticstream.events import get_event_trigger

    return lsm_strip_update_event_flow.to_deployment(
        name="lsm_strip_update_event_flow",
        triggers=[get_event_trigger(STRIP_COMPRESSED)],
    )
