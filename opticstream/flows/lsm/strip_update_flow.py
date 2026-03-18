from datetime import datetime
from typing import Any, Dict, List

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from opticstream.flows.lsm.event import CHANNEL_READY, STRIP_COMPRESSED
from opticstream.flows.lsm.prefect_events import emit_channel_lsm_event
from opticstream.flows.lsm.utils import (
    channel_ident_from_strip,
    load_scan_config_for_payload,
    strip_ident_from_payload,
)
from opticstream.state.lsm_project_state import LSMChannelId, LSMProjectStateView, LSM_STATE_SERVICE
from opticstream.state.project_state_core import ProcessingState


@task(task_run_name="{project_name}-lsm-strip-artifact")
def update_lsm_strip_artifact_task(
    project_name: str,
    state: LSMProjectStateView,
    strips_per_slice: int,
) -> str:
    """
    Publish a Prefect table artifact summarizing LSM strip progress from project state.
    """
    logger = get_run_logger()
    artifact_key = f"{project_name.lower().replace('_', '-')}-lsm-strip-progress"
    table_data: List[Dict[str, Any]] = []
    total_completed_strips = 0
    total_strip_slots = 0

    for slice_id in sorted(state.slices.keys()):
        slice_view = state.slices[slice_id]
        for channel_id in sorted(slice_view.channels.keys()):
            channel_view = slice_view.channels[channel_id]
            total = strips_per_slice
            compressed = archived = uploaded = completed = 0
            for strip_id in range(1, total + 1):
                sv = channel_view.strips.get(strip_id)
                if sv is None:
                    continue
                if sv.compressed:
                    compressed += 1
                if sv.archived:
                    archived += 1
                if sv.uploaded:
                    uploaded += 1
                if sv.processing_state == ProcessingState.COMPLETED:
                    completed += 1
            pct = (completed / total * 100.0) if total else 0.0
            table_data.append(
                {
                    "Slice": slice_id,
                    "Channel": channel_id,
                    "Total Strips": total,
                    "Compressed": compressed,
                    "Archived": archived,
                    "Uploaded": uploaded,
                    "Completed": completed,
                    "Progress": f"{pct:.1f}%",
                }
            )
            total_completed_strips += completed
            total_strip_slots += total

    if not table_data:
        logger.warning(
            f"No slice/channel rows in LSM state for artifact {artifact_key}"
        )
        return ""

    overall_pct = (
        total_completed_strips / total_strip_slots * 100.0
        if total_strip_slots
        else 0.0
    )
    milestones = [
        "✅" if overall_pct >= 25 else "⏳",
        "✅" if overall_pct >= 50 else "⏳",
        "✅" if overall_pct >= 75 else "⏳",
        "✅" if overall_pct >= 100 else "⏳",
    ]
    description = f"""LSM strip processing progress

Project: {project_name}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Overall completed strip slots: {total_completed_strips}/{total_strip_slots} ({overall_pct:.1f}%)

Milestones:
- {milestones[0]} 25% Complete
- {milestones[1]} 50% Complete
- {milestones[2]} 75% Complete
- {milestones[3]} 100% Complete
"""

    create_table_artifact(key=artifact_key, table=table_data, description=description)
    logger.info(f"Updated LSM strip artifact {artifact_key}")
    return artifact_key


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
    state = LSM_STATE_SERVICE.peek_project_by_parts(project_name)
    cfg = load_scan_config_for_payload(project_name, payload)
    update_lsm_strip_artifact_task(
        project_name=project_name,
        state=state,
        strips_per_slice=cfg.strips_per_slice,
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
