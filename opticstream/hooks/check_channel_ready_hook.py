"""Hook to emit CHANNEL_READY when all strips in a channel are compressed."""

from __future__ import annotations

from typing import Any

from prefect.logging.loggers import flow_run_logger

from opticstream.config.lsm_scan_config import get_lsm_scan_config
from opticstream.events.lsm_event_emitters import emit_channel_lsm_event
from opticstream.events.lsm_events import CHANNEL_READY
from opticstream.state.lsm_project_state import LSMChannelId, LSM_STATE_SERVICE
from opticstream.utils.flow_run_name_parse import (
    missing_required_fields,
    parse_flow_run_name_fields,
)

_REQUIRED_FIELDS = ("project_name", "slice_id", "channel_id")


def check_channel_ready_hook(flow: Any, flow_run: Any, state: Any) -> None:
    """
    Prefect on_completion hook: emit CHANNEL_READY when every strip in the
    channel has been compressed.

    Parses ``project_name``, ``slice_id``, and ``channel_id`` from the flow
    run name, loads the scan config to obtain ``strips_per_slice``, and checks
    the channel state.
    """
    logger = flow_run_logger(flow_run, flow)
    parsed = parse_flow_run_name_fields(flow_run.name or "")
    missing = missing_required_fields(parsed, _REQUIRED_FIELDS)
    if missing:
        logger.info(
            "check_channel_ready_hook skipped for run %s; missing fields: %s",
            flow_run.name,
            ", ".join(missing),
        )
        return

    channel_ident = LSMChannelId(
        project_name=str(parsed["project_name"]),
        slice_id=int(parsed["slice_id"]),
        channel_id=int(parsed["channel_id"]),
    )

    scan_config = get_lsm_scan_config(str(parsed["project_name"]))
    channel_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    if channel_view is None:
        logger.info(
            "check_channel_ready_hook: no channel state for %s", channel_ident
        )
        return

    if channel_view.all_compressed(total_strips=scan_config.strips_per_slice):
        emit_channel_lsm_event(CHANNEL_READY, channel_ident)
        logger.info(
            "check_channel_ready_hook: emitted CHANNEL_READY for %s", channel_ident
        )
