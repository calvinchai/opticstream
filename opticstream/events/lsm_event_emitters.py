"""
Prefect event emission helpers for LSM flows.
"""

from __future__ import annotations

from typing import Any, Mapping

from opticstream.events.event_emitter_core import ScopedEventAdapter, emit_scoped_ident_event
from opticstream.state.lsm_project_state import LSMChannelId, LSMStripId


LSM_CHANNEL_EVENT_ADAPTER = ScopedEventAdapter[LSMChannelId](
    build_ident_payload=lambda ident: {
        "channel_ident": ident.model_dump(mode="json"),
    },
)

LSM_STRIP_EVENT_ADAPTER = ScopedEventAdapter[LSMStripId](
    build_ident_payload=lambda ident: {
        "strip_ident": ident.model_dump(mode="json"),
    },
)


def emit_channel_lsm_event(
    event: str,
    channel_ident: LSMChannelId,
    *,
    extra_resource: Mapping[str, str] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> None:
    """Emit a channel-scoped LSM event with standard resource and payload."""
    emit_scoped_ident_event(
        event,
        channel_ident,
        adapter=LSM_CHANNEL_EVENT_ADAPTER,
        extra_resource=extra_resource,
        extra_payload=extra_payload,
    )


def emit_strip_lsm_event(
    event: str,
    strip_ident: LSMStripId,
    *,
    extra_resource: Mapping[str, str] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> None:
    """Emit a strip-scoped LSM event with standard resource and payload."""
    emit_scoped_ident_event(
        event,
        strip_ident,
        adapter=LSM_STRIP_EVENT_ADAPTER,
        extra_resource=extra_resource,
        extra_payload=extra_payload,
    )
