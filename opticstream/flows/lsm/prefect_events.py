"""Prefect event emission helpers for LSM flows (consistent resource/payload shape)."""

from __future__ import annotations

from prefect.events import emit_event

from opticstream.state.lsm_project_state import LSMChannelId


def emit_channel_lsm_event(event: str, channel_ident: LSMChannelId) -> None:
    """Emit a channel-scoped LSM event with standard resource and payload."""
    emit_event(
        event,
        resource={
            "prefect.resource.id": f"{channel_ident}",
            "linc.opticstream.project": channel_ident.project_name,
        },
        payload={
            "channel_ident": channel_ident.model_dump(mode="json"),
        },
    )
