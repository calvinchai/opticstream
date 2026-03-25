"""
Prefect event emission helpers for PSOCT flows.
"""

from __future__ import annotations

from typing import Any, Mapping

from opticstream.events.event_emitter_core import (
    ScopedEventAdapter,
    emit_scoped_ident_event,
)
from opticstream.state.oct_project_state import OCTBatchId, OCTMosaicId, OCTSliceId


PSOCT_BATCH_EVENT_ADAPTER = ScopedEventAdapter[OCTBatchId](
    build_ident_payload=lambda ident: {
        "batch_ident": ident.model_dump(mode="json"),
    },
)

PSOCT_MOSAIC_EVENT_ADAPTER = ScopedEventAdapter[OCTMosaicId](
    build_ident_payload=lambda ident: {
        "mosaic_ident": ident.model_dump(mode="json"),
    },
)

PSOCT_SLICE_EVENT_ADAPTER = ScopedEventAdapter[OCTSliceId](
    build_ident_payload=lambda ident: {
        "slice_ident": ident.model_dump(mode="json"),
    },
)


def emit_mosaic_psoct_event(
    event: str,
    mosaic_ident: OCTMosaicId,
    *,
    extra_resource: Mapping[str, str] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> None:
    """Emit a mosaic-scoped PSOCT event with standard resource and payload."""
    emit_scoped_ident_event(
        event,
        mosaic_ident,
        adapter=PSOCT_MOSAIC_EVENT_ADAPTER,
        extra_resource=extra_resource,
        extra_payload=extra_payload,
    )


def emit_batch_psoct_event(
    event: str,
    batch_ident: OCTBatchId,
    *,
    extra_resource: Mapping[str, str] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> None:
    emit_scoped_ident_event(
        event,
        batch_ident,
        adapter=PSOCT_BATCH_EVENT_ADAPTER,
        extra_resource=extra_resource,
        extra_payload=extra_payload,
    )


def emit_slice_psoct_event(
    event: str,
    slice_ident: OCTSliceId,
    *,
    extra_resource: Mapping[str, str] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> None:
    emit_scoped_ident_event(
        event,
        slice_ident,
        adapter=PSOCT_SLICE_EVENT_ADAPTER,
        extra_resource=extra_resource,
        extra_payload=extra_payload,
    )
