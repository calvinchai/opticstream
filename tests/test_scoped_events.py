from __future__ import annotations

import pytest

from opticstream.events.event_emitter_core import (
    ScopedEventAdapter,
    emit_scoped_ident_event,
)


class _FakeIdent:
    def __init__(self, project_name: str, ident: str) -> None:
        self.project_name = project_name
        self._ident = ident

    def __str__(self) -> str:
        return self._ident


def test_emit_scoped_ident_event_merges_shared_adapter_and_extra_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: dict[str, object] = {}

    def _fake_emit_event(event: str, *, resource: dict[str, str], payload: dict[str, object]) -> None:
        emitted["event"] = event
        emitted["resource"] = resource
        emitted["payload"] = payload

    monkeypatch.setattr("opticstream.events.event_emitter_core.emit_event", _fake_emit_event)

    ident = _FakeIdent(project_name="proj-a", ident="proj-a/slice-1/ch-1")
    adapter = ScopedEventAdapter[_FakeIdent](
        build_ident_payload=lambda i: {"channel_ident": {"id": str(i)}},
        build_default_resource=lambda _i: {"linc.opticstream.system": "lsm"},
        build_default_payload=lambda _i: {"pipeline_step": "volume-stitch"},
    )

    emit_scoped_ident_event(
        "linc.opticstream.lsm.channel.volume_stitched",
        ident,
        adapter=adapter,
        extra_resource={"host": "node-01"},
        extra_payload={"volume_path": "/tmp/out.zarr"},
    )

    assert emitted["event"] == "linc.opticstream.lsm.channel.volume_stitched"
    assert emitted["resource"] == {
        "prefect.resource.id": "proj-a/slice-1/ch-1",
        "linc.opticstream.project": "proj-a",
        "linc.opticstream.system": "lsm",
        "host": "node-01",
    }
    assert emitted["payload"] == {
        "channel_ident": {"id": "proj-a/slice-1/ch-1"},
        "pipeline_step": "volume-stitch",
        "volume_path": "/tmp/out.zarr",
    }


def test_emit_scoped_ident_event_rejects_shared_resource_override() -> None:
    ident = _FakeIdent(project_name="proj-a", ident="proj-a/slice-1/ch-1")
    adapter = ScopedEventAdapter[_FakeIdent](
        build_ident_payload=lambda _i: {"channel_ident": {"id": "x"}},
    )

    with pytest.raises(ValueError, match="extra_resource cannot override shared resource keys"):
        emit_scoped_ident_event(
            "evt",
            ident,
            adapter=adapter,
            extra_resource={"prefect.resource.id": "override"},
        )


def test_emit_scoped_ident_event_rejects_payload_override() -> None:
    ident = _FakeIdent(project_name="proj-a", ident="proj-a/slice-1/ch-1")
    adapter = ScopedEventAdapter[_FakeIdent](
        build_ident_payload=lambda _i: {"channel_ident": {"id": "x"}},
    )

    with pytest.raises(ValueError, match="extra_payload cannot override base payload keys"):
        emit_scoped_ident_event(
            "evt",
            ident,
            adapter=adapter,
            extra_payload={"channel_ident": {"id": "override"}},
        )
