"""Unit tests for opticstream.state.state_guards."""

import pytest
from types import SimpleNamespace

from opticstream.state.state_guards import (
    RunDecision,
    enter_flow_stage,
    force_rerun_from_payload,
    enter_milestone_stage,
)
from opticstream.state.project_state_core import ProcessingState


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({}, False),
        ({"force_rerun": False}, False),
        ({"force_rerun": True}, True),
        ({"force_rerun": 1}, True),
        ({"force_rerun": 0}, False),
    ],
)
def test_force_rerun_from_payload(payload, expected):
    assert force_rerun_from_payload(payload) is expected


def _mock_logger(monkeypatch):
    class _Logger:
        def info(self, *_args, **_kwargs):
            return None
        def warning(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(
        "opticstream.flows.lsm.state_guards.get_run_logger", lambda: _Logger()
    )


def test_enter_flow_stage_skip_completed(monkeypatch):
    _mock_logger(monkeypatch)
    called = {"started": False}

    decision = enter_flow_stage(
        SimpleNamespace(processing_state=ProcessingState.COMPLETED),
        force_rerun=False,
        skip_if_running=False,
        item_ident="item-1",
        mark_started=lambda: called.__setitem__("started", True),
    )
    assert decision == RunDecision.SKIPPED
    assert called["started"] is False


def test_enter_flow_stage_restarted(monkeypatch):
    _mock_logger(monkeypatch)
    called = {"started": False}

    decision = enter_flow_stage(
        SimpleNamespace(processing_state=ProcessingState.COMPLETED),
        force_rerun=True,
        skip_if_running=False,
        item_ident="item-1",
        mark_started=lambda: called.__setitem__("started", True),
    )
    assert decision == RunDecision.RESTARTED
    assert called["started"] is True


def test_enter_flow_stage_running_and_forced_warns_and_restarts(monkeypatch):
    _mock_logger(monkeypatch)
    called = {"started": False}

    decision = enter_flow_stage(
        SimpleNamespace(processing_state=ProcessingState.RUNNING),
        force_rerun=True,
        skip_if_running=True,
        item_ident="item-1",
        mark_started=lambda: called.__setitem__("started", True),
    )
    assert decision == RunDecision.RESTARTED
    assert called["started"] is True


def test_enter_milestone_stage_skip_when_done(monkeypatch):
    _mock_logger(monkeypatch)
    calls = {"reset": 0}

    decision = enter_milestone_stage(
        item_state_view=SimpleNamespace(uploaded=True),
        item_ident="strip-1",
        field_name="uploaded",
        force_rerun=False,
        reset=lambda: calls.__setitem__("reset", calls["reset"] + 1),
    )
    assert decision == RunDecision.SKIPPED
    assert calls["reset"] == 0


def test_enter_milestone_stage_restarted_when_done_and_forced(monkeypatch):
    _mock_logger(monkeypatch)
    calls = {"reset": 0}

    decision = enter_milestone_stage(
        item_state_view=SimpleNamespace(uploaded=True),
        item_ident="strip-1",
        field_name="uploaded",
        force_rerun=True,
        reset=lambda: calls.__setitem__("reset", calls["reset"] + 1),
    )
    assert decision == RunDecision.RESTARTED
    assert calls["reset"] == 1


def test_enter_milestone_stage_started_when_not_done(monkeypatch):
    _mock_logger(monkeypatch)
    calls = {"reset": 0}

    decision = enter_milestone_stage(
        item_state_view=SimpleNamespace(uploaded=False),
        item_ident="strip-1",
        field_name="uploaded",
        force_rerun=False,
        reset=lambda: calls.__setitem__("reset", calls["reset"] + 1),
    )
    assert decision == RunDecision.STARTED
    assert calls["reset"] == 1
