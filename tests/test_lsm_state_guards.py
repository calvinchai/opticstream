"""Unit tests for opticstream.flows.lsm.state_guards."""

import pytest

from opticstream.flows.lsm.state_guards import (
    force_rerun_from_payload,
    idempotent_stage_action,
    skip_top_level_flow,
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


@pytest.mark.parametrize(
    "state,force,skip_running,expect_skip",
    [
        (None, False, False, False),
        (None, True, True, False),
        (ProcessingState.PENDING, False, False, False),
        (ProcessingState.RUNNING, False, False, False),
        (ProcessingState.RUNNING, False, True, True),
        (ProcessingState.RUNNING, True, True, False),
        (ProcessingState.COMPLETED, False, False, True),
        (ProcessingState.COMPLETED, True, False, False),
        (ProcessingState.COMPLETED, False, True, True),
        (ProcessingState.FAILED, False, False, False),
    ],
)
def test_skip_top_level_flow(state, force, skip_running, expect_skip):
    assert (
        skip_top_level_flow(state, force_rerun=force, skip_if_running=skip_running)
        is expect_skip
    )


@pytest.mark.parametrize(
    "flag_done,force,expected",
    [
        (False, False, "run_fresh"),
        (False, True, "run_fresh"),
        (True, False, "skip"),
        (True, True, "run_after_reset"),
    ],
)
def test_idempotent_stage_action(flag_done, force, expected):
    assert idempotent_stage_action(flag_done, force) == expected
