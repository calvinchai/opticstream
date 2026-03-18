"""
Shared helpers for force_rerun and idempotent LSM flow stages.
"""

from __future__ import annotations

from typing import Any, Callable, Literal, Mapping

from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMStripId,
    LSM_STATE_SERVICE,
)
from opticstream.state.project_state_core import ProcessingState

MilestonePrep = Literal["skip", "proceed"]

IdempotentStage = Literal["skip", "run_after_reset", "run_fresh"]


def force_rerun_from_payload(payload: Mapping[str, Any]) -> bool:
    return bool(payload.get("force_rerun", False))


def skip_top_level_flow(
    processing_state: ProcessingState | None,
    *,
    force_rerun: bool,
    skip_if_running: bool = False,
) -> bool:
    """
    Return True if the top-level flow should no-op (log and return).

    Skips when completed (unless force_rerun). Optionally skips when running.
    """
    if processing_state is None:
        return False
    if processing_state == ProcessingState.COMPLETED and not force_rerun:
        return True
    if skip_if_running and processing_state == ProcessingState.RUNNING and not force_rerun:
        return True
    return False


def idempotent_stage_action(flag_done: bool, force_rerun: bool) -> IdempotentStage:
    """
    Classify idempotent strip/channel stages (compressed, uploaded, volume_stitched, ...).

    - skip: milestone already done and not forcing
    - run_after_reset: done but forcing — caller should reset then run body
    - run_fresh: not done — caller may reset-as-setup then run (e.g. compress always resets)
    """
    if flag_done and not force_rerun:
        return "skip"
    if flag_done and force_rerun:
        return "run_after_reset"
    return "run_fresh"


def prepare_idempotent_channel_milestone(
    logger: Any,
    *,
    milestone_done: bool,
    force_rerun: bool,
    skip_log: str,
    force_log: str,
    channel_ident: LSMChannelId,
    reset_channel: Callable[[Any], None],
) -> MilestonePrep:
    """
    Log skip/force, then reset channel milestone state unless skipping entirely.

    Caller runs work after this returns ``proceed``.
    """
    stage = idempotent_stage_action(milestone_done, force_rerun)
    if stage == "skip":
        logger.info(skip_log)
        return "skip"
    if stage == "run_after_reset":
        logger.info(force_log)
    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        reset_channel(ch)
    return "proceed"


def prepare_idempotent_strip_milestone(
    logger: Any,
    *,
    milestone_done: bool,
    force_rerun: bool,
    skip_log: str,
    force_log: str,
    strip_ident: LSMStripId,
    reset_strip: Callable[[Any], None],
) -> MilestonePrep:
    """Same as channel variant for strip-level milestones (upload, compress, ...)."""
    stage = idempotent_stage_action(milestone_done, force_rerun)
    if stage == "skip":
        logger.info(skip_log)
        return "skip"
    if stage == "run_after_reset":
        logger.info(force_log)
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as s:
        reset_strip(s)
    return "proceed"
