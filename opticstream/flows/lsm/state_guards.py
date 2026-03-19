"""
Shared helpers for force_rerun and idempotent LSM flow stages.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from enum import Enum
from typing import Any, Callable, Mapping

from prefect import get_run_logger

from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMStripId,
    LSM_STATE_SERVICE,
)
from opticstream.state.project_state_core import ProcessingState


def force_rerun_from_payload(payload: Mapping[str, Any]) -> bool:
    return bool(payload.get("force_rerun", False))


class RunDecision(str, Enum):
    SKIPPED = "skipped"
    STARTED = "started"
    RESTARTED = "restarted"


def enter_flow_stage(
    item_state_view: Any | None,
    *,
    force_rerun: bool,
    skip_if_running: bool,
    item_ident: Any,
    mark_started: Callable[[], None] | None = None,
) -> RunDecision:
    """
    Decide whether a top-level flow should run, log the decision, and mark started.

    This is intentionally small and verbose-friendly: the caller can pass the full
    ident object (e.g. ``LSMStripId``) and we use ``str(entity)`` for logging.
    """
    logger = get_run_logger()
    label = str(item_ident)

    processing_state: ProcessingState | None = (
        item_state_view.processing_state if item_state_view is not None else None
    )

    def _default_mark_started() -> None:
        opener = _default_open_state(item_ident=item_ident)
        with opener() as state:
            if not hasattr(state, "mark_started"):
                raise AttributeError(f"{type(state).__name__} has no method mark_started()")
            state.mark_started()

    if mark_started is None:
        mark_started = _default_mark_started

    if processing_state is None:
        mark_started()
        return RunDecision.STARTED

    if processing_state == ProcessingState.COMPLETED and not force_rerun:
        logger.info(
            f"{label} already completed; "
            "skipping processing because force_rerun is False"
        )
        return RunDecision.SKIPPED

    if skip_if_running:
        if processing_state == ProcessingState.RUNNING and not force_rerun:
            logger.info(
                f"{label} already running; "
                "skipping processing because force_rerun is False"
            )
            return RunDecision.SKIPPED
        else:
            logger.warning(f"{label} already running; you may want to check the running flow to avoid duplicate processing")
    decision = RunDecision.STARTED
    if processing_state == ProcessingState.COMPLETED and force_rerun:
        logger.info(f"{label} already completed; forcing rerun")
        decision = RunDecision.RESTARTED

    mark_started()
    return decision


def _default_open_state(
    *,
    item_ident: Any,
) -> Callable[[], AbstractContextManager[Any]]:
    if isinstance(item_ident, LSMStripId):
        return lambda: LSM_STATE_SERVICE.open_strip(strip_ident=item_ident)
    if isinstance(item_ident, LSMChannelId):
        return lambda: LSM_STATE_SERVICE.open_channel(channel_ident=item_ident)
    raise TypeError(
        f"enter_milestone_stage: unsupported item_ident type for default open_state: {type(item_ident)!r}"
    )


def enter_milestone_stage(
    *,
    item_state_view: Any | None,
    item_ident: Any,
    field_name: str,
    force_rerun: bool,
    open_state: Callable[[], AbstractContextManager[Any]] | None = None,
) -> RunDecision:
    """
    Generic idempotent milestone gate/reset using item view + milestone field name.
    """
    logger = get_run_logger()
    label = str(item_ident)
    milestone_done = bool(
        item_state_view is not None and getattr(item_state_view, field_name, False)
    )
    if milestone_done and not force_rerun:
        logger.info(
            f"{label} already marked {field_name}; "
            "skipping because force_rerun is False"
        )
        return RunDecision.SKIPPED
    is_rerun = milestone_done and force_rerun
    if is_rerun:
        logger.info(f"{label} already marked {field_name}; forcing rerun")

    opener = open_state or _default_open_state(item_ident=item_ident)
    reset_method = f"reset_{field_name}"
    with opener() as state:
        if not hasattr(state, reset_method):
            raise AttributeError(
                f"{type(state).__name__} has no method {reset_method}()"
            )
        getattr(state, reset_method)()

    if is_rerun:
        return RunDecision.RESTARTED
    return RunDecision.STARTED
