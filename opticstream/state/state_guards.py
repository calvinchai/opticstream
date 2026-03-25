from contextlib import AbstractContextManager
from enum import Enum
from typing import Any, Callable, Mapping

from prefect import get_run_logger

from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMStripId,
    LSM_STATE_SERVICE,
)
from opticstream.state.oct_project_state import OCTBatchId, OCTMosaicId, OCT_STATE_SERVICE
from opticstream.state.project_state_core import ProcessingState


def force_rerun_from_payload(payload: Mapping[str, Any]) -> bool:
    return bool(payload.get("force_rerun", False))


class RunDecision(str, Enum):
    SKIPPED = "skipped"
    STARTED = "started"
    RESTARTED = "restarted"


def _default_state_opener(
    *,
    item_ident: Any,
) -> Callable[[], AbstractContextManager[Any]]:
    if isinstance(item_ident, LSMStripId):
        return lambda: LSM_STATE_SERVICE.open_strip(strip_ident=item_ident)
    if isinstance(item_ident, LSMChannelId):
        return lambda: LSM_STATE_SERVICE.open_channel(channel_ident=item_ident)
    if isinstance(item_ident, OCTBatchId):
        return lambda: OCT_STATE_SERVICE.open_batch(batch_ident=item_ident)
    if isinstance(item_ident, OCTMosaicId):
        return lambda: OCT_STATE_SERVICE.open_mosaic(mosaic_ident=item_ident)
    raise TypeError(
        f"unsupported item_ident type for default open_state: {type(item_ident)!r}"
    )


def _default_mark_started(
    *,
    item_ident: Any,
) -> Callable[[], None]:
    def _mark_started() -> None:
        opener = _default_state_opener(item_ident=item_ident)
        with opener() as state:
            if not hasattr(state, "mark_started"):
                raise AttributeError(
                    f"{type(state).__name__} has no method mark_started()"
                )
            state.mark_started()

    return _mark_started


def _default_reset_milestone(
    *,
    item_ident: Any,
    field_name: str,
) -> Callable[[], None]:
    def _reset() -> None:
        opener = _default_state_opener(item_ident=item_ident)
        reset_method = f"reset_{field_name}"
        with opener() as state:
            if not hasattr(state, reset_method):
                raise AttributeError(
                    f"{type(state).__name__} has no method {reset_method}()"
                )
            getattr(state, reset_method)()

    return _reset


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
    """
    logger = get_run_logger()
    label = str(item_ident)

    processing_state: ProcessingState | None = (
        item_state_view.processing_state if item_state_view is not None else None
    )

    mark_started = mark_started or _default_mark_started(item_ident=item_ident)

    if processing_state is None:
        mark_started()
        return RunDecision.STARTED

    if processing_state == ProcessingState.COMPLETED:
        if not force_rerun:
            logger.info(
                f"{label} already completed; "
                "skipping processing because force_rerun is False"
            )
            return RunDecision.SKIPPED
        logger.info(f"{label} already completed; forcing rerun")
        mark_started()
        return RunDecision.RESTARTED

    if skip_if_running and processing_state == ProcessingState.RUNNING:
        if not force_rerun:
            logger.info(
                f"{label} already running; "
                "skipping processing because force_rerun is False"
            )
            return RunDecision.SKIPPED
        logger.warning(
            f"{label} already running; force_rerun=True may cause duplicate processing"
        )
        mark_started()
        return RunDecision.RESTARTED

    mark_started()
    logger.info(f"{label} started")
    return RunDecision.STARTED


def enter_milestone_stage(
    *,
    item_state_view: Any | None,
    item_ident: Any,
    field_name: str,
    force_rerun: bool,
    reset: Callable[[], None] | None = None,
) -> RunDecision:
    """
    Decide whether a milestone stage should run.

    If the milestone is already complete and ``force_rerun`` is False, skip.
    Otherwise, reset the milestone before execution, because re-entering the
    stage invalidates any previously produced artifact until the stage succeeds
    again.
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

    reset = reset or _default_reset_milestone(
        item_ident=item_ident,
        field_name=field_name,
    )
    reset()

    return RunDecision.RESTARTED if is_rerun else RunDecision.STARTED


def should_skip_run(run_decision: RunDecision) -> bool:
    return run_decision == RunDecision.SKIPPED
