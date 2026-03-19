from __future__ import annotations

from typing import Any, Callable, ParamSpec, TypeVar

from prefect import get_run_logger

from opticstream.events.lsm_event_emitters import (
    emit_channel_lsm_event,
    emit_strip_lsm_event,
)
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMStripId,
    LSM_STATE_SERVICE,
)
from opticstream.state.milestone_runtime import MilestoneAdapter, guarded_milestone
from opticstream.state.milestone_hooks import (
    channel_failure_slack_hook,
    strip_failure_slack_hook,
)
from opticstream.state.oct_project_state import OCTBatchId

P = ParamSpec("P")
R = TypeVar("R")


def _set_state_field_true(state: Any, field_name: str) -> None:
    setter_name = f"set_{field_name}"
    if not hasattr(state, setter_name):
        raise AttributeError(f"{type(state).__name__} has no method {setter_name}()")
    getattr(state, setter_name)(True)


def _channel_set_done(channel_ident: LSMChannelId, field_name: str) -> None:
    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        _set_state_field_true(ch, field_name)


def _strip_set_done(strip_ident: LSMStripId, field_name: str) -> None:
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as st:
        _set_state_field_true(st, field_name)


CHANNEL_MILESTONE_ADAPTER = MilestoneAdapter[LSMChannelId, Any](
    peek_view=lambda channel_ident: LSM_STATE_SERVICE.peek_channel(
        channel_ident=channel_ident
    ),
    set_done=_channel_set_done,
)

STRIP_MILESTONE_ADAPTER = MilestoneAdapter[LSMStripId, Any](
    peek_view=lambda strip_ident: LSM_STATE_SERVICE.peek_strip(
        strip_ident=strip_ident
    ),
    set_done=_strip_set_done,
)


def channel_processing_milestone(
    field_name: str,
    *,
    success_event: str | None = None,
    evaluate_result: Callable[[R], None] | None = None,
    on_success: Callable[[LSMChannelId, R], None] | None = None,
    on_failure: Callable[[LSMChannelId, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    def _get_ident(*args: P.args, **kwargs: P.kwargs) -> LSMChannelId:
        if "channel_ident" not in kwargs:
            raise TypeError("expected keyword argument 'channel_ident'")
        return kwargs["channel_ident"]

    def _combined_success(channel_ident: LSMChannelId, result: R) -> None:
        logger = get_run_logger()
        if success_event is not None:
            emit_channel_lsm_event(success_event, channel_ident)
            logger.info(f"Emitted {success_event} for {channel_ident}")
        if on_success is not None:
            on_success(channel_ident, result)

    return guarded_milestone(
        field_name=field_name,
        get_ident=_get_ident,
        adapter=CHANNEL_MILESTONE_ADAPTER,
        evaluate_result=evaluate_result,
        on_success=_combined_success,
        on_failure=on_failure or channel_failure_slack_hook(field_name),
    )


def strip_processing_milestone(
    field_name: str,
    *,
    success_event: str | None = None,
    evaluate_result: Callable[[R], None] | None = None,
    on_success: Callable[[LSMStripId, R], None] | None = None,
    on_failure: Callable[[LSMStripId, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    def _get_ident(*args: P.args, **kwargs: P.kwargs) -> LSMStripId:
        if "strip_ident" not in kwargs:
            raise TypeError("expected keyword argument 'strip_ident'")
        return kwargs["strip_ident"]

    def _combined_success(strip_ident: LSMStripId, result: R) -> None:
        logger = get_run_logger()
        if success_event is not None:
            emit_strip_lsm_event(success_event, strip_ident)
            logger.info(f"Emitted {success_event} for {strip_ident}")
        if on_success is not None:
            on_success(strip_ident, result)

    return guarded_milestone(
        field_name=field_name,
        get_ident=_get_ident,
        adapter=STRIP_MILESTONE_ADAPTER,
        evaluate_result=evaluate_result,
        on_success=_combined_success,
        on_failure=on_failure or strip_failure_slack_hook(field_name),
    )
