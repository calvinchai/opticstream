from __future__ import annotations

from typing import Any, Callable, ParamSpec, TypeVar

from prefect import get_run_logger

from opticstream.events.psoct_event_emitters import (
    emit_batch_psoct_event,
    emit_mosaic_psoct_event,
    emit_slice_psoct_event,
)
from opticstream.state.oct_project_state import (
    OCTBatchId,
    OCTMosaicId,
    OCTSliceId,
    OCT_STATE_SERVICE,
)
from opticstream.state.milestone_runtime import MilestoneAdapter, guarded_milestone


P = ParamSpec("P")
R = TypeVar("R")


def _set_state_field_true(state: Any, field_name: str) -> None:
    setter_name = f"set_{field_name}"
    if not hasattr(state, setter_name):
        raise AttributeError(f"{type(state).__name__} has no method {setter_name}()")
    getattr(state, setter_name)(True)


def _oct_batch_set_done(batch_ident: OCTBatchId, field_name: str) -> None:
    with OCT_STATE_SERVICE.open_batch(batch_ident=batch_ident) as batch:
        _set_state_field_true(batch, field_name)


def _oct_mosaic_set_done(mosaic_ident: OCTMosaicId, field_name: str) -> None:
    with OCT_STATE_SERVICE.open_mosaic(mosaic_ident=mosaic_ident) as mosaic:
        _set_state_field_true(mosaic, field_name)


def _oct_slice_set_done(slice_ident: OCTSliceId, field_name: str) -> None:
    with OCT_STATE_SERVICE.open_slice(slice_ident=slice_ident) as sl:
        _set_state_field_true(sl, field_name)


OCT_BATCH_MILESTONE_ADAPTER = MilestoneAdapter[OCTBatchId, Any](
    peek_view=lambda batch_ident: OCT_STATE_SERVICE.peek_batch(batch_ident=batch_ident),
    set_done=_oct_batch_set_done,
)

OCT_MOSAIC_MILESTONE_ADAPTER = MilestoneAdapter[OCTMosaicId, Any](
    peek_view=lambda mosaic_ident: OCT_STATE_SERVICE.peek_mosaic(
        mosaic_ident=mosaic_ident
    ),
    set_done=_oct_mosaic_set_done,
)

OCT_SLICE_MILESTONE_ADAPTER = MilestoneAdapter[OCTSliceId, Any](
    peek_view=lambda slice_ident: OCT_STATE_SERVICE.peek_slice(slice_ident=slice_ident),
    set_done=_oct_slice_set_done,
)


def oct_batch_processing_milestone(
    field_name: str,
    *,
    success_event: str | None = None,
    evaluate_result: Callable[[R], None] | None = None,
    on_success: Callable[[OCTBatchId, R], None] | None = None,
    on_failure: Callable[[OCTBatchId, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    def _get_ident(*args: P.args, **kwargs: P.kwargs) -> OCTBatchId:
        if "batch_id" not in kwargs:
            raise TypeError("expected keyword argument 'batch_id'")
        return kwargs["batch_id"]

    def _combined_success(batch_ident: OCTBatchId, result: R) -> None:
        logger = get_run_logger()
        if success_event is not None:
            emit_batch_psoct_event(success_event, batch_ident)
            logger.info(f"Emitted {success_event} for {batch_ident}")
        if on_success is not None:
            on_success(batch_ident, result)

    return guarded_milestone(
        field_name=field_name,
        get_ident=_get_ident,
        adapter=OCT_BATCH_MILESTONE_ADAPTER,
        evaluate_result=evaluate_result,
        on_success=_combined_success,
        on_failure=on_failure,
    )


def oct_mosaic_processing_milestone(
    field_name: str,
    *,
    success_event: str | None = None,
    evaluate_result: Callable[[R], None] | None = None,
    on_success: Callable[[OCTMosaicId, R], None] | None = None,
    on_failure: Callable[[OCTMosaicId, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    def _get_ident(*args: P.args, **kwargs: P.kwargs) -> OCTMosaicId:
        if "mosaic_ident" not in kwargs:
            raise TypeError("expected keyword argument 'mosaic_ident'")
        return kwargs["mosaic_ident"]

    def _combined_success(mosaic_ident: OCTMosaicId, result: R) -> None:
        logger = get_run_logger()
        if success_event is not None:
            emit_mosaic_psoct_event(success_event, mosaic_ident)
            logger.info(f"Emitted {success_event} for {mosaic_ident}")
        if on_success is not None:
            on_success(mosaic_ident, result)

    return guarded_milestone(
        field_name=field_name,
        get_ident=_get_ident,
        adapter=OCT_MOSAIC_MILESTONE_ADAPTER,
        evaluate_result=evaluate_result,
        on_success=_combined_success,
        on_failure=on_failure,
    )


def oct_slice_processing_milestone(
    field_name: str,
    *,
    success_event: str | None = None,
    evaluate_result: Callable[[R], None] | None = None,
    on_success: Callable[[OCTSliceId, R], None] | None = None,
    on_failure: Callable[[OCTSliceId, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    def _get_ident(*args: P.args, **kwargs: P.kwargs) -> OCTSliceId:
        if "slice_ident" not in kwargs:
            raise TypeError("expected keyword argument 'slice_ident'")
        return kwargs["slice_ident"]

    def _combined_success(slice_ident: OCTSliceId, result: R) -> None:
        logger = get_run_logger()
        if success_event is not None:
            emit_slice_psoct_event(success_event, slice_ident)
            logger.info(f"Emitted {success_event} for {slice_ident}")
        if on_success is not None:
            on_success(slice_ident, result)

    return guarded_milestone(
        field_name=field_name,
        get_ident=_get_ident,
        adapter=OCT_SLICE_MILESTONE_ADAPTER,
        evaluate_result=evaluate_result,
        on_success=_combined_success,
        on_failure=on_failure,
    )
