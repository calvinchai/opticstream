from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from prefect import get_run_logger

from opticstream.state.state_guards import RunDecision, enter_milestone_stage

P = ParamSpec("P")
R = TypeVar("R")
TIdent = TypeVar("TIdent")
TStateView = TypeVar("TStateView")


@dataclass(frozen=True)
class MilestoneAdapter(Generic[TIdent, TStateView]):
    peek_view: Callable[[TIdent], TStateView | None]
    set_done: Callable[[TIdent, str], None]


def guarded_milestone(
    *,
    field_name: str,
    get_ident: Callable[..., TIdent],
    adapter: MilestoneAdapter[TIdent, Any],
    evaluate_result: Callable[[R], None] | None = None,
    on_success: Callable[[TIdent, R], None] | None = None,
    on_failure: Callable[[TIdent, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    """
    Generic milestone wrapper.

    Semantics:
    - skipped execution returns None
    - func may raise to signal failure
    - evaluate_result(result) may also raise to signal failure
    - on success, mark milestone done then run on_success
    - on failure, run on_failure then re-raise
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R | None]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
            logger = get_run_logger()

            item_ident = get_ident(*args, **kwargs)
            force_rerun = bool(kwargs.get("force_rerun", False))
            item_state_view = adapter.peek_view(item_ident)

            decision = enter_milestone_stage(
                item_state_view=item_state_view,
                item_ident=item_ident,
                field_name=field_name,
                force_rerun=force_rerun,
            )
            if decision == RunDecision.SKIPPED:
                logger.info(f"{item_ident} skipped milestone {field_name}")
                return None

            try:
                result = func(*args, **kwargs)
                if evaluate_result is not None:
                    evaluate_result(result)
            except Exception as exc:
                if on_failure is not None:
                    try:
                        on_failure(item_ident, exc)
                    except Exception:
                        logger.exception(
                            f"Failure hook failed for {item_ident} field={field_name}"
                        )
                raise

            adapter.set_done(item_ident, field_name)

            if on_success is not None:
                on_success(item_ident, result)

            logger.info(f"{item_ident} completed milestone {field_name}")
            return result

        return wrapper

    return decorator
