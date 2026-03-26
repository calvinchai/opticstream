from __future__ import annotations

from typing import Literal

from opticstream.state.project_state_core import ProcessingState

MarkStatus = Literal["pending", "running", "completed", "failed"]
MARK_STATUS_FIELDS: set[str] = {"pending", "running", "completed", "failed"}


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean value (true/false), got: {value!r}")


def apply_mark_status(target: object, status: MarkStatus) -> None:
    if status == "pending":
        if hasattr(target, "processing_state"):
            target.processing_state = ProcessingState.PENDING  # type: ignore[attr-defined]
            target.processing_started_at = None  # type: ignore[attr-defined]
            target.processing_finished_at = None  # type: ignore[attr-defined]
            target.touch()  # type: ignore[attr-defined]
            return
        raise ValueError("Target does not support pending state updates.")

    method_name = {
        "running": "mark_started",
        "completed": "mark_completed",
        "failed": "mark_failed",
    }[status]
    method = getattr(target, method_name, None)
    if method is None:
        raise ValueError(f"Target does not support status update: {status}")
    method()


def apply_mark_field(target: object, field: str, value: str) -> None:
    setter = getattr(target, f"set_{field}", None)
    if setter is None:
        raise ValueError(f"Unknown or unsupported field for this hierarchy: {field!r}")
    setter(parse_bool(value))


def validate_mark_field_and_value(field: str, value: str | None) -> bool:
    is_status = field in MARK_STATUS_FIELDS
    if not is_status and value is None:
        raise ValueError("`value` is required for non-status fields (e.g. true/false).")
    if is_status and value is not None:
        raise ValueError("Do not provide `value` for status fields like `completed`.")
    return is_status
