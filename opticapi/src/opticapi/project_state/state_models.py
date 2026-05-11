"""Shared project-state enums and view mixin (no Prefect)."""

from __future__ import annotations

from enum import Enum
from typing import ClassVar, Generic, TypeVar

from pydantic import BaseModel

TView = TypeVar("TView", bound=BaseModel)


class ToViewMixin(Generic[TView]):
    VIEW_MODEL: ClassVar[type[TView]]

    def to_view(self) -> TView:
        return self.VIEW_MODEL.model_validate(self.model_dump())


class ProcessingState(str, Enum):
    """Generic processing lifecycle state shared by project-state models."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
