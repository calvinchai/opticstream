"""
OCT-specific project state models and services backed by Prefect Variables.

This module defines:
- OCTBatchState, OCTMosaicState, OCTSliceState, OCTProjectState
- OCT-specific naming helpers for Prefect Variable keys and lock names
- make_oct_store() and OCTProjectStateService, built on the generic project_state_store

Hierarchy (in-memory and persisted JSON):
    project -> slice -> mosaic -> batch
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from pydantic import BaseModel, ConfigDict, Field

from opticstream.utils.naming_convention import get_project_name
from opticstream.state.project_state_core import (
    PrefectProjectLock,
    PrefectVariableProjectStateRepository,
    BaseProjectStateStore,
    ensure_limit,
)


"""
Hierarchy (in-memory and persisted JSON):
    project -> slice -> mosaic -> batch
"""


# ------------------------------------------------------------------------------
# Naming helpers
# ------------------------------------------------------------------------------


def _state_variable_key(project_name: str) -> str:
    """Prefect Variable key where OCT project state JSON is stored."""
    return f"{get_project_name(project_name)}_oct_project_state"


def _state_lock_name(project_name: str) -> str:
    """Global concurrency limit name used for exclusive access to OCT project state."""
    return f"{get_project_name(project_name)}_oct_state_lock"


def ensure_lock(project_name: str) -> None:
    """Ensure the OCT project state lock exists."""
    asyncio.run(ensure_limit(_state_lock_name(project_name), 1))


# ------------------------------------------------------------------------------
# Domain models
# ------------------------------------------------------------------------------


class OCTStateView(BaseModel):
    """Base immutable view for OCT state objects."""

    model_config = ConfigDict(frozen=True)

    project_name: str = Field(..., min_length=1)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class OCTStateMutationsMixin:
    """Common lifecycle helpers for mutable OCT state objects."""

    def touch(self) -> None:
        self.updated_at = datetime.now()


class OCTBatchStateView(OCTStateView):
    """
    Readonly view of a single OCT batch.

    Tracks processing lifecycle flags and timestamps.
    """

    slice_number: int = Field(..., ge=0)
    mosaic_id: int = Field(..., ge=0)
    batch_id: int = Field(..., ge=0)
    started: bool = False
    processed: bool = False
    uploaded: bool = False
    archived: bool = False

    started_at: datetime | None = None
    processed_at: datetime | None = None
    uploaded_at: datetime | None = None
    archived_at: datetime | None = None


class OCTBatchState(OCTStateMutationsMixin, OCTBatchStateView):
    """Mutable OCT batch state."""

    model_config = ConfigDict(frozen=False)

    def mark_started(self) -> None:
        now = datetime.now()
        self.started = True
        self.started_at = now
        self.touch()

    def mark_processed(self) -> None:
        now = datetime.now()
        self.processed = True
        self.processed_at = now
        self.touch()

    def mark_uploaded(self) -> None:
        now = datetime.now()
        self.uploaded = True
        self.uploaded_at = now
        self.touch()

    def mark_archived(self) -> None:
        now = datetime.now()
        self.archived = True
        self.archived_at = now
        self.touch()

    def to_view(self) -> OCTBatchStateView:
        return OCTBatchStateView.model_validate(self.model_dump())


class OCTMosaicStateView(OCTStateView):
    """Readonly view for one OCT mosaic; batches are keyed by batch_id."""

    slice_number: int = Field(..., ge=0)
    mosaic_id: int = Field(..., ge=0)
    batches: dict[int, OCTBatchStateView] = Field(default_factory=dict)

    def iter_batches(self) -> Iterator[OCTBatchStateView]:
        return iter(self.batches.values())


class OCTMosaicState(OCTStateMutationsMixin, OCTMosaicStateView):
    """Mutable OCT mosaic state."""

    model_config = ConfigDict(frozen=False)
    batches: dict[int, OCTBatchState] = Field(default_factory=dict)

    def get_or_create_batch(self, batch_id: int) -> OCTBatchState:
        if batch_id not in self.batches:
            self.batches[batch_id] = OCTBatchState(
                project_name=self.project_name,
                slice_number=self.slice_number,
                mosaic_id=self.mosaic_id,
                batch_id=batch_id,
            )
        return self.batches[batch_id]

    def to_view(self) -> OCTMosaicStateView:
        return OCTMosaicStateView.model_validate(self.model_dump())


class OCTSliceStateView(OCTStateView):
    """Readonly view for one OCT slice; mosaics are keyed by mosaic_id."""

    slice_number: int = Field(..., ge=0)
    mosaics: dict[int, OCTMosaicStateView] = Field(default_factory=dict)

    def iter_mosaics(self) -> Iterator[OCTMosaicStateView]:
        return iter(self.mosaics.values())


class OCTSliceState(OCTStateMutationsMixin, OCTSliceStateView):
    """Mutable OCT slice state."""

    model_config = ConfigDict(frozen=False)
    mosaics: dict[int, OCTMosaicState] = Field(default_factory=dict)

    def get_or_create_mosaic(self, mosaic_id: int) -> OCTMosaicState:
        if mosaic_id not in self.mosaics:
            self.mosaics[mosaic_id] = OCTMosaicState(
                project_name=self.project_name,
                slice_number=self.slice_number,
                mosaic_id=mosaic_id,
            )
        return self.mosaics[mosaic_id]

    def to_view(self) -> OCTSliceStateView:
        return OCTSliceStateView.model_validate(self.model_dump())


class OCTProjectStateView(OCTStateView):
    """
    Readonly view of the entire persisted OCT state for one project.
    """

    schema_version: int = 1
    slices: dict[int, OCTSliceStateView] = Field(default_factory=dict)

    def get_batch(
        self,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchStateView | None:
        slice_state = self.slices.get(slice_number)
        if slice_state is None:
            return None
        mosaic_state = slice_state.mosaics.get(mosaic_id)
        if mosaic_state is None:
            return None
        return mosaic_state.batches.get(batch_id)

    def iter_mosaics(self) -> Iterator[OCTMosaicStateView]:
        for slice_state in self.slices.values():
            yield from slice_state.iter_mosaics()

    def iter_batches(self) -> Iterator[OCTBatchStateView]:
        for mosaic_state in self.iter_mosaics():
            yield from mosaic_state.iter_batches()


class OCTProjectState(OCTStateMutationsMixin, OCTProjectStateView):
    """
    Entire persisted OCT state for one project (mutable form used for storage).
    """

    model_config = ConfigDict(frozen=False)
    slices: dict[int, OCTSliceState] = Field(default_factory=dict)

    def get_or_create_slice(self, slice_number: int) -> OCTSliceState:
        if slice_number not in self.slices:
            self.slices[slice_number] = OCTSliceState(
                project_name=self.project_name,
                slice_number=slice_number,
            )
        return self.slices[slice_number]

    def get_or_create_mosaic(self, slice_number: int, mosaic_id: int) -> OCTMosaicState:
        slice_state = self.get_or_create_slice(slice_number)
        return slice_state.get_or_create_mosaic(mosaic_id)

    def get_or_create_batch(
        self,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchState:
        mosaic_state = self.get_or_create_mosaic(slice_number, mosaic_id)
        return mosaic_state.get_or_create_batch(batch_id)

    def to_view(self) -> OCTProjectStateView:
        return OCTProjectStateView.model_validate(self.model_dump())


    # --------------------------------------------------------------------------
    # OCT-specific ProjectStateStore wiring
    # --------------------------------------------------------------------------


def _make_oct_repository() -> PrefectVariableProjectStateRepository[OCTProjectState]:
    """Factory for the OCT Prefect-backed repository."""
    return PrefectVariableProjectStateRepository(_state_variable_key, OCTProjectState)


def _make_oct_lock() -> PrefectProjectLock:
    """Factory for the OCT Prefect-based project lock."""
    return PrefectProjectLock(_state_lock_name)


OCTProjectStateStore = BaseProjectStateStore[OCTProjectState]


def make_oct_store() -> OCTProjectStateStore:
    """Construct a ProjectStateStore wired to OCT-specific repo and lock."""
    return OCTProjectStateStore(
        repository=_make_oct_repository(),
        lock=_make_oct_lock(),
    )


class OCTProjectStateService:
    """
    OCT-specific state service exposing open/read/peek APIs.
    """

    def __init__(self, store: OCTProjectStateStore | None = None) -> None:
        self._store = store or make_oct_store()

    # ------------------------------------------------------------------
    # Mutable scoped access (open_*)
    # ------------------------------------------------------------------

    @contextmanager
    def open_project(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> Iterator[OCTProjectState]:
        with self._store.open(
            project_name,
            getter=lambda state: state,
            timeout_seconds=timeout_seconds,
        ) as project:
            yield project

    @contextmanager
    def open_slice(
        self,
        project_name: str,
        *,
        slice_number: int,
        timeout_seconds: float | None = None,
    ) -> Iterator[OCTSliceState]:
        with self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_slice(slice_number),
            timeout_seconds=timeout_seconds,
        ) as slice_state:
            yield slice_state

    @contextmanager
    def open_mosaic(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        timeout_seconds: float | None = None,
    ) -> Iterator[OCTMosaicState]:
        with self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_mosaic(slice_number, mosaic_id),
            timeout_seconds=timeout_seconds,
        ) as mosaic:
            yield mosaic

    @contextmanager
    def open_batch(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> Iterator[OCTBatchState]:
        with self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_batch(
                slice_number=slice_number,
                mosaic_id=mosaic_id,
                batch_id=batch_id,
            ),
            timeout_seconds=timeout_seconds,
        ) as batch:
            yield batch

    # ------------------------------------------------------------------
    # Locked readonly access (read_*)
    # ------------------------------------------------------------------

    def read_project(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTProjectStateView:
        return self._store.read(
            project_name,
            reader=lambda state: state.to_view(),
            timeout_seconds=timeout_seconds,
        )

    def read_slice(
        self,
        project_name: str,
        *,
        slice_number: int,
        timeout_seconds: float | None = None,
    ) -> OCTSliceStateView | None:
        def reader(state: OCTProjectState) -> OCTSliceStateView | None:
            slice_state = state.slices.get(slice_number)
            return None if slice_state is None else slice_state.to_view()

        return self._store.read(
            project_name,
            reader=reader,
            timeout_seconds=timeout_seconds,
        )

    def read_mosaic(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTMosaicStateView | None:
        def reader(state: OCTProjectState) -> OCTMosaicStateView | None:
            slice_state = state.slices.get(slice_number)
            if slice_state is None:
                return None
            mosaic_state = slice_state.mosaics.get(mosaic_id)
            return None if mosaic_state is None else mosaic_state.to_view()

        return self._store.read(
            project_name,
            reader=reader,
            timeout_seconds=timeout_seconds,
        )

    def read_batch(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTBatchStateView | None:
        def reader(state: OCTProjectState) -> OCTBatchStateView | None:
            slice_state = state.slices.get(slice_number)
            if slice_state is None:
                return None
            mosaic_state = slice_state.mosaics.get(mosaic_id)
            if mosaic_state is None:
                return None
            batch = mosaic_state.batches.get(batch_id)
            return None if batch is None else batch.to_view()

        return self._store.read(
            project_name,
            reader=reader,
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Unlocked readonly access (peek_*)
    # ------------------------------------------------------------------

    def peek_project(self, project_name: str) -> OCTProjectStateView:
        return self._store.peek(
            project_name,
            reader=lambda state: state.to_view(),
        )

    def peek_slice(
        self,
        project_name: str,
        *,
        slice_number: int,
    ) -> OCTSliceStateView | None:
        def reader(state: OCTProjectState) -> OCTSliceStateView | None:
            slice_state = state.slices.get(slice_number)
            return None if slice_state is None else slice_state.to_view()

        return self._store.peek(project_name, reader=reader)

    def peek_mosaic(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
    ) -> OCTMosaicStateView | None:
        def reader(state: OCTProjectState) -> OCTMosaicStateView | None:
            slice_state = state.slices.get(slice_number)
            if slice_state is None:
                return None
            mosaic_state = slice_state.mosaics.get(mosaic_id)
            return None if mosaic_state is None else mosaic_state.to_view()

        return self._store.peek(project_name, reader=reader)

    def peek_batch(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchStateView | None:
        def reader(state: OCTProjectState) -> OCTBatchStateView | None:
            slice_state = state.slices.get(slice_number)
            if slice_state is None:
                return None
            mosaic_state = slice_state.mosaics.get(mosaic_id)
            if mosaic_state is None:
                return None
            batch = mosaic_state.batches.get(batch_id)
            return None if batch is None else batch.to_view()

        return self._store.peek(project_name, reader=reader)


OCT_STATE_SERVICE = OCTProjectStateService(make_oct_store())

