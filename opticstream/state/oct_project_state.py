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

from datetime import datetime
from typing import Iterator

from pydantic import BaseModel, Field

from opticstream.utils.naming_convention import get_project_name
from opticstream.state.project_state_store import (
    PrefectProjectLock,
    PrefectVariableProjectStateRepository,
    ProjectStateStore as BaseProjectStateStore,
    ensure_limit,
)


# ------------------------------------------------------------------------------
# Naming helpers
# ------------------------------------------------------------------------------


def _state_variable_key(project_name: str) -> str:
    """Prefect Variable key where OCT project state JSON is stored."""
    return f"{get_project_name(project_name)}_oct_project_state"


def _state_lock_name(project_name: str) -> str:
    """Global concurrency limit name used for exclusive access to OCT project state."""
    return f"{get_project_name(project_name)}_oct_state_lock"


# ------------------------------------------------------------------------------
# Domain models
# ------------------------------------------------------------------------------


class OCTBatchState(BaseModel):
    """
    State of a single OCT batch.

    Tracks processing lifecycle flags and timestamps.
    """

    batch_id: int = Field(..., ge=0)

    started: bool = False
    processed: bool = False
    uploaded: bool = False
    archived: bool = False

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    started_at: datetime | None = None
    processed_at: datetime | None = None
    uploaded_at: datetime | None = None
    archived_at: datetime | None = None

    def _touch(self) -> None:
        self.updated_at = datetime.now()

    def mark_started(self) -> None:
        now = datetime.now()
        self.started = True
        self.started_at = now
        self._touch()

    def mark_processed(self) -> None:
        now = datetime.now()
        self.processed = True
        self.processed_at = now
        self._touch()

    def mark_uploaded(self) -> None:
        now = datetime.now()
        self.uploaded = True
        self.uploaded_at = now
        self._touch()

    def mark_archived(self) -> None:
        now = datetime.now()
        self.archived = True
        self.archived_at = now
        self._touch()


class OCTMosaicState(BaseModel):
    """State for one OCT mosaic; batches are keyed by batch_id."""

    mosaic_id: int = Field(..., ge=0)
    batches: dict[int, OCTBatchState] = Field(default_factory=dict)

    def get_or_create_batch(self, batch_id: int) -> OCTBatchState:
        if batch_id not in self.batches:
            self.batches[batch_id] = OCTBatchState(batch_id=batch_id)
        return self.batches[batch_id]

    def iter_batches(self) -> Iterator[OCTBatchState]:
        return iter(self.batches.values())


class OCTSliceState(BaseModel):
    """State for one OCT slice; mosaics are keyed by mosaic_id."""

    slice_number: int = Field(..., ge=0)
    mosaics: dict[int, OCTMosaicState] = Field(default_factory=dict)

    def get_or_create_mosaic(self, mosaic_id: int) -> OCTMosaicState:
        if mosaic_id not in self.mosaics:
            self.mosaics[mosaic_id] = OCTMosaicState(mosaic_id=mosaic_id)
        return self.mosaics[mosaic_id]

    def iter_mosaics(self) -> Iterator[OCTMosaicState]:
        return iter(self.mosaics.values())


class OCTProjectState(BaseModel):
    """
    Entire persisted OCT state for one project.

    Pure domain model:
    - no Prefect imports
    - no storage logic
    - safe to unit test directly
    """

    schema_version: int = 1
    project_name: str = Field(..., min_length=1)
    slices: dict[int, OCTSliceState] = Field(default_factory=dict)

    def get_or_create_slice(self, slice_number: int) -> OCTSliceState:
        if slice_number not in self.slices:
            self.slices[slice_number] = OCTSliceState(slice_number=slice_number)
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

    def iter_mosaics(self) -> Iterator[OCTMosaicState]:
        for slice_state in self.slices.values():
            yield from slice_state.iter_mosaics()

    def iter_batches(self) -> Iterator[OCTBatchState]:
        for mosaic_state in self.iter_mosaics():
            yield from mosaic_state.iter_batches()


# ------------------------------------------------------------------------------
# OCT-specific ProjectStateStore wiring
# ------------------------------------------------------------------------------


def _make_oct_repository() -> PrefectVariableProjectStateRepository[OCTProjectState]:
    """Factory for the OCT Prefect-backed repository."""
    return PrefectVariableProjectStateRepository(_state_variable_key, OCTProjectState)


def _make_oct_lock() -> PrefectProjectLock:
    """Factory for the OCT Prefect-based project lock."""
    return PrefectProjectLock(_state_lock_name)


ProjectStateStore = BaseProjectStateStore[OCTProjectState]


def make_oct_store() -> ProjectStateStore:
    """Construct a ProjectStateStore wired to OCT-specific repo and lock."""
    return ProjectStateStore(
        repository=_make_oct_repository(),
        lock=_make_oct_lock(),
    )


class OCTProjectStateService:
    """
    High-level OCT project state command API.

    Flows should use this to avoid open-coded state mutations.
    """

    def __init__(self, store: ProjectStateStore | None = None) -> None:
        self._store = store or make_oct_store()

    def _with_batch(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTBatchState:
        # Convenience for internal use: get or create the batch under lock.
        with self._store.locked(project_name, timeout_seconds=timeout_seconds) as state:
            return state.get_or_create_batch(slice_number, mosaic_id, batch_id)

    def mark_batch_started(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> None:
        with self._store.locked(project_name, timeout_seconds=timeout_seconds) as state:
            batch = state.get_or_create_batch(slice_number, mosaic_id, batch_id)
            batch.mark_started()

    def mark_batch_processed(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> None:
        with self._store.locked(project_name, timeout_seconds=timeout_seconds) as state:
            batch = state.get_or_create_batch(slice_number, mosaic_id, batch_id)
            batch.mark_processed()

    def mark_batch_uploaded(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> None:
        with self._store.locked(project_name, timeout_seconds=timeout_seconds) as state:
            batch = state.get_or_create_batch(slice_number, mosaic_id, batch_id)
            batch.mark_uploaded()

    def mark_batch_archived(
        self,
        project_name: str,
        *,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> None:
        with self._store.locked(project_name, timeout_seconds=timeout_seconds) as state:
            batch = state.get_or_create_batch(slice_number, mosaic_id, batch_id)
            batch.mark_archived()

