"""
LSM-specific project state models and services.

This module defines:
- LSMStripProcessingState, LSMStripId, LSMStripState, LSMChannelState, LSMSliceState, LSMProjectState
- LSM-specific naming helpers for Prefect Variable keys and lock names
- make_lsm_store() and LSMProjectStateService, built on the generic project_state_store

Hierarchy:
    project -> slice -> channel -> strip
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from enum import Enum
from typing import Iterator
from contextlib import contextmanager

from pydantic import BaseModel, ConfigDict, Field

from opticstream.utils.naming_convention import get_project_name
from opticstream.state.project_state_core import (
    PrefectProjectLock,
    PrefectVariableProjectStateRepository,
    BaseProjectStateStore,
    ensure_limit,
)


# ------------------------------------------------------------------------------
# Naming helpers
# ------------------------------------------------------------------------------


def _state_variable_key(project_name: str) -> str:
    """Prefect Variable key where project state JSON is stored."""
    return f"{get_project_name(project_name)}_lsm_project_state"


def _state_lock_name(project_name: str) -> str:
    """Global concurrency limit name used for exclusive access to project state."""
    return f"{get_project_name(project_name)}_lsm_state_lock"

def ensure_lock(project_name: str) -> None:
    """Ensure the LSM project state lock exists."""
    asyncio.run(ensure_limit(_state_lock_name(project_name), 1))

class ProcessingState(str, Enum):
    """Processing state of a single strip."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class LSMStripId(BaseModel):
    """Identity of a strip within a project."""

    slice_id: int = Field(..., ge=0)
    strip_id: int = Field(..., ge=0)
    channel_id: int = Field(1, ge=0)

class LSMStateView(BaseModel):
    model_config = ConfigDict(frozen=True)
    processing_state: ProcessingState = ProcessingState.PENDING
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    processing_started_at: datetime | None = None
    processing_finished_at: datetime | None = None
    @property
    def finished(self) -> bool:
        return self.processing_finished_at is not None


class LSMStateMutationsMixin:
    def touch(self) -> None:
        self.updated_at = datetime.now()
    def mark_started(self) -> None:
        now = datetime.now()
        self.processing_state = ProcessingState.RUNNING
        self.processing_started_at = now
        self.updated_at = now

    def mark_completed(self) -> None:
        now = datetime.now()
        self.processing_state = ProcessingState.COMPLETED
        self.processing_finished_at = now
        self.updated_at = now

    def mark_failed(self) -> None:
        now = datetime.now()
        self.processing_state = ProcessingState.FAILED
        self.processing_finished_at = now
        self.updated_at = now


class LSMStripStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    strip_id: int = Field(..., ge=0)
    channel_id: int = Field(..., ge=0)
    backed_up: bool = False
    compressed: bool = False
    uploaded: bool = False


class LSMStripState(LSMStateMutationsMixin, LSMStripStateView):
    model_config = ConfigDict(frozen=False)

    def set_backed_up(self, value: bool = True) -> None:
        self.backed_up = value
        self.touch()
    
    def set_compressed(self, value: bool = True) -> None:
        self.compressed = value
        self.touch()
    
    def set_uploaded(self, value: bool = True) -> None:
        self.uploaded = value
        self.touch()

    def to_view(self) -> LSMStripStateView:
        return LSMStripStateView.model_validate(self.model_dump())

class LSMChannelStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    channel_id: int = Field(1, ge=0)

    strips: dict[int, LSMStripStateView] = Field(default_factory=dict)
    stitched: bool = False

    def all_finished(self, total_strips: int) -> bool:
        return all(i in self.strips and self.strips[i].finished for i in range(1, total_strips + 1))


class LSMChannelState(LSMStateMutationsMixin, LSMChannelStateView):
    model_config = ConfigDict(frozen=False)
    strips: dict[int, LSMStripState] = Field(default_factory=dict)

    def set_stitched(self, value: bool = True) -> None:
        self.stitched = value
        self.touch()

    def get_or_create_strip(self, strip_id: int) -> LSMStripState:
        if strip_id not in self.strips:
            self.strips[strip_id] = LSMStripState(
                slice_id=self.slice_id,
                channel_id=self.channel_id,
                strip_id=strip_id,
            )
        return self.strips[strip_id]

    def to_view(self) -> LSMChannelStateView:
        return LSMChannelStateView.model_validate(self.model_dump())


class LSMSliceStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    channels: dict[int, LSMChannelStateView] = Field(default_factory=dict)

    def all_finished(self, total_channels: int = 1) -> bool:
        return all(i in self.channels and self.channels[i].finished for i in range(1, total_channels + 1))


class LSMSliceState(LSMStateMutationsMixin, LSMSliceStateView):
    model_config = ConfigDict(frozen=False)
    channels: dict[int, LSMChannelState] = Field(default_factory=dict)

    def get_or_create_channel(self, channel_id: int) -> LSMChannelState:
        if channel_id not in self.channels:
            self.channels[channel_id] = LSMChannelState(
                slice_id=self.slice_id,
                channel_id=channel_id,
            )
        return self.channels[channel_id]

    def to_view(self) -> LSMSliceStateView:
        return LSMSliceStateView.model_validate(self.model_dump())


class LSMProjectStateView(LSMStateView):
    slices: dict[int, LSMSliceStateView] = Field(default_factory=dict)

    def all_finished(self, total_slices: int = 1) -> bool:
        return all(i in self.slices and self.slices[i].finished for i in range(1, total_slices + 1))

    def get_strip(
        self,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripStateView | None:
        """Return strip view if it exists, else None (read-only helper)."""
        slice_state = self.slices.get(slice_id)
        if slice_state is None:
            return None
        channel_state = slice_state.channels.get(channel_id)
        if channel_state is None:
            return None
        return channel_state.strips.get(strip_id)

    def get_strip_by_id(self, strip: LSMStripId) -> LSMStripStateView | None:
        return self.get_strip(
            slice_id=strip.slice_id,
            strip_id=strip.strip_id,
            channel_id=strip.channel_id,
        )

    def iter_strips(self) -> Iterator[LSMStripStateView]:
        for slice_state in self.slices.values():
            for channel_state in slice_state.channels.values():
                yield from channel_state.strips.values()


class LSMProjectState(LSMStateMutationsMixin, LSMProjectStateView):
    model_config = ConfigDict(frozen=False)
    slices: dict[int, LSMSliceState] = Field(default_factory=dict)

    def get_or_create_slice(self, slice_id: int) -> LSMSliceState:
        if slice_id not in self.slices:
            self.slices[slice_id] = LSMSliceState(
                slice_id=slice_id,
            )
        return self.slices[slice_id]

    def get_or_create_channel(self, slice_id: int, channel_id: int) -> LSMChannelState:
        slice_state = self.get_or_create_slice(slice_id)
        return slice_state.get_or_create_channel(channel_id)

    def get_or_create_strip(
        self,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripState:
        """Return existing strip state or create/register a new one."""
        return self.get_or_create_channel(slice_id, channel_id).get_or_create_strip(strip_id)

    def get_or_create_strip_by_id(self, strip: LSMStripId) -> LSMStripState:
        return self.get_or_create_strip(
            slice_id=strip.slice_id,
            strip_id=strip.strip_id,
            channel_id=strip.channel_id,
        )

    def to_view(self) -> LSMProjectStateView:
        return LSMProjectStateView.model_validate(self.model_dump())


# ------------------------------------------------------------------------------
# LSM-specific ProjectStateStore wiring
# ------------------------------------------------------------------------------


def _make_lsm_repository() -> PrefectVariableProjectStateRepository[LSMProjectState]:
    """Factory for the LSM Prefect-backed repository."""
    return PrefectVariableProjectStateRepository(_state_variable_key, LSMProjectState)


def _make_lsm_lock() -> PrefectProjectLock:
    """Factory for the LSM Prefect-based project lock."""
    return PrefectProjectLock(_state_lock_name)


LSMProjectStateStore = BaseProjectStateStore[LSMProjectState]


def make_lsm_store() -> LSMProjectStateStore:
    """Construct a ProjectStateStore wired to LSM-specific repo and lock."""
    return LSMProjectStateStore(
        repository=_make_lsm_repository(),
        lock=_make_lsm_lock(),
    )

class LSMProjectStateService:
    """
    LSM-specific state service exposing open/read/peek APIs.
    """

    def __init__(self, store: LSMProjectStateStore | None = None) -> None:
        self._store = store or make_lsm_store()

    # ------------------------------------------------------------------
    # Mutable scoped access (open_*)
    # ------------------------------------------------------------------

    @contextmanager
    def open_project(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> Iterator[LSMProjectState]:
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
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> Iterator[LSMSliceState]:
        with self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_slice(slice_id),
            timeout_seconds=timeout_seconds,
        ) as slice_state:
            yield slice_state

    @contextmanager
    def open_channel(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
        timeout_seconds: float | None = None,
    ) -> Iterator[LSMChannelState]:
        with self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_channel(slice_id, channel_id),
            timeout_seconds=timeout_seconds,
        ) as channel:
            yield channel

    @contextmanager
    def open_strip(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
        timeout_seconds: float | None = None,
    ) -> Iterator[LSMStripState]:
        with self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_strip(
                slice_id=slice_id,
                strip_id=strip_id,
                channel_id=channel_id,
            ),
            timeout_seconds=timeout_seconds,
        ) as strip:
            yield strip

    # ------------------------------------------------------------------
    # Locked readonly access (read_*)
    # ------------------------------------------------------------------

    def read_project(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMProjectStateView:
        return self._store.read(
            project_name,
            reader=lambda state: state.to_view(),
            timeout_seconds=timeout_seconds,
        )

    def read_slice(
        self,
        project_name: str,
        *,
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> LSMSliceStateView | None:
        def reader(state: LSMProjectState) -> LSMSliceStateView | None:
            slice_state = state.slices.get(slice_id)
            return None if slice_state is None else slice_state.to_view()

        return self._store.read(
            project_name,
            reader=reader,
            timeout_seconds=timeout_seconds,
        )

    def read_channel(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
        timeout_seconds: float | None = None,
    ) -> LSMChannelStateView | None:
        def reader(state: LSMProjectState) -> LSMChannelStateView | None:
            slice_state = state.slices.get(slice_id)
            if slice_state is None:
                return None
            channel_state = slice_state.channels.get(channel_id)
            return None if channel_state is None else channel_state.to_view()

        return self._store.read(
            project_name,
            reader=reader,
            timeout_seconds=timeout_seconds,
        )

    def read_strip(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
        timeout_seconds: float | None = None,
    ) -> LSMStripStateView | None:
        def reader(state: LSMProjectState) -> LSMStripStateView | None:
            slice_state = state.slices.get(slice_id)
            if slice_state is None:
                return None
            channel_state = slice_state.channels.get(channel_id)
            if channel_state is None:
                return None
            strip = channel_state.strips.get(strip_id)
            return None if strip is None else strip.to_view()

        return self._store.read(
            project_name,
            reader=reader,
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Unlocked readonly access (peek_*)
    # ------------------------------------------------------------------

    def peek_project(self, project_name: str) -> LSMProjectStateView:
        return self._store.peek(
            project_name,
            reader=lambda state: state.to_view(),
        )

    def peek_slice(
        self,
        project_name: str,
        *,
        slice_id: int,
    ) -> LSMSliceStateView | None:
        def reader(state: LSMProjectState) -> LSMSliceStateView | None:
            slice_state = state.slices.get(slice_id)
            return None if slice_state is None else slice_state.to_view()

        return self._store.peek(project_name, reader=reader)

    def peek_channel(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
    ) -> LSMChannelStateView | None:
        def reader(state: LSMProjectState) -> LSMChannelStateView | None:
            slice_state = state.slices.get(slice_id)
            if slice_state is None:
                return None
            channel_state = slice_state.channels.get(channel_id)
            return None if channel_state is None else channel_state.to_view()

        return self._store.peek(project_name, reader=reader)

    def peek_strip(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripStateView | None:
        def reader(state: LSMProjectState) -> LSMStripStateView | None:
            slice_state = state.slices.get(slice_id)
            if slice_state is None:
                return None
            channel_state = slice_state.channels.get(channel_id)
            if channel_state is None:
                return None
            strip = channel_state.strips.get(strip_id)
            return None if strip is None else strip.to_view()

        return self._store.peek(project_name, reader=reader)


LSM_STATE_SERVICE = LSMProjectStateService(make_lsm_store())