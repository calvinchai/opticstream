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
from contextlib import AbstractContextManager
from typing import ClassVar, Iterator

from pydantic import BaseModel, ConfigDict, Field

from opticstream.utils.naming_convention import normalize_project_name
from opticstream.state.project_state_postgres import PostgresProjectStateRepository
from opticstream.state.project_state_core import (
    BaseProjectStateStore,
    PrefectProjectLock,
    ProcessingState,
    ToViewMixin,
    ensure_limit,
)


# ------------------------------------------------------------------------------
# Naming helpers
# ------------------------------------------------------------------------------


LSM_PROJECT_TYPE = "lsm"
STATE_DB_BLOCK_NAME = "opticstream-db"


def _state_lock_name(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}_lsm_state_lock"


def ensure_lock(project_name: str) -> None:
    asyncio.run(ensure_limit(_state_lock_name(project_name), 1))


class LSMProjectId(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_name: str = Field(..., min_length=1)


class LSMSliceId(LSMProjectId):
    slice_id: int = Field(..., ge=0)


class LSMChannelId(LSMSliceId):
    channel_id: int = Field(..., ge=0)


class LSMStripId(LSMChannelId):
    strip_id: int = Field(..., ge=0)


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
    archived: bool = False
    compressed: bool = False
    uploaded: bool = False


class LSMStripState(
    LSMStateMutationsMixin, LSMStripStateView, ToViewMixin[LSMStripStateView]
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[LSMStripStateView]] = LSMStripStateView

    def set_archived(self, value: bool = True) -> None:
        self.archived = value
        self.touch()

    def set_compressed(self, value: bool = True) -> None:
        self.compressed = value
        self.touch()

    def set_uploaded(self, value: bool = True) -> None:
        self.uploaded = value
        self.touch()

    def reset_compressed(self) -> None:
        self.compressed = False
        self.uploaded = False
        self.touch()

    def reset_uploaded(self) -> None:
        self.uploaded = False
        self.touch()

    def reset_archived(self) -> None:
        self.archived = False
        self.touch()


class LSMChannelStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    channel_id: int = Field(1, ge=0)

    strips: dict[int, LSMStripStateView] = Field(default_factory=dict)
    mip_stitched: bool = False
    volume_stitched: bool = False
    volume_uploaded: bool = False

    def all_completed(self, total_strips: int) -> bool:
        return all(
            i in self.strips and self.strips[i].finished
            for i in range(1, total_strips + 1)
        )

    def all_compressed(self, total_strips: int) -> bool:
        return all(
            i in self.strips and self.strips[i].compressed
            for i in range(1, total_strips + 1)
        )


class LSMChannelState(
    LSMStateMutationsMixin,
    LSMChannelStateView,
    ToViewMixin[LSMChannelStateView],
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[LSMChannelStateView]] = LSMChannelStateView
    strips: dict[int, LSMStripState] = Field(default_factory=dict)

    def set_mip_stitched(self, value: bool = True) -> None:
        self.mip_stitched = value
        self.touch()

    def set_volume_stitched(self, value: bool = True) -> None:
        self.volume_stitched = value
        self.touch()

    def reset_mip_stitched(self) -> None:
        self.mip_stitched = False
        self.volume_stitched = False
        self.volume_uploaded = False
        self.touch()

    def reset_volume_stitched(self) -> None:
        self.volume_stitched = False
        self.volume_uploaded = False
        self.touch()

    def set_volume_uploaded(self, value: bool = True) -> None:
        self.volume_uploaded = value
        self.touch()

    def reset_volume_uploaded(self) -> None:
        self.volume_uploaded = False
        self.touch()

    def get_or_create_strip(self, strip_id: int) -> LSMStripState:
        if strip_id not in self.strips:
            self.strips[strip_id] = LSMStripState(
                slice_id=self.slice_id,
                channel_id=self.channel_id,
                strip_id=strip_id,
            )
        return self.strips[strip_id]


class LSMSliceStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    channels: dict[int, LSMChannelStateView] = Field(default_factory=dict)

    def all_finished(self, total_channels: int = 1) -> bool:
        return all(
            i in self.channels and self.channels[i].finished
            for i in range(1, total_channels + 1)
        )


class LSMSliceState(
    LSMStateMutationsMixin, LSMSliceStateView, ToViewMixin[LSMSliceStateView]
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[LSMSliceStateView]] = LSMSliceStateView
    channels: dict[int, LSMChannelState] = Field(default_factory=dict)

    def get_or_create_channel(self, channel_id: int) -> LSMChannelState:
        if channel_id not in self.channels:
            self.channels[channel_id] = LSMChannelState(
                slice_id=self.slice_id,
                channel_id=channel_id,
            )
        return self.channels[channel_id]


class LSMProjectStateView(LSMStateView):
    slices: dict[int, LSMSliceStateView] = Field(default_factory=dict)

    def all_finished(self, total_slices: int = 1) -> bool:
        return all(
            i in self.slices and self.slices[i].finished
            for i in range(1, total_slices + 1)
        )

    def get_slice(self, slice_id: int) -> LSMSliceStateView | None:
        return self.slices.get(slice_id)

    def get_channel_by_parts(
        self,
        slice_id: int,
        channel_id: int,
    ) -> LSMChannelStateView | None:
        slice_state = self.get_slice(slice_id)
        if slice_state is None:
            return None
        return slice_state.channels.get(channel_id)

    def get_strip_by_parts(
        self,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripStateView | None:
        """Return strip view if it exists, else None (read-only helper)."""
        channel_state = self.get_channel_by_parts(
            slice_id=slice_id,
            channel_id=channel_id,
        )
        if channel_state is None:
            return None
        return channel_state.strips.get(strip_id)

    def get_strip(self, strip_ident: LSMStripId) -> LSMStripStateView | None:
        return self.get_strip_by_parts(
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
        )

    def iter_strips(self) -> Iterator[LSMStripStateView]:
        for slice_state in self.slices.values():
            for channel_state in slice_state.channels.values():
                yield from channel_state.strips.values()


class LSMProjectState(
    LSMStateMutationsMixin,
    LSMProjectStateView,
    ToViewMixin[LSMProjectStateView],
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[LSMProjectStateView]] = LSMProjectStateView
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

    def get_or_create_strip_by_parts(
        self,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripState:
        return self.get_or_create_channel(slice_id, channel_id).get_or_create_strip(
            strip_id
        )

    def get_or_create_strip(self, strip_ident: LSMStripId) -> LSMStripState:
        return self.get_or_create_strip_by_parts(
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
        )

    def delete_slice(self, slice_id: int) -> bool:
        if slice_id not in self.slices:
            return False
        del self.slices[slice_id]
        self.touch()
        return True

    def delete_channel(self, slice_id: int, channel_id: int) -> bool:
        slice_state = self.slices.get(slice_id)
        if slice_state is None or channel_id not in slice_state.channels:
            return False
        del slice_state.channels[channel_id]
        self.touch()
        return True

    def delete_strip(self, slice_id: int, channel_id: int, strip_id: int) -> bool:
        slice_state = self.slices.get(slice_id)
        if slice_state is None:
            return False
        channel_state = slice_state.channels.get(channel_id)
        if channel_state is None or strip_id not in channel_state.strips:
            return False
        del channel_state.strips[strip_id]
        self.touch()
        return True


def _get_slice_view(
    state: LSMProjectState,
    *,
    slice_id: int,
) -> LSMSliceStateView | None:
    slice_state = state.slices.get(slice_id)
    return None if slice_state is None else slice_state.to_view()


def _get_channel_view(
    state: LSMProjectState,
    *,
    slice_id: int,
    channel_id: int,
) -> LSMChannelStateView | None:
    slice_state = state.slices.get(slice_id)
    if slice_state is None:
        return None
    channel_state = slice_state.channels.get(channel_id)
    return None if channel_state is None else channel_state.to_view()


def _get_strip_view(
    state: LSMProjectState,
    *,
    slice_id: int,
    strip_id: int,
    channel_id: int = 1,
) -> LSMStripStateView | None:
    slice_state = state.slices.get(slice_id)
    if slice_state is None:
        return None
    channel_state = slice_state.channels.get(channel_id)
    if channel_state is None:
        return None
    strip_state = channel_state.strips.get(strip_id)
    return None if strip_state is None else strip_state.to_view()


# ------------------------------------------------------------------------------
# LSM-specific ProjectStateStore wiring
# ------------------------------------------------------------------------------


def _make_lsm_repository() -> PostgresProjectStateRepository[LSMProjectState]:
    return PostgresProjectStateRepository(
        block_name=STATE_DB_BLOCK_NAME,
        model_cls=LSMProjectState,
        project_type=LSM_PROJECT_TYPE,
        table_name="project_state",
    )


def _make_lsm_lock() -> PrefectProjectLock:
    return PrefectProjectLock(_state_lock_name)


LSMProjectStateStore = BaseProjectStateStore[LSMProjectState]


def make_lsm_store() -> LSMProjectStateStore:
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

    def open_project(
        self,
        project_ident: LSMProjectId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMProjectState]:
        return self.open_project_by_parts(
            project_name=project_ident.project_name,
            timeout_seconds=timeout_seconds,
        )

    def open_project_by_parts(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMProjectState]:
        return self._store.open(
            project_name,
            getter=lambda state: state,
            timeout_seconds=timeout_seconds,
        )

    def open_slice(
        self,
        slice_ident: LSMSliceId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMSliceState]:
        return self.open_slice_by_parts(
            project_name=slice_ident.project_name,
            slice_id=slice_ident.slice_id,
            timeout_seconds=timeout_seconds,
        )

    def open_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMSliceState]:
        return self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_slice(slice_id),
            timeout_seconds=timeout_seconds,
        )

    def open_channel(
        self,
        channel_ident: LSMChannelId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMChannelState]:
        return self.open_channel_by_parts(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
            timeout_seconds=timeout_seconds,
        )

    def open_channel_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMChannelState]:
        return self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_channel(slice_id, channel_id),
            timeout_seconds=timeout_seconds,
        )

    def open_strip(
        self,
        strip_ident: LSMStripId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMStripState]:
        return self.open_strip_by_parts(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
            timeout_seconds=timeout_seconds,
        )

    def open_strip_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMStripState]:
        return self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_strip_by_parts(
                slice_id=slice_id,
                strip_id=strip_id,
                channel_id=channel_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Locked readonly access (read_*)
    # ------------------------------------------------------------------

    def read_project(
        self,
        project_ident: LSMProjectId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMProjectStateView:
        return self.read_project_by_parts(
            project_name=project_ident.project_name,
            timeout_seconds=timeout_seconds,
        )

    def read_project_by_parts(
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
        slice_ident: LSMSliceId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMSliceStateView | None:
        return self.read_slice_by_parts(
            project_name=slice_ident.project_name,
            slice_id=slice_ident.slice_id,
            timeout_seconds=timeout_seconds,
        )

    def read_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> LSMSliceStateView | None:
        return self._store.read(
            project_name,
            reader=lambda state: _get_slice_view(
                state,
                slice_id=slice_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    def read_channel(
        self,
        channel_ident: LSMChannelId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMChannelStateView | None:
        return self.read_channel_by_parts(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
            timeout_seconds=timeout_seconds,
        )

    def read_channel_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
        timeout_seconds: float | None = None,
    ) -> LSMChannelStateView | None:
        return self._store.read(
            project_name,
            reader=lambda state: _get_channel_view(
                state,
                slice_id=slice_id,
                channel_id=channel_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    def read_strip(
        self,
        strip_ident: LSMStripId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMStripStateView | None:
        return self.read_strip_by_parts(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
            timeout_seconds=timeout_seconds,
        )

    def read_strip_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
        timeout_seconds: float | None = None,
    ) -> LSMStripStateView | None:
        return self._store.read(
            project_name,
            reader=lambda state: _get_strip_view(
                state,
                slice_id=slice_id,
                strip_id=strip_id,
                channel_id=channel_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Unlocked readonly access (peek_*)
    # ------------------------------------------------------------------

    def peek_project(self, project_ident: LSMProjectId) -> LSMProjectStateView:
        return self.peek_project_by_parts(project_name=project_ident.project_name)

    def peek_project_by_parts(self, project_name: str) -> LSMProjectStateView:
        return self._store.peek(
            project_name,
            reader=lambda state: state.to_view(),
        )

    def peek_slice(
        self,
        slice_ident: LSMSliceId,
    ) -> LSMSliceStateView | None:
        return self.peek_slice_by_parts(
            project_name=slice_ident.project_name,
            slice_id=slice_ident.slice_id,
        )

    def peek_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
    ) -> LSMSliceStateView | None:
        return self._store.peek(
            project_name,
            reader=lambda state: _get_slice_view(
                state,
                slice_id=slice_id,
            ),
        )

    def peek_channel(
        self,
        channel_ident: LSMChannelId,
    ) -> LSMChannelStateView | None:
        return self.peek_channel_by_parts(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
        )

    def peek_channel_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
    ) -> LSMChannelStateView | None:
        return self._store.peek(
            project_name,
            reader=lambda state: _get_channel_view(
                state,
                slice_id=slice_id,
                channel_id=channel_id,
            ),
        )

    def peek_strip(
        self,
        strip_ident: LSMStripId,
    ) -> LSMStripStateView | None:
        return self.peek_strip_by_parts(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
        )

    def peek_strip_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripStateView | None:
        return self._store.peek(
            project_name,
            reader=lambda state: _get_strip_view(
                state,
                slice_id=slice_id,
                strip_id=strip_id,
                channel_id=channel_id,
            ),
        )


LSM_STATE_SERVICE = LSMProjectStateService(make_lsm_store())
