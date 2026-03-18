"""OCT-specific project state models and services backed by Prefect Variables."""

from __future__ import annotations

import asyncio
from contextlib import AbstractContextManager
from datetime import datetime
from typing import ClassVar, Iterator

from pydantic import BaseModel, ConfigDict, Field

from opticstream.utils.naming_convention import get_project_name
from opticstream.state.project_state_core import (
    BaseProjectStateStore,
    PrefectProjectLock,
    PrefectVariableProjectStateRepository,
    ProcessingState,
    ToViewMixin,
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
    return f"{get_project_name(project_name)}_oct_project_state"


def _state_lock_name(project_name: str) -> str:
    return f"{get_project_name(project_name)}_oct_state_lock"


def ensure_lock(project_name: str) -> None:
    asyncio.run(ensure_limit(_state_lock_name(project_name), 1))


# ------------------------------------------------------------------------------
# Domain models
# ------------------------------------------------------------------------------


def _derive_slice_number_from_mosaic_id(mosaic_id: int) -> int:
    """
    Derive slice_number from mosaic_id when it is not explicitly provided.

    Current convention: slice_number is computed as mosaic_id // 2.
    """
    return mosaic_id // 2


class OCTProjectId(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_name: str = Field(..., min_length=1)


class OCTSliceId(OCTProjectId):
    slice_number: int = Field(..., ge=0)


class OCTMosaicId(OCTProjectId):
    # slice_number is optional because many call sites only provide mosaic_id.
    slice_number: int | None = Field(default=None, ge=0)
    mosaic_id: int = Field(..., ge=0)


class OCTBatchId(OCTMosaicId):
    batch_id: int = Field(..., ge=0)


class OCTStateView(BaseModel):
    """Base immutable view for OCT state objects."""

    model_config = ConfigDict(frozen=True)

    processing_state: ProcessingState = ProcessingState.PENDING
    processing_started_at: datetime | None = None
    processing_finished_at: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    @property
    def finished(self) -> bool:
        return self.processing_finished_at is not None


class OCTStateMutationsMixin:
    """Common lifecycle helpers for mutable OCT state objects."""

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


class OCTBatchStateView(OCTStateView):
    """
    Readonly view of a single OCT batch.

    Tracks processing lifecycle flags and timestamps.
    """

    slice_number: int = Field(..., ge=0)
    mosaic_id: int = Field(..., ge=0)
    batch_id: int = Field(..., ge=0)
    processed: bool = False
    uploaded: bool = False
    archived: bool = False


class OCTBatchState(OCTStateMutationsMixin, OCTBatchStateView, ToViewMixin[OCTBatchStateView]):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[OCTBatchStateView]] = OCTBatchStateView

    def set_processed(self, value: bool = True) -> None:
        self.processed = value
        self.touch()

    def set_uploaded(self, value: bool = True) -> None:
        self.uploaded = value
        self.touch()

    def set_archived(self, value: bool = True) -> None:
        self.archived = value
        self.touch()


class OCTMosaicStateView(OCTStateView):
    """Readonly view for one OCT mosaic; batches are keyed by batch_id."""

    slice_number: int = Field(..., ge=0)
    mosaic_id: int = Field(..., ge=0)
    enface_stitched: bool = False
    volume_stitched: bool = False
    enface_uploaded: bool = False
    volume_uploaded: bool = False
    batches: dict[int, OCTBatchStateView] = Field(default_factory=dict)

    def iter_batches(self) -> Iterator[OCTBatchStateView]:
        return iter(self.batches.values())

    def all_batches_done(self, total_batches: int) -> bool:
        """
        Return True if this mosaic has at least one batch and all batches have finished processing.
        """
        if not self.batches:
            return False

        return all(
            i in self.batches and self.batches[i].finished
            for i in range(1, total_batches + 1)
        )


class OCTMosaicState(
    OCTStateMutationsMixin,
    OCTMosaicStateView,
    ToViewMixin[OCTMosaicStateView],
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[OCTMosaicStateView]] = OCTMosaicStateView
    batches: dict[int, OCTBatchState] = Field(default_factory=dict)

    def get_batch(self, batch_id: int) -> OCTBatchState | None:
        return self.batches.get(batch_id)

    def get_or_create_batch(self, batch_id: int) -> OCTBatchState:
        if batch_id not in self.batches:
            self.batches[batch_id] = OCTBatchState(
                slice_number=self.slice_number,
                mosaic_id=self.mosaic_id,
                batch_id=batch_id,
            )
        return self.batches[batch_id]

    def set_enface_stitched(self, value: bool = True) -> None:
        self.enface_stitched = value
        self.touch()

    def set_volume_stitched(self, value: bool = True) -> None:
        self.volume_stitched = value
        self.touch()

    def set_enface_uploaded(self, value: bool = True) -> None:
        self.enface_uploaded = value
        self.touch()

    def set_volume_uploaded(self, value: bool = True) -> None:
        self.volume_uploaded = value
        self.touch()


class OCTSliceStateView(OCTStateView):
    """Readonly view for one OCT slice; mosaics are keyed by mosaic_id."""

    slice_number: int = Field(..., ge=0)
    mosaics: dict[int, OCTMosaicStateView] = Field(default_factory=dict)
    registered: bool = False
    uploaded: bool = False

    def iter_mosaics(self) -> Iterator[OCTMosaicStateView]:
        return iter(self.mosaics.values())

    def all_mosaics_done(self, total_mosaics: int | None = None) -> bool:
        """
        Return True if the expected number of mosaics for this slice have all finished processing.

        When total_mosaics is not provided, defaults to 2 expected mosaics per slice.
        """
        target = total_mosaics or 2
        if len(self.mosaics) < target:
            return False
        return all(mosaic.finished for mosaic in self.mosaics.values())


class OCTSliceState(
    OCTStateMutationsMixin,
    OCTSliceStateView,
    ToViewMixin[OCTSliceStateView],
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[OCTSliceStateView]] = OCTSliceStateView
    mosaics: dict[int, OCTMosaicState] = Field(default_factory=dict)

    def get_mosaic(self, mosaic_id: int) -> OCTMosaicState | None:
        return self.mosaics.get(mosaic_id)

    def get_or_create_mosaic(self, mosaic_id: int) -> OCTMosaicState:
        if mosaic_id not in self.mosaics:
            self.mosaics[mosaic_id] = OCTMosaicState(
                slice_number=self.slice_number,
                mosaic_id=mosaic_id,
            )
        return self.mosaics[mosaic_id]

    def set_registered(self, value: bool = True) -> None:
        self.registered = value
        self.touch()

    def set_uploaded(self, value: bool = True) -> None:
        self.uploaded = value
        self.touch()


class OCTProjectStateView(OCTStateView):
    """
    Readonly view of the entire persisted OCT state for one project.
    """

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


class OCTProjectState(
    OCTStateMutationsMixin,
    OCTProjectStateView,
    ToViewMixin[OCTProjectStateView],
):
    """
    Entire persisted OCT state for one project (mutable form used for storage).
    """
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[OCTProjectStateView]] = OCTProjectStateView
    slices: dict[int, OCTSliceState] = Field(default_factory=dict)

    def get_slice(self, slice_number: int) -> OCTSliceState | None:
        return self.slices.get(slice_number)

    def get_mosaic(self, slice_number: int, mosaic_id: int) -> OCTMosaicState | None:
        slice_state = self.get_slice(slice_number)
        if slice_state is None:
            return None
        return slice_state.get_mosaic(mosaic_id)

    def get_batch(
        self,
        slice_number: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchState | None:
        mosaic_state = self.get_mosaic(slice_number, mosaic_id)
        if mosaic_state is None:
            return None
        return mosaic_state.get_batch(batch_id)

    def get_or_create_slice(self, slice_number: int) -> OCTSliceState:
        if slice_number not in self.slices:
            self.slices[slice_number] = OCTSliceState(
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


def _get_slice_view(
    state: OCTProjectState,
    *,
    slice_number: int,
) -> OCTSliceStateView | None:
    slice_state = state.slices.get(slice_number)
    return None if slice_state is None else slice_state.to_view()


def _get_mosaic_view(
    state: OCTProjectState,
    *,
    slice_number: int,
    mosaic_id: int,
) -> OCTMosaicStateView | None:
    mosaic_state = state.get_mosaic(slice_number, mosaic_id)
    return None if mosaic_state is None else mosaic_state.to_view()


def _get_batch_view(
    state: OCTProjectState,
    *,
    slice_number: int,
    mosaic_id: int,
    batch_id: int,
) -> OCTBatchStateView | None:
    batch_state = state.get_batch(slice_number, mosaic_id, batch_id)
    return None if batch_state is None else batch_state.to_view()

# ------------------------------------------------------------------------------
# OCT-specific ProjectStateStore wiring
# ------------------------------------------------------------------------------


def _make_oct_repository() -> PrefectVariableProjectStateRepository[OCTProjectState]:
    return PrefectVariableProjectStateRepository(_state_variable_key, OCTProjectState)


def _make_oct_lock() -> PrefectProjectLock:
    return PrefectProjectLock(_state_lock_name)


OCTProjectStateStore = BaseProjectStateStore[OCTProjectState]


def make_oct_store() -> OCTProjectStateStore:
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

    def open_project(
        self,
        project_ident: OCTProjectId | str,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTProjectState]:
        project_name = (
            project_ident
            if isinstance(project_ident, str)
            else project_ident.project_name
        )
        return self.open_project_by_parts(
            project_name=project_name,
            timeout_seconds=timeout_seconds,
        )

    def open_project_by_parts(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTProjectState]:
        return self._store.open(
            project_name,
            getter=lambda state: state,
            timeout_seconds=timeout_seconds,
        )

    def open_slice(
        self,
        slice_ident: OCTSliceId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTSliceState]:
        if slice_ident is not None:
            return self.open_slice_by_parts(
                project_name=slice_ident.project_name,
                slice_number=slice_ident.slice_number,
                timeout_seconds=timeout_seconds,
            )
        if project_name is None or slice_number is None:
            raise ValueError("Provide slice_ident or (project_name, slice_number).")
        return self.open_slice_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            timeout_seconds=timeout_seconds,
        )

    def open_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTSliceState]:
        return self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_slice(slice_number),
            timeout_seconds=timeout_seconds,
        )

    def open_mosaic(
        self,
        mosaic_ident: OCTMosaicId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        mosaic_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTMosaicState]:
        if mosaic_ident is not None:
            return self.open_mosaic_by_parts(
                project_name=mosaic_ident.project_name,
                slice_number=mosaic_ident.slice_number,
                mosaic_id=mosaic_ident.mosaic_id,
                timeout_seconds=timeout_seconds,
            )
        if project_name is None or mosaic_id is None:
            raise ValueError("Provide mosaic_ident or (project_name, mosaic_id).")
        return self.open_mosaic_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            mosaic_id=mosaic_id,
            timeout_seconds=timeout_seconds,
        )

    def open_mosaic_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int | None = None,
        mosaic_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTMosaicState]:
        resolved_slice_number = (
            _derive_slice_number_from_mosaic_id(mosaic_id)
            if slice_number is None
            else slice_number
        )
        return self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_mosaic(
                resolved_slice_number,
                mosaic_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    def open_batch(
        self,
        batch_ident: OCTBatchId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        mosaic_id: int | None = None,
        batch_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTBatchState]:
        if batch_ident is not None:
            return self.open_batch_by_parts(
                project_name=batch_ident.project_name,
                slice_number=batch_ident.slice_number,
                mosaic_id=batch_ident.mosaic_id,
                batch_id=batch_ident.batch_id,
                timeout_seconds=timeout_seconds,
            )
        if (
            project_name is None
            or mosaic_id is None
            or batch_id is None
        ):
            raise ValueError(
                "Provide batch_ident or (project_name, mosaic_id, batch_id)."
            )
        return self.open_batch_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            timeout_seconds=timeout_seconds,
        )

    def open_batch_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int | None = None,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTBatchState]:
        resolved_slice_number = (
            _derive_slice_number_from_mosaic_id(mosaic_id)
            if slice_number is None
            else slice_number
        )
        return self._store.open(
            project_name,
            getter=lambda state: state.get_or_create_batch(
                slice_number=resolved_slice_number,
                mosaic_id=mosaic_id,
                batch_id=batch_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Locked readonly access (read_*)
    # ------------------------------------------------------------------

    def read_project(
        self,
        project_ident: OCTProjectId | str,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTProjectStateView:
        project_name = (
            project_ident
            if isinstance(project_ident, str)
            else project_ident.project_name
        )
        return self.read_project_by_parts(
            project_name=project_name,
            timeout_seconds=timeout_seconds,
        )

    def read_project_by_parts(
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
        slice_ident: OCTSliceId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        timeout_seconds: float | None = None,
    ) -> OCTSliceStateView | None:
        if slice_ident is not None:
            return self.read_slice_by_parts(
                project_name=slice_ident.project_name,
                slice_number=slice_ident.slice_number,
                timeout_seconds=timeout_seconds,
            )
        if project_name is None or slice_number is None:
            raise ValueError("Provide slice_ident or (project_name, slice_number).")
        return self.read_slice_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            timeout_seconds=timeout_seconds,
        )

    def read_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int,
        timeout_seconds: float | None = None,
    ) -> OCTSliceStateView | None:
        return self._store.read(
            project_name,
            reader=lambda state: _get_slice_view(
                state,
                slice_number=slice_number,
            ),
            timeout_seconds=timeout_seconds,
        )

    def read_mosaic(
        self,
        mosaic_ident: OCTMosaicId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        mosaic_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> OCTMosaicStateView | None:
        if mosaic_ident is not None:
            return self.read_mosaic_by_parts(
                project_name=mosaic_ident.project_name,
                slice_number=mosaic_ident.slice_number,
                mosaic_id=mosaic_ident.mosaic_id,
                timeout_seconds=timeout_seconds,
            )
        if project_name is None or mosaic_id is None:
            raise ValueError("Provide mosaic_ident or (project_name, mosaic_id).")
        return self.read_mosaic_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            mosaic_id=mosaic_id,
            timeout_seconds=timeout_seconds,
        )

    def read_mosaic_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int | None = None,
        mosaic_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTMosaicStateView | None:
        resolved_slice_number = (
            _derive_slice_number_from_mosaic_id(mosaic_id)
            if slice_number is None
            else slice_number
        )
        return self._store.read(
            project_name,
            reader=lambda state: _get_mosaic_view(
                state,
                slice_number=resolved_slice_number,
                mosaic_id=mosaic_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    def read_batch(
        self,
        batch_ident: OCTBatchId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        mosaic_id: int | None = None,
        batch_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> OCTBatchStateView | None:
        if batch_ident is not None:
            return self.read_batch_by_parts(
                project_name=batch_ident.project_name,
                slice_number=batch_ident.slice_number,
                mosaic_id=batch_ident.mosaic_id,
                batch_id=batch_ident.batch_id,
                timeout_seconds=timeout_seconds,
            )
        if project_name is None or mosaic_id is None or batch_id is None:
            raise ValueError(
                "Provide batch_ident or (project_name, mosaic_id, batch_id)."
            )
        return self.read_batch_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            timeout_seconds=timeout_seconds,
        )

    def read_batch_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int | None = None,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTBatchStateView | None:
        resolved_slice_number = (
            _derive_slice_number_from_mosaic_id(mosaic_id)
            if slice_number is None
            else slice_number
        )
        return self._store.read(
            project_name,
            reader=lambda state: _get_batch_view(
                state,
                slice_number=resolved_slice_number,
                mosaic_id=mosaic_id,
                batch_id=batch_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Unlocked readonly access (peek_*)
    # ------------------------------------------------------------------

    def peek_project(
        self,
        project_ident: OCTProjectId | str,
    ) -> OCTProjectStateView:
        project_name = (
            project_ident
            if isinstance(project_ident, str)
            else project_ident.project_name
        )
        return self.peek_project_by_parts(project_name=project_name)

    def peek_project_by_parts(
        self,
        project_name: str,
    ) -> OCTProjectStateView:
        return self._store.peek(
            project_name,
            reader=lambda state: state.to_view(),
        )

    def peek_slice(
        self,
        slice_ident: OCTSliceId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
    ) -> OCTSliceStateView | None:
        if slice_ident is not None:
            return self.peek_slice_by_parts(
                project_name=slice_ident.project_name,
                slice_number=slice_ident.slice_number,
            )
        if project_name is None or slice_number is None:
            raise ValueError("Provide slice_ident or (project_name, slice_number).")
        return self.peek_slice_by_parts(
            project_name=project_name,
            slice_number=slice_number,
        )

    def peek_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int,
    ) -> OCTSliceStateView | None:
        return self._store.peek(
            project_name,
            reader=lambda state: _get_slice_view(
                state,
                slice_number=slice_number,
            ),
        )

    def peek_mosaic(
        self,
        mosaic_ident: OCTMosaicId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        mosaic_id: int | None = None,
    ) -> OCTMosaicStateView | None:
        if mosaic_ident is not None:
            return self.peek_mosaic_by_parts(
                project_name=mosaic_ident.project_name,
                slice_number=mosaic_ident.slice_number,
                mosaic_id=mosaic_ident.mosaic_id,
            )
        if project_name is None or mosaic_id is None:
            raise ValueError("Provide mosaic_ident or (project_name, mosaic_id).")
        return self.peek_mosaic_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            mosaic_id=mosaic_id,
        )

    def peek_mosaic_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int | None = None,
        mosaic_id: int,
    ) -> OCTMosaicStateView | None:
        resolved_slice_number = (
            _derive_slice_number_from_mosaic_id(mosaic_id)
            if slice_number is None
            else slice_number
        )
        return self._store.peek(
            project_name,
            reader=lambda state: _get_mosaic_view(
                state,
                slice_number=resolved_slice_number,
                mosaic_id=mosaic_id,
            ),
        )

    def peek_batch(
        self,
        batch_ident: OCTBatchId | None = None,
        *,
        project_name: str | None = None,
        slice_number: int | None = None,
        mosaic_id: int | None = None,
        batch_id: int | None = None,
    ) -> OCTBatchStateView | None:
        if batch_ident is not None:
            return self.peek_batch_by_parts(
                project_name=batch_ident.project_name,
                slice_number=batch_ident.slice_number,
                mosaic_id=batch_ident.mosaic_id,
                batch_id=batch_ident.batch_id,
            )
        if project_name is None or mosaic_id is None or batch_id is None:
            raise ValueError(
                "Provide batch_ident or (project_name, mosaic_id, batch_id)."
            )
        return self.peek_batch_by_parts(
            project_name=project_name,
            slice_number=slice_number,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
        )

    def peek_batch_by_parts(
        self,
        project_name: str,
        *,
        slice_number: int | None = None,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchStateView | None:
        resolved_slice_number = (
            _derive_slice_number_from_mosaic_id(mosaic_id)
            if slice_number is None
            else slice_number
        )
        return self._store.peek(
            project_name,
            reader=lambda state: _get_batch_view(
                state,
                slice_number=resolved_slice_number,
                mosaic_id=mosaic_id,
                batch_id=batch_id,
            ),
        )


OCT_STATE_SERVICE = OCTProjectStateService(make_oct_store())

