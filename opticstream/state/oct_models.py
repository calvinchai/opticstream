"""OCT-specific project state models."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Iterator

from pydantic import BaseModel, ConfigDict, Field

from opticstream.utils.naming_convention import normalize_project_name
from opticstream.state.project_state_core import (
    ProcessingState,
    ToViewMixin,
)


OCT_PROJECT_TYPE = "oct"
STATE_REDIS_BLOCK_NAME = "opticstream-redis"


def _state_lock_name(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}_oct_state_lock"


def ensure_lock(project_name: str) -> None:
    """No-op — Redis locks are created on demand."""


def _derive_slice_id_from_mosaic_id(mosaic_id: int) -> int:
    return mosaic_id // 2


class OCTProjectId(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_name: str = Field(..., min_length=1)


class OCTSliceId(OCTProjectId):
    slice_id: int = Field(..., ge=0)


class OCTMosaicId(OCTSliceId):
    mosaic_id: int = Field(..., ge=0)


class OCTBatchId(OCTMosaicId):
    batch_id: int = Field(..., ge=0)


class OCTStateView(BaseModel):
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
    slice_id: int = Field(..., ge=0)
    mosaic_id: int = Field(..., ge=0)
    batch_id: int = Field(..., ge=0)
    complexed: bool = False
    volume_processed: bool = False
    enface_processed: bool = False
    uploaded: bool = False
    archived: bool = False


class OCTBatchState(
    OCTStateMutationsMixin, OCTBatchStateView, ToViewMixin[OCTBatchStateView]
):
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[OCTBatchStateView]] = OCTBatchStateView

    def reset_archived(self) -> None:
        self.archived = False
        self.uploaded = False
        self.touch()

    def set_uploaded(self, value: bool = True) -> None:
        self.uploaded = value
        self.touch()

    def set_archived(self, value: bool = True) -> None:
        self.archived = value
        self.touch()

    def set_complexed(self, value: bool = True) -> None:
        self.complexed = value
        self.touch()

    def set_volume_processed(self, value: bool = True) -> None:
        self.volume_processed = value
        self.touch()

    def set_enface_processed(self, value: bool = True) -> None:
        self.enface_processed = value
        self.touch()

    def reset_complexed(self) -> None:
        self.complexed = False
        self.volume_processed = False
        self.enface_processed = False
        self.touch()

    def reset_volume_processed(self) -> None:
        self.volume_processed = False
        self.touch()

    def reset_enface_processed(self) -> None:
        self.enface_processed = False
        self.touch()

    def reset_uploaded(self) -> None:
        self.uploaded = False
        self.touch()


class OCTMosaicStateView(OCTStateView):
    slice_id: int = Field(..., ge=0)
    mosaic_id: int = Field(..., ge=0)
    enface_stitched: bool = False
    volume_stitched: bool = False
    enface_uploaded: bool = False
    volume_uploaded: bool = False
    batches: dict[int, OCTBatchStateView] = Field(default_factory=dict)

    def iter_batches(self) -> Iterator[OCTBatchStateView]:
        return iter(self.batches.values())

    def all_batches_done(self, total_batches: int) -> bool:
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
                slice_id=self.slice_id,
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

    def reset_enface_stitched(self) -> None:
        self.enface_stitched = False
        self.enface_uploaded = False
        self.touch()

    def reset_volume_stitched(self) -> None:
        self.volume_stitched = False
        self.volume_uploaded = False
        self.touch()

    def reset_enface_uploaded(self) -> None:
        self.enface_uploaded = False
        self.touch()

    def reset_volume_uploaded(self) -> None:
        self.volume_uploaded = False
        self.touch()


class OCTSliceStateView(OCTStateView):
    slice_id: int = Field(..., ge=0)
    mosaics: dict[int, OCTMosaicStateView] = Field(default_factory=dict)
    registered: bool = False
    uploaded: bool = False

    def iter_mosaics(self) -> Iterator[OCTMosaicStateView]:
        return iter(self.mosaics.values())

    def all_mosaics_done(self, total_mosaics: int | None = None) -> bool:
        target = total_mosaics or 2
        if len(self.mosaics) < target:
            return False
        return all(mosaic.finished for mosaic in self.mosaics.values())

    def all_mosaics_enface_stitched(self, total_mosaics: int | None = None) -> bool:
        target = total_mosaics or 2
        if len(self.mosaics) < target:
            return False
        return all(mosaic.enface_stitched for mosaic in self.mosaics.values())


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
                slice_id=self.slice_id,
                mosaic_id=mosaic_id,
            )
        return self.mosaics[mosaic_id]

    def set_registered(self, value: bool = True) -> None:
        self.registered = value
        self.touch()

    def set_uploaded(self, value: bool = True) -> None:
        self.uploaded = value
        self.touch()

    def reset_registered(self) -> None:
        self.registered = False
        self.uploaded = False
        self.touch()

    def reset_uploaded(self) -> None:
        self.uploaded = False
        self.touch()


class OCTProjectStateView(OCTStateView):
    slices: dict[int, OCTSliceStateView] = Field(default_factory=dict)

    def get_batch(
        self,
        slice_id: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchStateView | None:
        slice_state = self.slices.get(slice_id)
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
    model_config = ConfigDict(frozen=False)
    VIEW_MODEL: ClassVar[type[OCTProjectStateView]] = OCTProjectStateView
    slices: dict[int, OCTSliceState] = Field(default_factory=dict)

    def get_slice(self, slice_id: int) -> OCTSliceState | None:
        return self.slices.get(slice_id)

    def get_mosaic(self, slice_id: int, mosaic_id: int) -> OCTMosaicState | None:
        slice_state = self.get_slice(slice_id)
        if slice_state is None:
            return None
        return slice_state.get_mosaic(mosaic_id)

    def get_batch(
        self,
        slice_id: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchState | None:
        mosaic_state = self.get_mosaic(slice_id, mosaic_id)
        if mosaic_state is None:
            return None
        return mosaic_state.get_batch(batch_id)

    def get_or_create_slice(self, slice_id: int) -> OCTSliceState:
        if slice_id not in self.slices:
            self.slices[slice_id] = OCTSliceState(
                slice_id=slice_id,
            )
        return self.slices[slice_id]

    def get_or_create_mosaic(self, slice_id: int, mosaic_id: int) -> OCTMosaicState:
        slice_state = self.get_or_create_slice(slice_id)
        return slice_state.get_or_create_mosaic(mosaic_id)

    def get_or_create_batch(
        self,
        slice_id: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchState:
        mosaic_state = self.get_or_create_mosaic(slice_id, mosaic_id)
        return mosaic_state.get_or_create_batch(batch_id)

    def delete_slice(self, slice_id: int) -> bool:
        if slice_id not in self.slices:
            return False
        del self.slices[slice_id]
        self.touch()
        return True

    def delete_mosaic(self, slice_id: int, mosaic_id: int) -> bool:
        slice_state = self.slices.get(slice_id)
        if slice_state is None or mosaic_id not in slice_state.mosaics:
            return False
        del slice_state.mosaics[mosaic_id]
        self.touch()
        return True

    def delete_batch(self, slice_id: int, mosaic_id: int, batch_id: int) -> bool:
        slice_state = self.slices.get(slice_id)
        if slice_state is None:
            return False
        mosaic_state = slice_state.mosaics.get(mosaic_id)
        if mosaic_state is None or batch_id not in mosaic_state.batches:
            return False
        del mosaic_state.batches[batch_id]
        self.touch()
        return True
