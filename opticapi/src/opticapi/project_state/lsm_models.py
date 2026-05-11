"""LSM project state: IDs, read-only views, and mutable persistence models."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Iterator

from pydantic import BaseModel, ConfigDict, Field

from opticapi.naming import normalize_project_name
from opticapi.project_state.state_models import ProcessingState, ToViewMixin

LSM_PROJECT_TYPE = "lsm"


def lsm_state_lock_name(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}_lsm_state_lock"


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


class LSMStripStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    strip_id: int = Field(..., ge=0)
    channel_id: int = Field(..., ge=0)
    archived: bool = False
    compressed: bool = False
    uploaded: bool = False


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


class LSMSliceStateView(LSMStateView):
    slice_id: int = Field(..., ge=0)
    channels: dict[int, LSMChannelStateView] = Field(default_factory=dict)

    def all_finished(self, total_channels: int = 1) -> bool:
        return all(
            i in self.channels and self.channels[i].finished
            for i in range(1, total_channels + 1)
        )


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
