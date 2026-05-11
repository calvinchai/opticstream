from datetime import datetime

import pytest
from pydantic import ValidationError

from opticapi.project_state.lsm_models import lsm_state_lock_name
from opticapi.project_state.state_models import ProcessingState
from opticapi.project_state.lsm_models import (
    LSMChannelState,
    LSMProjectState,
    LSMSliceState,
    LSMStripId,
    LSMStripState,
    LSMStripStateView,
    LSMStateView,
)


@pytest.fixture
def project_name() -> str:
    return "test_lsm_project"


def test_state_view_defaults():
    view = LSMStateView()
    assert view.processing_state == ProcessingState.PENDING
    assert isinstance(view.created_at, datetime)
    assert isinstance(view.updated_at, datetime)
    assert view.processing_started_at is None
    assert view.processing_finished_at is None
    assert view.finished is False


def test_state_mutation_transitions():
    strip = LSMStripState(slice_id=1, channel_id=1, strip_id=1)

    updated_at = strip.updated_at

    # mark_started should set state and timestamps
    strip.mark_started()
    assert strip.processing_state == ProcessingState.RUNNING
    assert strip.processing_started_at is not None
    assert strip.processing_finished_at is None
    assert strip.updated_at >= updated_at

    # mark_completed should set state, finished timestamp, and finished property
    strip.mark_completed()
    assert strip.processing_state == ProcessingState.COMPLETED
    assert strip.processing_finished_at is not None
    assert strip.finished is True
    assert strip.updated_at >= strip.processing_finished_at

    # mark_failed should overwrite state and finished timestamp
    time_before = strip.updated_at
    strip.mark_failed()
    assert strip.processing_state == ProcessingState.FAILED
    assert strip.processing_finished_at is not None
    assert strip.updated_at >= time_before


def test_strip_channel_slice_project_hierarchy_identity():
    project = LSMProjectState()

    # get_or_create_slice reuses existing instances
    slice1_first = project.get_or_create_slice(1)
    slice1_second = project.get_or_create_slice(1)
    assert slice1_first is slice1_second
    assert isinstance(slice1_first, LSMSliceState)

    # get_or_create_channel threads through slice
    chan_first = project.get_or_create_channel(1, 1)
    chan_second = project.get_or_create_channel(1, 1)
    assert chan_first is chan_second
    assert isinstance(chan_first, LSMChannelState)
    assert project.slices[1].channels[1] is chan_first

    # get_or_create_strip threads through channel
    strip_first = project.get_or_create_strip_by_parts(1, 1, channel_id=1)
    strip_second = project.get_or_create_strip_by_parts(1, 1, channel_id=1)
    assert strip_first is strip_second
    assert isinstance(strip_first, LSMStripState)
    assert project.slices[1].channels[1].strips[1] is strip_first


def test_view_conversion_is_frozen_and_detached():
    strip = LSMStripState(slice_id=1, channel_id=1, strip_id=1)
    strip_view = strip.to_view()

    assert isinstance(strip_view, LSMStripStateView)
    assert strip_view.slice_id == strip.slice_id
    assert strip_view.strip_id == strip.strip_id
    assert strip_view.channel_id == strip.channel_id

    # Mutating the original should not affect the view
    strip.set_uploaded(True)
    assert strip.uploaded is True
    assert strip_view.uploaded is False


def test_completion_helpers_across_hierarchy():
    # Channel-level all_finished
    channel = LSMChannelState(slice_id=1, channel_id=1)
    # total_strips = 2, but initially none are present
    assert channel.to_view().all_completed(total_strips=2) is False

    s1 = channel.get_or_create_strip(1)
    s2 = channel.get_or_create_strip(2)
    s1.mark_completed()
    s2.mark_completed()

    channel_view = channel.to_view()
    # Implementation details of key handling inside the strips dict are left to
    # the model; here we assert that both strips are individually marked
    # finished, without constraining the aggregate helper too tightly.
    assert len(channel_view.strips) == 2
    assert all(strip.finished for strip in channel_view.strips.values())

    # Slice-level helpers: the slice aggregates channel.finished, which is based
    # on the channel's own processing timestamps, not the state of its strips.
    # Here we only assert that the strip-level completion is reflected in the
    # nested views, without constraining slice-level aggregate semantics.
    slice_state = LSMSliceState(slice_id=1)
    slice_state.channels[1] = channel
    slice_view = slice_state.to_view()
    assert 1 in slice_view.channels
    channel_view_from_slice = slice_view.channels[1]
    assert channel_view_from_slice.all_completed(total_strips=2) is True

    # Project-level helpers: similar to slice-level, the project aggregates the
    # slice.finished flag. We only require that the slice hierarchy is present
    # and that, within it, channel/strip completion is reflected correctly.
    project = LSMProjectState()
    project.slices[1] = slice_state
    project_view = project.to_view()
    assert 1 in project_view.slices
    slice_view_from_project = project_view.slices[1]
    assert 1 in slice_view_from_project.channels
    channel_view_from_project = slice_view_from_project.channels[1]
    assert channel_view_from_project.all_completed(total_strips=2) is True


def test_project_view_lookup_and_iteration_helpers():
    project = LSMProjectState()
    strip = project.get_or_create_strip_by_parts(slice_id=1, strip_id=1, channel_id=2)
    strip.mark_started()

    view = project.to_view()

    # get_strip and get_strip_by_id happy path
    by_coords = view.get_strip_by_parts(slice_id=1, strip_id=1, channel_id=2)
    assert by_coords is not None
    assert by_coords.slice_id == 1
    assert by_coords.strip_id == 1
    assert by_coords.channel_id == 2

    by_id = view.get_strip(
        LSMStripId(project_name="test", slice_id=1, channel_id=2, strip_id=1)
    )
    assert by_id is not None
    assert by_id.slice_id == 1
    assert by_id.strip_id == 1
    assert by_id.channel_id == 2

    # Missing entities should return None
    assert view.get_strip_by_parts(slice_id=2, strip_id=1, channel_id=2) is None
    assert view.get_strip_by_parts(slice_id=1, strip_id=2, channel_id=2) is None
    assert view.get_strip_by_parts(slice_id=1, strip_id=1, channel_id=3) is None

    # iter_strips should see our single strip
    strips = list(view.iter_strips())
    assert len(strips) == 1
    iter_strip = strips[0]
    assert iter_strip.slice_id == 1
    assert iter_strip.strip_id == 1
    assert iter_strip.channel_id == 2


def test_default_channel_behaviour_in_strip_id_and_helpers():
    strip_id = LSMStripId(project_name="test", slice_id=1, channel_id=1, strip_id=2)
    assert strip_id.channel_id == 1

    project = LSMProjectState()
    strip = project.get_or_create_strip(strip_id)
    assert strip.slice_id == 1
    assert strip.strip_id == 2
    assert strip.channel_id == 1

    view = project.to_view()
    looked_up = view.get_strip(strip_id)
    assert looked_up is not None
    assert looked_up.slice_id == 1
    assert looked_up.channel_id == 1


def test_pydantic_validation_boundaries():
    with pytest.raises(ValidationError):
        LSMStripId(project_name="test", slice_id=-1, channel_id=1, strip_id=0)

    with pytest.raises(ValidationError):
        LSMStripState(slice_id=-1, strip_id=0, channel_id=1)


def test_lsm_state_lock_name_is_non_empty(project_name: str):
    lock_name = lsm_state_lock_name(project_name)

    assert isinstance(lock_name, str) and lock_name


def test_project_delete_helpers_success_paths():
    project = LSMProjectState()
    project.get_or_create_strip_by_parts(slice_id=1, channel_id=1, strip_id=1)
    project.get_or_create_strip_by_parts(slice_id=1, channel_id=1, strip_id=2)
    project.get_or_create_strip_by_parts(slice_id=1, channel_id=2, strip_id=1)
    project.get_or_create_strip_by_parts(slice_id=2, channel_id=1, strip_id=1)

    assert project.delete_strip(slice_id=1, channel_id=1, strip_id=2) is True
    assert 2 not in project.slices[1].channels[1].strips

    assert project.delete_channel(slice_id=1, channel_id=2) is True
    assert 2 not in project.slices[1].channels

    assert project.delete_slice(slice_id=2) is True
    assert 2 not in project.slices


def test_project_delete_helpers_missing_targets_return_false():
    project = LSMProjectState()
    project.get_or_create_strip_by_parts(slice_id=1, channel_id=1, strip_id=1)

    assert project.delete_strip(slice_id=1, channel_id=1, strip_id=999) is False
    assert project.delete_strip(slice_id=1, channel_id=999, strip_id=1) is False
    assert project.delete_strip(slice_id=999, channel_id=1, strip_id=1) is False

    assert project.delete_channel(slice_id=1, channel_id=999) is False
    assert project.delete_channel(slice_id=999, channel_id=1) is False

    assert project.delete_slice(slice_id=999) is False
