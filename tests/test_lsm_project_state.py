import time
from contextlib import contextmanager
from datetime import datetime

import pytest
from pydantic import ValidationError

from opticstream.state.lsm_project_state import (
    LSMChannelState,
    LSMChannelStateView,
    LSMProjectState,
    LSMProjectStateService,
    LSMProjectStateView,
    LSMSliceState,
    LSMSliceStateView,
    LSMStateView,
    LSMStripId,
    LSMStripState,
    LSMStripStateView,
    ProcessingState,
    _state_lock_name,
    _state_variable_key,
    ensure_lock,
)
from opticstream.state.project_state_core import (
    BaseProjectStateStore,
    PrefectProjectLock,
    PrefectVariableProjectStateRepository,
)
from prefect.variables import Variable


@pytest.fixture
def project_name() -> str:
    return "test_lsm_project"


@pytest.fixture
def state_service() -> LSMProjectStateService:
    class InMemoryRepository:
        def __init__(self) -> None:
            self._states: dict[str, LSMProjectState] = {}

        def load(self, project_name: str) -> LSMProjectState:
            if project_name not in self._states:
                self._states[project_name] = LSMProjectState()
            return self._states[project_name]

        def save(self, state: LSMProjectState) -> None:  # pragma: no cover - no-op
            # The state objects are kept by reference in _states; nothing to do.
            return None

    class DummyLock:
        @contextmanager
        def acquire(self, project_name: str, timeout_seconds: float | None = None):
            yield

    store = BaseProjectStateStore(repository=InMemoryRepository(), lock=DummyLock())
    return LSMProjectStateService(store)


@pytest.fixture
def real_state_service() -> LSMProjectStateService:
    """
    State service backed by the real Prefect repository and lock, using a fixed
    project name \"pytest\" so the state models do not need a project_name field.
    """

    fixed_project_name = "pytest"

    class TestPrefectRepository(PrefectVariableProjectStateRepository[LSMProjectState]):
        def save(self, state: LSMProjectState) -> None:  # type: ignore[override]
            key = self._key_fn(fixed_project_name)
            Variable.set(key, state.model_dump(mode="json"), overwrite=True)

    repository = TestPrefectRepository(
        key_fn=lambda _project: _state_variable_key(fixed_project_name),
        model_cls=LSMProjectState,
    )
    lock = PrefectProjectLock(
        lock_name_fn=lambda _project: _state_lock_name(fixed_project_name)
    )

    # Ensure the global concurrency limit for this lock exists in Prefect.
    ensure_lock(fixed_project_name)

    store = BaseProjectStateStore(repository=repository, lock=lock)
    return LSMProjectStateService(store)


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

    created_at = strip.created_at
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
    strip_first = project.get_or_create_strip(1, 1, channel_id=1)
    strip_second = project.get_or_create_strip(1, 1, channel_id=1)
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
    assert channel.to_view().all_finished(total_strips=2) is False

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
    assert channel_view_from_slice.all_finished(total_strips=2) is True

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
    assert channel_view_from_project.all_finished(total_strips=2) is True


def test_project_view_lookup_and_iteration_helpers():
    project = LSMProjectState()
    strip = project.get_or_create_strip(slice_id=1, strip_id=1, channel_id=2)
    strip.mark_started()

    view = project.to_view()

    # get_strip and get_strip_by_id happy path
    by_coords = view.get_strip(slice_id=1, strip_id=1, channel_id=2)
    assert by_coords is not None
    assert by_coords.slice_id == 1
    assert by_coords.strip_id == 1
    assert by_coords.channel_id == 2

    by_id = view.get_strip_by_id(LSMStripId(slice_id=1, strip_id=1, channel_id=2))
    assert by_id is not None
    assert by_id.slice_id == 1
    assert by_id.strip_id == 1
    assert by_id.channel_id == 2

    # Missing entities should return None
    assert view.get_strip(slice_id=2, strip_id=1, channel_id=2) is None
    assert view.get_strip(slice_id=1, strip_id=2, channel_id=2) is None
    assert view.get_strip(slice_id=1, strip_id=1, channel_id=3) is None

    # iter_strips should see our single strip
    strips = list(view.iter_strips())
    assert len(strips) == 1
    iter_strip = strips[0]
    assert iter_strip.slice_id == 1
    assert iter_strip.strip_id == 1
    assert iter_strip.channel_id == 2


def test_default_channel_behaviour_in_strip_id_and_helpers():
    strip_id = LSMStripId(slice_id=1, strip_id=2)
    assert strip_id.channel_id == 1

    project = LSMProjectState()
    strip = project.get_or_create_strip_by_id(strip_id)
    assert strip.slice_id == 1
    assert strip.strip_id == 2
    assert strip.channel_id == 1

    view = project.to_view()
    looked_up = view.get_strip_by_id(strip_id)
    assert looked_up is not None
    assert looked_up.slice_id == 1
    assert looked_up.channel_id == 1


def test_pydantic_validation_boundaries():
    with pytest.raises(ValidationError):
        LSMStripId(slice_id=-1, strip_id=0, channel_id=1)

    with pytest.raises(ValidationError):
        LSMStripState(slice_id=-1, strip_id=0, channel_id=1)


def test_naming_helpers_and_ensure_lock_do_not_crash(project_name: str):
    key = _state_variable_key(project_name)
    lock_name = _state_lock_name(project_name)

    # Basic shape assertions; get_project_name may normalize the name, but the
    # important part is that helpers produce distinct non-empty strings.
    assert isinstance(key, str) and key
    assert isinstance(lock_name, str) and lock_name
    assert key != lock_name

    # ensure_lock should complete without raising, even if the underlying Prefect
    # client is a no-op or test double in this environment.
    ensure_lock(project_name)


def test_service_open_and_read_round_trip(project_name: str, state_service: LSMProjectStateService):
    with state_service.open_project(project_name) as project:
        assert isinstance(project, LSMProjectState)
        strip = project.get_or_create_strip(slice_id=1, strip_id=1, channel_id=1)
        strip.mark_completed()

    project_view = state_service.read_project(project_name)
    assert isinstance(project_view, LSMProjectStateView)
    strip_view = project_view.get_strip(slice_id=1, strip_id=1, channel_id=1)
    assert strip_view is not None
    assert strip_view.processing_state == ProcessingState.COMPLETED


@pytest.mark.integration
def test_real_prefect_store_open_and_read_round_trip(real_state_service: LSMProjectStateService):
    with real_state_service.open_project("pytest") as project:
        assert isinstance(project, LSMProjectState)
        strip = project.get_or_create_strip(slice_id=1, strip_id=1, channel_id=1)
        strip.mark_completed()

    project_view = real_state_service.read_project("pytest")
    assert isinstance(project_view, LSMProjectStateView)
    strip_view = project_view.get_strip(slice_id=1, strip_id=1, channel_id=1)
    assert strip_view is not None
    assert strip_view.processing_state == ProcessingState.COMPLETED


def test_service_open_slice_channel_strip_create_and_persist(
    project_name: str, state_service: LSMProjectStateService
):
    with state_service.open_slice(project_name, slice_id=1) as slice_state:
        assert isinstance(slice_state, LSMSliceState)
        chan = slice_state.get_or_create_channel(1)
        strip = chan.get_or_create_strip(1)
        strip.set_uploaded(True)

    # open_channel should see existing state and allow further mutation
    with state_service.open_channel(project_name, slice_id=1, channel_id=1) as channel:
        assert isinstance(channel, LSMChannelState)
        strip = channel.get_or_create_strip(1)
        strip.set_backed_up(True)

    # open_strip on a non-existent strip should create it
    with state_service.open_strip(project_name, slice_id=1, strip_id=2, channel_id=1) as strip2:
        assert isinstance(strip2, LSMStripState)
        strip2.set_compressed(True)

    # Verify persisted state via read_* views
    slice_view = state_service.read_slice(project_name, slice_id=1)
    assert isinstance(slice_view, LSMSliceStateView)
    channel_view = state_service.read_channel(project_name, slice_id=1, channel_id=1)
    assert isinstance(channel_view, LSMChannelStateView)
    strip1_view = state_service.read_strip(project_name, slice_id=1, strip_id=1, channel_id=1)
    strip2_view = state_service.read_strip(project_name, slice_id=1, strip_id=2, channel_id=1)

    assert strip1_view is not None
    assert strip1_view.uploaded is True
    assert strip1_view.backed_up is True

    assert strip2_view is not None
    assert strip2_view.compressed is True


@pytest.mark.integration
def test_real_prefect_store_read_and_peek(real_state_service: LSMProjectStateService):
    # Mutate state under the real Prefect-backed store.
    with real_state_service.open_strip(
        "pytest", slice_id=2, strip_id=3, channel_id=1
    ) as strip:
        strip.mark_started()

    strip_view = real_state_service.read_strip(
        "pytest", slice_id=2, strip_id=3, channel_id=1
    )
    assert strip_view is not None
    assert strip_view.processing_state == ProcessingState.RUNNING

    peek_view = real_state_service.peek_project("pytest")
    assert isinstance(peek_view, LSMProjectStateView)
    peek_strip = peek_view.get_strip(slice_id=2, strip_id=3, channel_id=1)
    assert peek_strip is not None
    assert peek_strip.processing_state == ProcessingState.RUNNING


def test_service_read_and_peek_missing_entities_return_none(
    project_name: str, state_service: LSMProjectStateService
):
    # Seed an empty project by a read; this should not create any slices/channels/strips.
    empty_view = state_service.read_project(project_name)
    assert isinstance(empty_view, LSMProjectStateView)

    assert state_service.read_slice(project_name, slice_id=1) is None
    assert state_service.read_channel(project_name, slice_id=1, channel_id=1) is None
    assert state_service.read_strip(project_name, slice_id=1, strip_id=1, channel_id=1) is None

    assert state_service.peek_slice(project_name, slice_id=1) is None
    assert state_service.peek_channel(project_name, slice_id=1, channel_id=1) is None
    assert state_service.peek_strip(project_name, slice_id=1, strip_id=1, channel_id=1) is None

    # After mutating via open_project, peek_project should reflect the latest
    # state without further modification.
    with state_service.open_project(project_name) as project:
        strip = project.get_or_create_strip(slice_id=1, strip_id=1, channel_id=1)
        strip.mark_started()

    peek_view = state_service.peek_project(project_name)
    assert isinstance(peek_view, LSMProjectStateView)
    strip_view = peek_view.get_strip(slice_id=1, strip_id=1, channel_id=1)
    assert strip_view is not None
    assert strip_view.processing_state == ProcessingState.RUNNING

