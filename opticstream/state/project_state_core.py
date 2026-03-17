"""
Generic project-level state infrastructure shared across pipelines.

Responsibilities:
- Define a generic ProjectStateRepository protocol for loading/saving Pydantic models.
- Provide a concurrency lock abstraction (ProjectLock) and a Prefect-based implementation.
- Provide ProjectStateStore[TState] to coordinate lock -> load -> mutate -> save.
- Expose an ensure_limit helper for Prefect global concurrency limits.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Generic, Iterator, Protocol, TypeVar

from pydantic import BaseModel
from prefect.client.orchestration import get_client
from prefect.concurrency.sync import concurrency
from prefect.variables import Variable


TState = TypeVar("TState", bound=BaseModel)
TResult = TypeVar("TResult")
TItem = TypeVar("TItem")


class StateRepository(Protocol, Generic[TState]):
    """Persistence abstraction for project state."""

    def load(self, project_name: str) -> TState:
        ...

    def save(self, state: TState) -> None:
        ...


class PrefectVariableProjectStateRepository(Generic[TState]):
    """
    Prefect Variable-backed repository for project state.

    Parameterized by:
    - key_fn(project_name) -> variable name
    - model_cls: Pydantic model type for the state
    """

    def __init__(
        self,
        key_fn: Callable[[str], str],
        model_cls: type[TState],
    ) -> None:
        self._key_fn = key_fn
        self._model_cls = model_cls

    def load(self, project_name: str) -> TState:
        key = self._key_fn(project_name)
        raw = Variable.get(key, default=None)
        if raw is None:
            # type: ignore[call-arg] - callers must ensure model_cls accepts project_name
            return self._model_cls(project_name=project_name)  # type: ignore[return-value]
        return self._model_cls.model_validate(raw)

    def save(self, state: TState) -> None:
        # type: ignore[attr-defined] - state is expected to have project_name attribute
        key = self._key_fn(state.project_name)  # type: ignore[arg-type]
        Variable.set(key, state.model_dump(mode='json'), overwrite=True)


class ProjectLock(Protocol):
    """Lock abstraction for exclusive project-state access."""

    @contextmanager
    def acquire(
        self,
        project_name: str,
        timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        ...


class PrefectProjectLock:
    """
    Global concurrency-limit-based lock for project state.

    Parameterized by:
    - lock_name_fn(project_name) -> global concurrency limit name

    Requires a Prefect global concurrency limit with limit=1 for each lock name.
    """

    def __init__(self, lock_name_fn: Callable[[str], str]) -> None:
        self._lock_name_fn = lock_name_fn

    @contextmanager
    def acquire(
        self,
        project_name: str,
        timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        with concurrency(
            self._lock_name_fn(project_name),
            occupy=1,
            timeout_seconds=timeout_seconds,
            strict=True,
        ):
            yield


class BaseProjectStateStore(Generic[TState]):
    """
    Coordinating service for lock -> load -> mutate -> save.

    Pipelines should usually construct a store via small factory helpers that
    provide repository and lock instances appropriate for that pipeline.
    """

    def __init__(
        self,
        repository: StateRepository[TState],
        lock: ProjectLock,
    ) -> None:
        self._repository = repository
        self._lock = lock

    @contextmanager
    def locked(
        self,
        project_name: str,
        timeout_seconds: float | None = None,
    ) -> Iterator[TState]:
        """
        Yield project state under exclusive access and save on normal exit.
        """
        with self._lock.acquire(project_name, timeout_seconds=timeout_seconds):
            state = self._repository.load(project_name)
            yield state
            self._repository.save(state)

    def read(
        self,
        project_name: str,
        reader: Callable[[TState], TResult],
        timeout_seconds: float | None = None,
    ) -> TResult:
        """
        Locked readonly access using a callback.
        """
        with self.locked(project_name, timeout_seconds=timeout_seconds) as state:
            return reader(state)

    def update(
        self,
        project_name: str,
        mutate: Callable[[TState], None],
        timeout_seconds: float | None = None,
    ) -> None:
        """
        Locked mutation using a callback.

        Prefer `with store.open(...)` for structured updates to sub-items.
        """
        with self.locked(project_name, timeout_seconds=timeout_seconds) as state:
            mutate(state)

    def peek(
        self,
        project_name: str,
        reader: Callable[[TState], TResult],
    ) -> TResult:
        """
        Unlocked best-effort readonly access.
        """
        state = self._repository.load(project_name)
        return reader(state)

    @contextmanager
    def open(
        self,
        project_name: str,
        getter: Callable[[TState], TItem],
        timeout_seconds: float | None = None,
    ) -> Iterator[TItem]:
        """
        Locked context manager yielding a selected mutable item.
        """
        with self.locked(project_name, timeout_seconds=timeout_seconds) as state:
            yield getter(state)



async def ensure_limit(name: str, limit: int) -> None:
    """
    Create or update a Prefect global concurrency limit.

    Pipelines should call this with their lock-name function:
        await ensure_limit(lock_name_fn(project_name), 1)
    """
    async with get_client() as client:
        await client.upsert_global_concurrency_limit_by_name(name, limit)

