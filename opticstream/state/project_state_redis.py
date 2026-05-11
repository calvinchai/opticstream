"""
Redis-backed project-state repository, lock, and sharded state backend.

The sharded backend splits project state into granular Redis keys so that
workers operating on different strips/batches never contend on the same
key or lock.

Key layout (example for project type ``lsm``)::

    lsm:project:{name}:meta                                   → JSON string
    lsm:project:{name}:slice:{slice_id}:meta                  → JSON string
    lsm:project:{name}:channel:{slice_id}:{channel_id}:meta   → JSON string
    lsm:project:{name}:strips                                 → Hash
        field "{slice_id}:{channel_id}:{strip_id}"             → JSON string
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Generic, Iterator, TypeVar

from pydantic import BaseModel

TState = TypeVar("TState", bound=BaseModel)

if TYPE_CHECKING:
    from redis import Redis


def _get_redis_client(block_name: str) -> Redis:
    from prefect.blocks.system import Secret
    from redis import Redis

    secret = Secret.load(block_name)
    return Redis.from_url(secret.get(), decode_responses=True)


# ---------------------------------------------------------------------------
# Legacy monolithic helpers (kept for backward compatibility / tests)
# ---------------------------------------------------------------------------


class RedisProjectStateRepository(Generic[TState]):
    """Redis-backed repository for project state models (monolithic)."""

    def __init__(
        self,
        block_name: str,
        model_cls: type[TState],
        project_type: str,
    ) -> None:
        self._block_name = block_name
        self._model_cls = model_cls
        self._project_type = project_type
        self._client: Redis | None = None

    def _get_client(self) -> Redis:
        if self._client is None:
            self._client = _get_redis_client(self._block_name)
        return self._client

    def _key(self, project_name: str) -> str:
        return f"project_state:{self._project_type}:{project_name}"

    def load(self, project_name: str) -> TState:
        raw = self._get_client().get(self._key(project_name))
        if raw is None:
            return self._model_cls()  # type: ignore[return-value]
        return self._model_cls.model_validate(json.loads(raw))

    def save(self, project_name: str, state: TState) -> None:
        payload = json.dumps(state.model_dump(mode="json"))
        self._get_client().set(self._key(project_name), payload)


class RedisProjectLock:
    """Redis-backed distributed lock (monolithic, project-level)."""

    def __init__(
        self,
        block_name: str,
        lock_name_fn: Callable[[str], str],
    ) -> None:
        self._block_name = block_name
        self._lock_name_fn = lock_name_fn
        self._client: Redis | None = None

    def _get_client(self) -> Redis:
        if self._client is None:
            self._client = _get_redis_client(self._block_name)
        return self._client

    @contextmanager
    def acquire(
        self,
        project_name: str,
        timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        lock = self._get_client().lock(
            self._lock_name_fn(project_name),
            timeout=timeout_seconds,
            blocking_timeout=timeout_seconds,
        )
        if not lock.acquire():
            raise TimeoutError(
                f"Could not acquire lock for {project_name!r}"
            )
        try:
            yield
        finally:
            lock.release()


# ---------------------------------------------------------------------------
# Sharded state backend
# ---------------------------------------------------------------------------


class RedisStateBackend:
    """Low-level Redis operations for sharded project state.

    Provides two mutating context-managers (``open_hash_field`` and
    ``open_key``) that handle the lock → load → yield → save → unlock
    lifecycle, plus read helpers used by the peek/read service methods.
    """

    def __init__(self, block_name: str) -> None:
        self._block_name = block_name
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self._client is None:
            self._client = _get_redis_client(self._block_name)
        return self._client

    # -- mutating context-managers ------------------------------------------

    @contextmanager
    def open_hash_field(
        self,
        hash_key: str,
        field: str,
        model_cls: type[BaseModel],
        lock_key: str,
        *,
        default_factory: Callable[[], Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> Iterator[Any]:
        lock = self.client.lock(
            lock_key,
            timeout=timeout_seconds,
            blocking_timeout=timeout_seconds,
        )
        if not lock.acquire():
            raise TimeoutError(f"Could not acquire lock {lock_key!r}")
        try:
            raw = self.client.hget(hash_key, field)
            if raw is None:
                state = default_factory() if default_factory else model_cls()
            else:
                state = model_cls.model_validate(json.loads(raw))
            yield state
            self.client.hset(
                hash_key,
                field,
                json.dumps(state.model_dump(mode="json")),
            )
        finally:
            lock.release()

    @contextmanager
    def open_key(
        self,
        key: str,
        model_cls: type[BaseModel],
        lock_key: str,
        *,
        default_factory: Callable[[], Any] | None = None,
        exclude_on_save: set[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> Iterator[Any]:
        lock = self.client.lock(
            lock_key,
            timeout=timeout_seconds,
            blocking_timeout=timeout_seconds,
        )
        if not lock.acquire():
            raise TimeoutError(f"Could not acquire lock {lock_key!r}")
        try:
            raw = self.client.get(key)
            if raw is None:
                state = default_factory() if default_factory else model_cls()
            else:
                state = model_cls.model_validate(json.loads(raw))
            yield state
            self.client.set(
                key,
                json.dumps(state.model_dump(mode="json", exclude=exclude_on_save)),
            )
        finally:
            lock.release()

    # -- read helpers -------------------------------------------------------

    def load_key(self, key: str, model_cls: type[BaseModel]) -> Any | None:
        raw = self.client.get(key)
        if raw is None:
            return None
        return model_cls.model_validate(json.loads(raw))

    def load_hash_field(
        self, hash_key: str, field: str, model_cls: type[BaseModel],
    ) -> Any | None:
        raw = self.client.hget(hash_key, field)
        if raw is None:
            return None
        return model_cls.model_validate(json.loads(raw))

    def scan_hash_fields(
        self,
        hash_key: str,
        model_cls: type[BaseModel],
        match: str | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if match:
            cursor: int = 0
            while True:
                cursor, data = self.client.hscan(hash_key, cursor, match=match)
                for fld, raw in data.items():
                    result[fld] = model_cls.model_validate(json.loads(raw))
                if cursor == 0:
                    break
        else:
            for fld, raw in self.client.hgetall(hash_key).items():
                result[fld] = model_cls.model_validate(json.loads(raw))
        return result
