"""
Redis-backed sharded state backend.

Splits project state into granular Redis keys so that workers operating on
different strips/batches never contend on the same key or lock.

Key layout (example for project type ``lsm``)::

    lsm:project:{name}:meta                                   → JSON string
    lsm:project:{name}:slice:{slice_id}:meta                  → JSON string
    lsm:project:{name}:channel:{slice_id}:{channel_id}:meta   → JSON string
    lsm:project:{name}:strips:{slice_id}:{channel_id}         → Hash
        field "{strip_id}"                                     → JSON string
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Iterator

from pydantic import BaseModel

if TYPE_CHECKING:
    from redis import Redis


class RedisStateBackend:
    """Low-level Redis operations for sharded project state.

    Provides two mutating context-managers (``open_hash_field`` and
    ``open_key``) that handle the lock → load → yield → save → unlock
    lifecycle, plus read helpers used by the peek/read service methods.
    """

    def __init__(self, redis_client: Redis) -> None:
        self._client = redis_client

    @property
    def client(self) -> Redis:
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

    def load_all_hash_fields(
        self,
        hash_key: str,
        model_cls: type[BaseModel],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for fld, raw in self.client.hgetall(hash_key).items():
            result[fld] = model_cls.model_validate(json.loads(raw))
        return result
