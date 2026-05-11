"""
State service singletons backed by Prefect-managed Redis connection.

Provides ``LSM_STATE_SERVICE`` and ``OCT_STATE_SERVICE`` — the same service
classes from ``opticapi.project_state``, wired to the Redis instance stored
in the Prefect Secret block.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from opticapi.project_state.redis_backend import RedisStateBackend
from opticapi.project_state.lsm_state_service import LSMProjectStateService
from opticapi.project_state.oct_state_service import OCTProjectStateService

if TYPE_CHECKING:
    from redis import Redis


class _LazyRedisBackend(RedisStateBackend):
    """RedisStateBackend that defers the Prefect Secret lookup until first use."""

    def __init__(self, block_name: str) -> None:
        self._block_name = block_name
        self.__client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self.__client is None:
            from opticstream.state.project_state_redis import _get_redis_client

            self.__client = _get_redis_client(self._block_name)
        return self.__client


def _make_backend() -> _LazyRedisBackend:
    from opticstream.config.constants import STATE_REDIS_BLOCK_NAME

    return _LazyRedisBackend(STATE_REDIS_BLOCK_NAME)


LSM_STATE_SERVICE = LSMProjectStateService(_make_backend())
OCT_STATE_SERVICE = OCTProjectStateService(_make_backend())
