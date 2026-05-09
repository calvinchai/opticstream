"""
Redis-backed project-state repository.

This repository persists a full project state model as JSON in Redis,
keyed by ``project_state:{project_type}:{project_name}``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Generic

from opticstream.state.project_state_core import TState

if TYPE_CHECKING:
    from redis import Redis


class RedisProjectStateRepository(Generic[TState]):
    """Redis-backed repository for project state models."""

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
            from prefect.blocks.system import Secret
            from redis import Redis

            secret = Secret.load(self._block_name)
            self._client = Redis.from_url(secret.get(), decode_responses=True)
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
