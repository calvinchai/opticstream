"""Prefect-aware Redis client factory for state services."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis import Redis


def _get_redis_client(block_name: str) -> Redis:
    from prefect.blocks.system import Secret
    from redis import Redis

    secret = Secret.load(block_name)
    return Redis.from_url(secret.get(), decode_responses=True)
