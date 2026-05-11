"""Shared Redis client factory."""

from __future__ import annotations

from typing import Any


def make_redis_client(redis_url: str, *, require: bool = False) -> Any:
    """Return a Redis client for *redis_url*, or None on failure.

    If *require* is True, raises RuntimeError when the ``redis`` package is
    missing (used by callers that must have Redis to function at all).
    Connection errors from Redis.from_url are not caught here because the
    client is lazy — actual errors surface on first use.
    """
    try:
        from redis import Redis
    except ImportError as e:
        if require:
            raise RuntimeError(
                "Install the `redis` package to use Redis."
            ) from e
        return None
    return Redis.from_url(redis_url, decode_responses=True)
