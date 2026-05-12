"""Shared project state helpers for hub screens."""

from __future__ import annotations

from opticapi.project_state.redis_backend import RedisStateBackend
from redis import Redis


def project_status_icon(status: str) -> str:
    if status == "completed":
        return ":material/check_circle:"
    if status == "running":
        return ":material/sync:"
    if status == "failed":
        return ":material/error:"
    return ":material/hourglass_empty:"


def make_state_backend(redis_url: str) -> RedisStateBackend:
    return RedisStateBackend(Redis.from_url(redis_url, decode_responses=True))
