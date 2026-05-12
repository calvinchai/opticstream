"""Redis access for node registry and hub purge."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from opticapi.node_contract import (
    LOG_MODULE_IDS,
    NODES_SET_KEY,
    node_last_seen_key,
    node_logs_key,
    node_meta_key,
    node_stats_key,
    node_stats_ts_key,
)


@dataclass(frozen=True)
class NodeRecord:
    node_id: str
    grpc_host: str
    grpc_port: int
    ipv4: str
    hostname: str
    started_at: str | None
    version: str | None
    last_seen_ts: float | None
    """Unix timestamp from Redis last_seen value, or None if key missing."""
    online: bool
    """True if last_seen exists and age <= grace window (caller sets)."""


def _client(redis_url: str) -> Any:
    from redis import Redis

    return Redis.from_url(redis_url, decode_responses=True)


def list_node_ids(redis_url: str) -> list[str]:
    """All `node_id` values in the Redis registry set (`opticnode:nodes`)."""
    r = _client(redis_url)
    members = r.smembers(NODES_SET_KEY) or set()
    return sorted(members)


def list_manage_node_ids(redis_url: str) -> list[str]:
    """Node ids listed on the hub manage/overview UI (same as `list_node_ids`)."""
    return list_node_ids(redis_url)


def get_node(redis_url: str, node_id: str, *, online_grace_s: float, now: float) -> NodeRecord:
    r = _client(redis_url)
    meta = r.hgetall(node_meta_key(node_id)) or {}
    last_raw = r.get(node_last_seen_key(node_id))
    last_ts: float | None = None
    if last_raw is not None:
        try:
            last_ts = float(last_raw)
        except ValueError:
            last_ts = None
    online = last_ts is not None and (now - last_ts) <= online_grace_s
    port_s = meta.get("grpc_port") or "50051"
    try:
        grpc_port = int(port_s)
    except ValueError:
        grpc_port = 50051
    return NodeRecord(
        node_id=node_id,
        grpc_host=meta.get("grpc_host") or "",
        grpc_port=grpc_port,
        ipv4=meta.get("ipv4") or "",
        hostname=meta.get("hostname") or "",
        started_at=meta.get("started_at"),
        version=meta.get("version"),
        last_seen_ts=last_ts,
        online=online,
    )


def get_node_module_logs_redis(redis_url: str, node_id: str, module_id: str, limit: int) -> list[str]:
    """Last `limit` lines for a module from Redis (LPUSH order: newest first)."""
    if module_id not in LOG_MODULE_IDS:
        return []
    r = _client(redis_url)
    key = node_logs_key(node_id, module_id)
    n = max(1, min(limit, 10_000))
    raw = r.lrange(key, 0, n - 1) or []
    return list(reversed(raw))


def get_node_stats(redis_url: str, node_id: str) -> dict[str, str]:
    r = _client(redis_url)
    return r.hgetall(node_stats_key(node_id)) or {}


def get_node_stats_history(redis_url: str, node_id: str, limit: int = 360) -> list[dict[str, str]]:
    """Return recent telemetry snapshots (newest first) from the time-series list."""
    import json

    r = _client(redis_url)
    raw = r.lrange(node_stats_ts_key(node_id), 0, limit - 1) or []
    out: list[dict[str, str]] = []
    for entry in raw:
        try:
            out.append(json.loads(entry))
        except (json.JSONDecodeError, TypeError):
            continue
    return out


def purge_node(redis_url: str, node_id: str) -> None:
    """Delete hub-side node keys and remove `node_id` from `opticnode:nodes`."""
    r = _client(redis_url)
    keys_to_del = [
        node_meta_key(node_id),
        node_last_seen_key(node_id),
        node_stats_key(node_id),
        node_stats_ts_key(node_id),
    ]
    for mid in LOG_MODULE_IDS:
        keys_to_del.append(node_logs_key(node_id, mid))
    pipe = r.pipeline()
    pipe.delete(*keys_to_del)
    pipe.srem(NODES_SET_KEY, node_id)
    pipe.execute()
