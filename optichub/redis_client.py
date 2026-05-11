"""Redis access for node registry and hub purge."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from opticnode.heartbeat import NODES_SET_KEY
from opticnode.logging_buffer import LOG_MODULE_IDS


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
    prefix = f"opticnode:{node_id}"
    meta = r.hgetall(f"{prefix}:meta") or {}
    last_raw = r.get(f"{prefix}:last_seen")
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
    key = f"opticnode:{node_id}:logs:{module_id}"
    n = max(1, min(limit, 10_000))
    raw = r.lrange(key, 0, n - 1) or []
    return list(reversed(raw))


def purge_node(redis_url: str, node_id: str) -> None:
    """Delete hub-side node keys and remove `node_id` from `opticnode:nodes`."""
    r = _client(redis_url)
    prefix = f"opticnode:{node_id}"
    keys_to_del = [f"{prefix}:meta", f"{prefix}:last_seen", f"{prefix}:stats", f"{prefix}:logs"]
    for mid in LOG_MODULE_IDS:
        keys_to_del.append(f"{prefix}:logs:{mid}")
    pipe = r.pipeline()
    pipe.delete(*keys_to_del)
    pipe.srem(NODES_SET_KEY, node_id)
    pipe.execute()
