"""Self-registration, Redis statistics publishing, and log streaming."""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any

from opticapi.node_contract import (
    NODES_SET_KEY,
    node_last_seen_key,
    node_meta_key,
    node_stats_key,
)
from opticnode.app.config import Settings
from opticnode.app.telemetry import TelemetryEngine, snapshot_to_flat_dict
from opticnode.app.redis_utils import make_redis_client
from opticnode.utils.network import NetworkPlanes, get_primary_ipv4
from opticnode import __version__

logger = logging.getLogger(__name__)

# When Redis is down or drops connections, avoid flooding logs every tick.
_REDIS_WARN_MIN_INTERVAL_S = 30.0
_REDIS_RECONNECT_BASE_S = 1.0
_REDIS_RECONNECT_MAX_S = 60.0


def _throttled_warning(last_emit: list[float], msg: str, *args: object) -> None:
    """Log *msg* as warning at most once per _REDIS_WARN_MIN_INTERVAL_S; else debug."""
    now = time.monotonic()
    if now - last_emit[0] >= _REDIS_WARN_MIN_INTERVAL_S:
        last_emit[0] = now
        logger.warning(msg, *args)
    else:
        logger.debug(msg, *args)


class HeartbeatLoop:
    """Background loop: register node, publish telemetry, maintain last_seen TTL.

    Redis is used as a registry (node presence) and stats sink. Per-module log tails
    are written directly by each module's _RedisHandler when a client is connected.
    Module state is served live via gRPC (ListModules), not stored in Redis.
    """

    def __init__(
        self,
        settings: Settings,
        stop_event: threading.Event,
        telemetry: TelemetryEngine,
        planes: NetworkPlanes,
        module_registry: Any = None,
    ) -> None:
        self._settings = settings
        self._stop = stop_event
        self._telemetry = telemetry
        self._planes = planes
        self._module_registry = module_registry

    def run(self) -> None:
        try:
            make_redis_client(self._settings.redis_url, require=True)
        except RuntimeError as e:
            logger.warning("%s — heartbeat disabled.", e)
            self._stop.wait()
            return

        node_id = self._settings.node_id
        interval = self._settings.heartbeat_interval_s
        ttl = self._settings.heartbeat_ttl_s
        last_seen_key = node_last_seen_key(node_id)
        stats_key = node_stats_key(node_id)
        meta_key = node_meta_key(node_id)

        started_at = str(time.time())
        hostname = socket.gethostname()
        client: Any | None = None
        last_redis_warn_at: list[float] = [0.0]
        reconnect_failures = 0

        while not self._stop.is_set():
            if client is None:
                try:
                    client = make_redis_client(self._settings.redis_url, require=True)
                    if self._module_registry is not None:
                        self._module_registry.set_redis_all(client)
                    client.sadd(NODES_SET_KEY, node_id)
                    reconnect_failures = 0
                    logger.info("Heartbeat connected to Redis.")
                except Exception as exc:
                    _throttled_warning(
                        last_redis_warn_at,
                        "Redis unreachable; will retry: %s",
                        exc,
                    )
                    client = None
                    if self._module_registry is not None:
                        self._module_registry.set_redis_all(None)
                    reconnect_failures += 1
                    delay = min(
                        _REDIS_RECONNECT_MAX_S,
                        _REDIS_RECONNECT_BASE_S * (2 ** min(reconnect_failures, 6)),
                    )
                    if self._stop.wait(timeout=delay):
                        break
                    continue

            try:
                mgmt_ip = self._settings.advertised_host or self._planes.mgmt_ip or get_primary_ipv4()
                data_ip = self._planes.data_ip or ""
                snap = self._telemetry.collect()
                mapping = snapshot_to_flat_dict(snap)
                mapping["uptime_s"] = str(time.monotonic())

                client.sadd(NODES_SET_KEY, node_id)
                client.setex(last_seen_key, ttl, str(time.time()))
                client.hset(stats_key, mapping=mapping)
                client.hset(
                    meta_key,
                    mapping={
                        "grpc_host": self._settings.grpc_host,
                        "grpc_port": str(self._settings.grpc_port),
                        "started_at": started_at,
                        "hostname": hostname,
                        "ipv4": mgmt_ip,
                        "mgmt_ip": mgmt_ip,
                        "data_ip": data_ip,
                        "mgmt_ifaces": ",".join(self._planes.mgmt),
                        "data_ifaces": ",".join(self._planes.data),
                        "version": __version__,
                    },
                )
            except Exception as exc:
                _throttled_warning(
                    last_redis_warn_at,
                    "Heartbeat Redis publish failed: %s",
                    exc,
                )
                try:
                    client.close()
                except Exception:
                    pass
                client = None
                if self._module_registry is not None:
                    self._module_registry.set_redis_all(None)

            if self._stop.wait(timeout=interval):
                break

        if self._module_registry is not None:
            self._module_registry.set_redis_all(None)


__all__ = ["HeartbeatLoop", "NODES_SET_KEY"]

