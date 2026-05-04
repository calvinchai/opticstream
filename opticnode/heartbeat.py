"""Self-registration, Redis statistics publishing, and log streaming."""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections import deque
from typing import Any

from . import __version__
from .config import Settings
from .telemetry import TelemetryEngine, snapshot_to_flat_dict
from .utils.network import NetworkPlanes, get_primary_ipv4

logger = logging.getLogger(__name__)

NODES_SET_KEY = "opticnode:nodes"
TOMBSTONES_SET_KEY = "opticnode:tombstones"


def _redis_client(settings: Settings) -> Any:
    try:
        from redis import Redis
    except ImportError as e:
        raise RuntimeError(
            "Install the `redis` package to use heartbeat Redis publishing."
        ) from e
    return Redis.from_url(settings.redis_url, decode_responses=True)


class ResilientRedisLogHandler(logging.Handler):
    """Stream logs to a Redis list; buffer locally when Redis is down."""

    def __init__(self, *, list_max: int) -> None:
        super().__init__()
        self._list_max = list_max
        self._client: Any = None
        self._key = ""
        self._local: deque[str] = deque(maxlen=list_max)
        self._lock = threading.Lock()

    def set_target(self, client: Any | None, key: str) -> None:
        with self._lock:
            self._client = client
            self._key = key

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            self.handleError(record)
            return
        with self._lock:
            r = self._client
            key = self._key
            if r is not None and key:
                try:
                    while self._local:
                        old = self._local.popleft()
                        r.lpush(key, old)
                        r.ltrim(key, 0, self._list_max - 1)
                    r.lpush(key, msg)
                    r.ltrim(key, 0, self._list_max - 1)
                    return
                except Exception:
                    pass
            try:
                self._local.append(msg)
            except Exception:
                self.handleError(record)


class HeartbeatLoop:
    """Background loop: register node, publish telemetry, maintain last_seen TTL."""

    def __init__(
        self,
        settings: Settings,
        stop_event: threading.Event,
        telemetry: TelemetryEngine,
        planes: NetworkPlanes,
        log_handler: ResilientRedisLogHandler | None = None,
    ) -> None:
        self._settings = settings
        self._stop = stop_event
        self._telemetry = telemetry
        self._planes = planes
        self._log_handler = log_handler

    def run(self) -> None:
        try:
            _redis_client(self._settings)
        except RuntimeError as e:
            logger.warning("%s — heartbeat disabled.", e)
            self._stop.wait()
            return

        node_id = self._settings.node_id
        interval = self._settings.heartbeat_interval_s
        ttl = self._settings.heartbeat_ttl_s
        key_prefix = f"opticnode:{node_id}"
        last_seen_key = f"{key_prefix}:last_seen"
        stats_key = f"{key_prefix}:stats"
        meta_key = f"{key_prefix}:meta"
        logs_key = f"{key_prefix}:logs"

        if self._log_handler is not None:
            fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
            self._log_handler.setFormatter(fmt)
            logging.getLogger().addHandler(self._log_handler)

        started_at = str(time.time())
        hostname = socket.gethostname()
        client: Any | None = None

        while not self._stop.is_set():
            if client is None:
                try:
                    client = _redis_client(self._settings)
                    if self._log_handler is not None:
                        self._log_handler.set_target(client, logs_key)
                    client.srem(TOMBSTONES_SET_KEY, node_id)
                    client.sadd(NODES_SET_KEY, node_id)
                    logger.info("Heartbeat connected to Redis.")
                except Exception as exc:
                    logger.warning("Redis unreachable; will retry: %s", exc)
                    client = None
                    if self._log_handler is not None:
                        self._log_handler.set_target(None, "")
                    if self._stop.wait(timeout=min(1.0, interval)):
                        break
                    continue

            try:
                mgmt_ip = self._settings.advertised_host or self._planes.mgmt_ip or get_primary_ipv4()
                data_ip = self._planes.data_ip or ""
                snap = self._telemetry.collect()
                mapping = snapshot_to_flat_dict(snap)
                mapping["uptime_s"] = str(time.monotonic())

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
            except Exception:
                logger.exception("Heartbeat Redis publish failed")
                try:
                    client.close()
                except Exception:
                    pass
                client = None
                if self._log_handler is not None:
                    self._log_handler.set_target(None, "")

            if self._stop.wait(timeout=interval):
                break

        if self._log_handler is not None:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler.set_target(None, "")
