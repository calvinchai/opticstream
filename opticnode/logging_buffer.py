"""Per-module log ring buffer + Redis tail mirror for node logs."""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any

from .config import Settings

LOG_MODULE_IDS = frozenset({"core", "copy_queue", "prefect_worker", "watcher"})


def logger_name_to_module_id(logger_name: str) -> str:
    """Map a Python logging logger name to a hub/module log bucket."""
    if logger_name.startswith("opticnode.modules.copy_queue") or logger_name.startswith(
        "opticnode.work_queue"
    ):
        return "copy_queue"
    if logger_name.startswith("opticnode.modules.prefect_worker"):
        return "prefect_worker"
    if logger_name.startswith("opticnode.modules.watcher"):
        return "watcher"
    if logger_name.startswith("opticnode."):
        return "core"
    return "core"


class NodeLogBuffer:
    """Thread-safe in-memory deques per module plus optional Redis LPUSH mirror."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._full_max = settings.log_full_max_lines
        self._redis_tail = settings.redis_log_tail_per_module
        self._deques: dict[str, deque[str]] = {m: deque(maxlen=self._full_max) for m in LOG_MODULE_IDS}
        self._redis: Any = None
        self._redis_prefix: str = f"opticnode:{settings.node_id}:logs:"
        self._pending: deque[tuple[str, str]] = deque(maxlen=10_000)

    def set_redis_client(self, client: Any | None) -> None:
        """Called from heartbeat when Redis connects or disconnects."""
        with self._lock:
            self._redis = client
            if client is not None:
                self._flush_pending_locked(client)

    def _flush_pending_locked(self, client: Any) -> None:
        while self._pending:
            mid, line = self._pending.popleft()
            self._redis_push_line_locked(client, mid, line)

    def _redis_push_line_locked(self, client: Any, module_id: str, line: str) -> None:
        key = f"{self._redis_prefix}{module_id}"
        try:
            client.lpush(key, line)
            client.ltrim(key, 0, self._redis_tail - 1)
        except Exception:
            self._pending.append((module_id, line))

    def append(self, module_id: str, line: str) -> None:
        if module_id not in LOG_MODULE_IDS:
            module_id = "core"
        with self._lock:
            self._deques[module_id].append(line)
            r = self._redis
            if r is not None:
                self._redis_push_line_locked(r, module_id, line)
            else:
                self._pending.append((module_id, line))

    def get_tail(self, module_id: str, n: int) -> list[str]:
        if module_id not in LOG_MODULE_IDS:
            return []
        with self._lock:
            d = self._deques.get(module_id)
            if not d:
                return []
            lines = list(d)
        return lines[-n:] if n > 0 else lines

    def get_all(self, module_id: str) -> list[str]:
        if module_id not in LOG_MODULE_IDS:
            return []
        with self._lock:
            d = self._deques.get(module_id)
            if not d:
                return []
            return list(d)


class NodeLogHandler(logging.Handler):
    """Formats records and appends to NodeLogBuffer (memory + Redis tail)."""

    def __init__(self, buffer: NodeLogBuffer) -> None:
        super().__init__()
        self._buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            self.handleError(record)
            return
        module_id = logger_name_to_module_id(record.name)
        self._buffer.append(module_id, msg)
