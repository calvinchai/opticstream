"""Per-module logging: rotating file + in-memory tail for RPC + Redis tail for post-mortem."""

from __future__ import annotations

import logging
import logging.handlers
import queue
from collections import deque
from pathlib import Path
from typing import Any

from opticapi.node_contract import node_logs_key

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")


class ModuleLog:
    """Attaches file, deque, and Redis handlers to a module's named logger.

    The log directory is created on first use. Redis writes happen inline on
    every emit — only attempted when a client is set, silently dropped on error.
    Setting propagate=False prevents double-emission to the root logger.
    """

    def __init__(
        self,
        module_name: str,
        log_dir: Path,
        node_id: str,
        tail: int = 200,
        redis_tail: int = 100,
        gui_queue: "queue.Queue[logging.LogRecord] | None" = None,
    ) -> None:
        self._name = module_name
        self._deque: deque[str] = deque(maxlen=tail)
        self._redis_client: Any = None

        log_dir.mkdir(parents=True, exist_ok=True)

        handlers: list[logging.Handler] = [
            _DequeHandler(self._deque),
            logging.handlers.RotatingFileHandler(
                log_dir / f"{module_name}.log",
                maxBytes=10 * 1024 * 1024,
                backupCount=3,
                encoding="utf-8",
            ),
            _RedisHandler(self._get_redis, node_logs_key(node_id, module_name), redis_tail),
        ]
        if gui_queue is not None:
            handlers.append(logging.handlers.QueueHandler(gui_queue))

        log = logging.getLogger(f"opticnode.modules.{module_name}")
        for h in handlers:
            h.setFormatter(_fmt)
            log.addHandler(h)
        log.propagate = False
        self._handlers = handlers
        self._log = log

    def _get_redis(self) -> Any:
        return self._redis_client

    def set_redis(self, client: Any) -> None:
        """Called by heartbeat when Redis connects or disconnects."""
        self._redis_client = client

    def get_tail(self, n: int = 100) -> list[str]:
        """Return the last n lines from the in-memory buffer (0 = all)."""
        lines = list(self._deque)
        return lines[-n:] if n > 0 else lines

    def close(self) -> None:
        for h in self._handlers:
            self._log.removeHandler(h)
            h.close()


class _DequeHandler(logging.Handler):
    def __init__(self, d: deque[str]) -> None:
        super().__init__()
        self._d = d

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._d.append(self.format(record))
        except Exception:
            self.handleError(record)


class _RedisHandler(logging.Handler):
    def __init__(self, get_client: Any, key: str, tail: int) -> None:
        super().__init__()
        self._get = get_client
        self._key = key
        self._tail = tail

    def emit(self, record: logging.LogRecord) -> None:
        client = self._get()
        if client is None:
            return
        try:
            client.lpush(self._key, self.format(record))
            client.ltrim(self._key, 0, self._tail - 1)
        except Exception:
            pass


__all__ = ["ModuleLog"]

