"""PrimoCache monitoring module — polls rxpcc and publishes stats to Redis."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any

from pydantic import Field

from opticnode.app.redis_utils import make_redis_client
from opticnode.utils.cli_parsers import PrimoCacheStats, parse_rxpcc_stats
from opticnode.modules.base import ModuleConfig, LoopModule

logger = logging.getLogger(__name__)


@dataclass
class PrimoCacheSnapshot:
    binary_present: bool
    is_active: bool
    stats: PrimoCacheStats | None
    error: str
    collected_at: float


class PrimoCacheMonitorConfig(ModuleConfig):
    poll_interval_s: float = Field(default=15.0, gt=0, description="Seconds between rxpcc polls")


class PrimoCacheMonitorModule(LoopModule):
    """Polls rxpcc on a background thread and publishes stats to Redis."""

    name = "primocache_monitor"
    Config = PrimoCacheMonitorConfig
    _thread_join_timeout = 20.0

    def __init__(self, redis_url: str, node_id: str, primocache_exe: str = "rxpcc.exe") -> None:
        super().__init__()
        self._redis_url = redis_url
        self._node_id = node_id
        self._rxpcc_path: str | None = shutil.which(primocache_exe)
        self._snapshot_lock = threading.Lock()
        self._snapshot = PrimoCacheSnapshot(
            binary_present=self._rxpcc_path is not None,
            is_active=False,
            stats=None,
            error="",
            collected_at=0.0,
        )
        if self._rxpcc_path is None:
            logger.warning(
                "PrimoCacheMonitor: %r not found on PATH — monitoring disabled.",
                primocache_exe,
            )

    def get_snapshot(self) -> PrimoCacheSnapshot:
        with self._snapshot_lock:
            return self._snapshot

    # ---------- loop ----------

    def _run_loop(self) -> None:
        cfg: PrimoCacheMonitorConfig = self._config  # type: ignore[assignment]
        redis_client: Any = make_redis_client(self._redis_url)
        if redis_client is None:
            logger.warning("PrimoCacheMonitor: Redis unavailable; stats will not be published.")
        stats_key = f"opticnode:{self._node_id}:primocache_stats"

        while not self._stop_event.is_set():
            self._poll(redis_client, stats_key)
            self._stop_event.wait(timeout=cfg.poll_interval_s)

        if redis_client is not None:
            try:
                redis_client.close()
            except Exception:
                pass

    # ---------- internal ----------

    def _poll(self, redis_client: Any, stats_key: str) -> None:
        if self._rxpcc_path is None:
            return
        now = time.time()
        is_active = False
        stats: PrimoCacheStats | None = None
        error = ""
        try:
            completed = subprocess.run(
                [self._rxpcc_path],
                capture_output=True,
                text=True,
                timeout=15.0,
                check=False,
            )
            out = (completed.stdout or "") + (completed.stderr or "")
            if "no cache found" not in out.lower():
                stats = parse_rxpcc_stats(out)
                is_active = bool(out.strip())
        except subprocess.TimeoutExpired:
            error = "rxpcc timed out"
        except Exception as exc:
            error = str(exc)
            logger.exception("PrimoCacheMonitor: rxpcc execution failed")

        snap = PrimoCacheSnapshot(
            binary_present=True,
            is_active=is_active,
            stats=stats,
            error=error,
            collected_at=now,
        )
        with self._snapshot_lock:
            self._snapshot = snap

        if redis_client is not None:
            try:
                mapping: dict[str, str] = {
                    "collected_at_unix": str(now),
                    "is_active": str(is_active).lower(),
                    "error": error[:2000],
                }
                if stats is not None:
                    d = asdict(stats)
                    raw_labels = d.pop("raw_labels", {})
                    mapping["stats_json"] = json.dumps(d, default=str)
                    if raw_labels:
                        mapping["raw_labels_json"] = json.dumps(raw_labels)[:8000]
                redis_client.hset(stats_key, mapping=mapping)
            except Exception:
                logger.warning("PrimoCacheMonitor: failed to publish stats to Redis.")
