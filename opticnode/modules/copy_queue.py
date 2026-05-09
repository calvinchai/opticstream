"""CopyQueueModule: wraps WorkQueue as a managed node module."""

from __future__ import annotations

import logging
import time
from typing import Any

from pydantic import Field

from .base import ModuleConfig, ModuleState, ModuleStatus, NodeModule

logger = logging.getLogger(__name__)


class CopyQueueConfig(ModuleConfig):
    worker_count: int = Field(default=1, ge=1)
    paused: bool = False


class CopyQueueModule(NodeModule):
    """Manages the local file copy/archive queue as a module."""

    name = "copy_queue"
    Config = CopyQueueConfig

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._config: CopyQueueConfig = CopyQueueConfig()
        self._state = ModuleState.STOPPED
        self._started_at: float | None = None
        self._work_queue: Any = None

    def start(self, config: CopyQueueConfig) -> None:
        if self._state == ModuleState.RUNNING:
            raise RuntimeError("CopyQueueModule is already running.")

        from opticnode.work_queue import WorkQueue

        self._config = config

        if self._work_queue is None:
            self._work_queue = WorkQueue(self._settings, num_workers=config.worker_count)
        else:
            self._work_queue.set_worker_count(config.worker_count)

        if config.paused:
            self._work_queue.pause()
        else:
            self._work_queue.resume()

        self._state = ModuleState.RUNNING
        self._started_at = time.time()
        logger.info("CopyQueueModule: started with config %s.", config)

    def stop(self) -> None:
        if self._work_queue is not None:
            self._work_queue.stop()
            self._work_queue = None
        self._state = ModuleState.STOPPED
        self._started_at = None
        logger.info("CopyQueueModule: stopped.")

    def reconfigure(self, patch: dict) -> None:
        new_config = CopyQueueConfig.model_validate({**self._config.model_dump(), **patch})
        self._config = new_config

        if self._work_queue is None:
            return

        if "worker_count" in patch:
            self._work_queue.set_worker_count(new_config.worker_count)

        if "paused" in patch:
            if new_config.paused:
                self._work_queue.pause()
            else:
                self._work_queue.resume()

    def submit_job(self, payload: dict) -> str:
        if self._work_queue is None:
            raise RuntimeError("CopyQueueModule is not running.")
        src = (payload.get("src_path") or "").strip()
        dst = (payload.get("dst_path") or "").strip()
        if not src or not dst:
            raise ValueError("payload must include non-empty 'src_path' and 'dst_path'")
        move_mode = bool(payload.get("move_mode", False))
        return self._work_queue.enqueue(src, dst, move_mode=move_mode)

    def status(self) -> ModuleStatus:
        return ModuleStatus(
            name=self.name,
            state=self._state,
            config=self._config.model_dump(),
            started_at=self._started_at,
            error="",
        )
