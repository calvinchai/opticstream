"""WatcherModule: manages a watchdog filesystem observer as a node module."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from pydantic import Field
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .base import ModuleConfig, ModuleState, ModuleStatus, NodeModule

logger = logging.getLogger(__name__)


class WatcherConfig(ModuleConfig):
    watch_path: str = Field(default="")
    recursive: bool = False


class _WatchLogHandler(FileSystemEventHandler):
    def on_any_event(self, event: Any) -> None:
        logger.info("watcher: %s %s", event.event_type, getattr(event, "src_path", ""))


class WatcherModule(NodeModule):
    """Watches a directory for filesystem events using watchdog."""

    name = "watcher"
    Config = WatcherConfig

    def __init__(self) -> None:
        self._config: WatcherConfig = WatcherConfig()
        self._state = ModuleState.STOPPED
        self._started_at: float | None = None
        self._error: str = ""
        self._observer: Observer | None = None

    def start(self, config: WatcherConfig) -> None:
        if self._state == ModuleState.RUNNING:
            raise RuntimeError("WatcherModule is already running.")

        path = config.watch_path.strip()
        if not path:
            raise ValueError("WatcherModule requires a non-empty 'watch_path'.")
        p = Path(path)
        if not p.is_dir():
            raise ValueError(f"WatcherModule: watch_path is not a directory: {path}")

        obs = Observer()
        obs.schedule(_WatchLogHandler(), str(p.resolve()), recursive=config.recursive)
        obs.start()

        self._observer = obs
        self._config = config
        self._state = ModuleState.RUNNING
        self._started_at = time.time()
        self._error = ""
        logger.info("WatcherModule: started watching %s (recursive=%s).", path, config.recursive)

    def stop(self) -> None:
        obs = self._observer
        self._observer = None
        self._state = ModuleState.STOPPING
        if obs is not None:
            obs.stop()
            obs.join(timeout=10.0)
        self._state = ModuleState.STOPPED
        self._started_at = None
        logger.info("WatcherModule: stopped.")

    def reconfigure(self, patch: dict) -> None:
        new_config = WatcherConfig.model_validate({**self._config.model_dump(), **patch})
        was_running = self._state == ModuleState.RUNNING
        if was_running:
            self.stop()
            self.start(new_config)
        else:
            self._config = new_config

    def status(self) -> ModuleStatus:
        return ModuleStatus(
            name=self.name,
            state=self._state,
            config=self._config.model_dump(),
            started_at=self._started_at,
            error=self._error,
        )
