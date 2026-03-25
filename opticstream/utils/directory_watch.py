"""
Shared directory-watcher primitives for CLI watch commands (LSM, OCT, etc.).

Uses watchdog with a polling fallback and a stability window before processing.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path
from queue import Queue

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

logger = logging.getLogger(__name__)


def wait_until_stable(folder: Path, stable_seconds: int) -> None:
    """
    Block until the given folder has had no file modifications for ``stable_seconds``.
    """
    last_change = time.time()

    def snapshot() -> list[tuple[str, float]]:
        return [(p.name, p.stat().st_mtime) for p in folder.rglob("*") if p.is_file()]

    prev = snapshot()

    while True:
        time.sleep(2)
        curr = snapshot()
        if curr != prev:
            last_change = time.time()
            prev = curr
        elif time.time() - last_change >= stable_seconds:
            return


class NewFolderHandler(FileSystemEventHandler):
    """
    Watchdog handler that enqueues newly created subdirectories exactly once.
    """

    def __init__(
        self,
        queue: Queue,
        seen: set[Path],
        lock: threading.Lock,
        should_handle: Callable[[Path], bool],
    ) -> None:
        self.queue = queue
        self.seen = seen
        self.lock = lock
        self._should_handle = should_handle

    def on_created(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            path = Path(event.src_path)
            if not self._should_handle(path):
                return
            with self.lock:
                if path not in self.seen:
                    logger.info("[NEW] (watchdog) %s", path.name)
                    self.seen.add(path)
                    self.queue.put(path)


def polling_scanner(
    queue: Queue,
    seen: set[Path],
    lock: threading.Lock,
    watch_dir: Path,
    poll_interval: int,
    should_handle: Callable[[Path], bool],
) -> None:
    """
    Polling-based fallback for environments where watchdog misses events.
    """
    while True:
        try:
            for d in watch_dir.iterdir():
                if not d.is_dir() or not should_handle(d):
                    continue
                with lock:
                    if d not in seen:
                        logger.info("[NEW] (polling) %s", d.name)
                        seen.add(d)
                        queue.put(d)
            time.sleep(poll_interval)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Polling error: %s", exc)
            time.sleep(poll_interval)


def run_watcher_until_interrupt(
    observer: Observer,
    *,
    running_message: str = "Watcher running (watchdog + polling)",
) -> None:
    """
    Block until Ctrl+C, then stop and join the watchdog observer.

    Use from the main thread while consumer / polling threads run as daemons.
    """
    logger.info("%s", running_message)
    logger.info("Press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down watcher...")
    observer.stop()
    observer.join()
