"""Optional desktop UI: log viewer and system tray."""

from __future__ import annotations

import logging
import queue


def launch_gui(log_queue: queue.Queue[logging.LogRecord], stop_event: object, server: object) -> None:
    """Start tray + log viewer (call from main thread; blocks in tk mainloop)."""
    from .tray import run_gui_blocking

    run_gui_blocking(log_queue, stop_event, server)
