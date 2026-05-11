"""Optional desktop UI: log viewer and system tray."""

from __future__ import annotations

import logging
import queue


def launch_gui(
    module_queues: dict[str, queue.Queue[logging.LogRecord]],
    stop_event: object,
    server: object,
) -> None:
    """Start tray + log viewer (call from main thread; blocks in tk mainloop).

    ``module_queues`` maps a display name (module name or "core") to a queue
    populated by a QueueHandler.  Each entry gets its own tab in the log viewer.
    """
    from .tray import run_gui_blocking

    run_gui_blocking(module_queues, stop_event, server)
