"""Optional desktop UI: log viewer and system tray."""

from __future__ import annotations

import logging
import queue


def launch_gui(
    module_queues: dict[str, queue.Queue[logging.LogRecord]],
    stop_event: object,
    server: object,
) -> None:
    """Start tray + log viewer (call from main thread; blocks in tk mainloop)."""
    from opticnode.gui.tray import run_gui_blocking

    run_gui_blocking(module_queues, stop_event, server)
