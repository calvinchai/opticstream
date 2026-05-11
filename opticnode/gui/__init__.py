"""Optional desktop UI: log viewer and system tray."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opticnode.app.runtime import NodeRuntime


def launch_gui(runtime: "NodeRuntime") -> None:
    """Start tray + log viewer (call from main thread; blocks in tk mainloop)."""
    from opticnode.gui.tray import run_gui_blocking

    run_gui_blocking(runtime.get_log_queues(), runtime)
