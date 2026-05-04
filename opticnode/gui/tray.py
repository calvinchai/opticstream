"""System tray + Tk main loop (desktop mode)."""

from __future__ import annotations

import logging
import queue
import threading
import tkinter as tk
from typing import Any

logger = logging.getLogger(__name__)


def _make_icon_image() -> Any:
    from PIL import Image

    return Image.new("RGB", (64, 64), color=(32, 110, 85))


def run_gui_blocking(log_queue: queue.Queue[logging.LogRecord], stop_event: Any, server: Any) -> None:
    """Run Tk on the main thread; pystray icon in a background thread."""
    import pystray
    from pystray import Menu, MenuItem

    root = tk.Tk()
    root.withdraw()

    from .log_viewer import LogViewerWindow

    viewer = LogViewerWindow(root, log_queue)

    def on_quit(icon: Any, _item: Any) -> None:
        stop_event.set()
        try:
            server.stop(5)
        except Exception:
            logger.exception("gRPC server stop failed")
        icon.stop()
        root.after(0, root.quit)

    def on_show(_icon: Any, _item: Any) -> None:
        root.after(0, viewer.show)

    def on_hide(_icon: Any, _item: Any) -> None:
        root.after(0, viewer.hide)

    menu = Menu(
        MenuItem("Show logs", on_show),
        MenuItem("Hide logs", on_hide),
        MenuItem("Quit", on_quit),
    )
    icon = pystray.Icon("opticnode", _make_icon_image(), "OpticNode", menu)

    def poll_logs() -> None:
        viewer.poll()
        root.after(150, poll_logs)

    def run_tray() -> None:
        icon.run()

    threading.Thread(target=run_tray, name="pystray", daemon=True).start()
    poll_logs()
    root.mainloop()
    try:
        icon.stop()
    except Exception:
        pass
