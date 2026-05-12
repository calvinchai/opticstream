"""System tray + Tk main loop (desktop mode)."""

from __future__ import annotations

import logging
import threading
import tkinter as tk
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opticnode.app.runtime import NodeRuntime

logger = logging.getLogger(__name__)


def _make_icon_image() -> Any:
    from PIL import Image

    return Image.new("RGB", (64, 64), color=(32, 110, 85))


def run_gui_blocking(runtime: "NodeRuntime") -> None:
    """Run Tk on the main thread; pystray icon in a background thread."""
    import pystray
    from pystray import Menu, MenuItem

    root = tk.Tk()
    root.withdraw()

    from opticnode.gui.main_window import MainWindow

    main = MainWindow(root, runtime)

    def on_quit(icon: Any, _item: Any) -> None:
        runtime.stop()
        icon.stop()
        root.after(0, root.quit)

    def on_show(_icon: Any, _item: Any) -> None:
        root.after(0, main.show)

    def on_hide(_icon: Any, _item: Any) -> None:
        root.after(0, main.hide)

    menu = Menu(
        MenuItem("Open", on_show),
        MenuItem("Hide", on_hide),
        MenuItem("Quit", on_quit),
    )
    icon = pystray.Icon("opticnode", _make_icon_image(), "OpticNode", menu)

    def run_tray() -> None:
        icon.run()

    threading.Thread(target=run_tray, name="pystray", daemon=True).start()
    root.mainloop()
    try:
        icon.stop()
    except Exception:
        pass
