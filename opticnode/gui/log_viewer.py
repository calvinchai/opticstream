"""Tkinter window showing per-module log tabs."""

from __future__ import annotations

import logging
import queue
import tkinter as tk
from tkinter import scrolledtext, ttk

logger = logging.getLogger(__name__)

_MAX_LINES = 5000
_CULL_LINES = 1000


class LogViewerWindow:
    """Toplevel with one scrolling tab per module; drains per-module queues."""

    def __init__(
        self,
        master: tk.Tk,
        queues: dict[str, queue.Queue[logging.LogRecord]],
    ) -> None:
        self._queues = queues
        self._win = tk.Toplevel(master)
        self._win.title("OpticNode logs")
        self._win.geometry("900x500")

        notebook = ttk.Notebook(self._win)
        notebook.pack(fill="both", expand=True)

        self._texts: dict[str, scrolledtext.ScrolledText] = {}
        for name in queues:
            frame = ttk.Frame(notebook)
            notebook.add(frame, text=name)
            text = scrolledtext.ScrolledText(frame, state="disabled", wrap="word", height=24)
            text.pack(fill="both", expand=True)
            self._texts[name] = text

        self._win.protocol("WM_DELETE_WINDOW", self.hide)

    def poll(self) -> None:
        for name, q in self._queues.items():
            text = self._texts.get(name)
            if text is None:
                continue
            drained = False
            while True:
                try:
                    record = q.get_nowait()
                except queue.Empty:
                    break
                drained = True
                msg = _format_record(record)
                text.configure(state="normal")
                text.insert("end", msg + "\n")
                text.see("end")
                text.configure(state="disabled")
            if drained:
                text.configure(state="normal")
                if int(text.index("end-1c").split(".")[0]) > _MAX_LINES:
                    text.delete("1.0", f"{_CULL_LINES}.0")
                text.configure(state="disabled")

    def show(self) -> None:
        self._win.deiconify()
        self._win.lift()

    def hide(self) -> None:
        self._win.withdraw()


def _format_record(record: logging.LogRecord) -> str:
    return f"{record.created:.3f} {record.levelname} {record.name}: {record.getMessage()}"
