"""Tkinter window showing recent log lines."""

from __future__ import annotations

import logging
import queue
import tkinter as tk
from tkinter import scrolledtext

logger = logging.getLogger(__name__)


class LogViewerWindow:
    """Toplevel with a scrolling log area; drains a queue.Queue of log records."""

    def __init__(self, master: tk.Tk, log_queue: queue.Queue[logging.LogRecord]) -> None:
        self._queue = log_queue
        self._win = tk.Toplevel(master)
        self._win.title("OpticNode logs")
        self._win.geometry("900x420")
        self._text = scrolledtext.ScrolledText(self._win, state="disabled", wrap="word", height=20)
        self._text.pack(fill="both", expand=True)
        self._win.protocol("WM_DELETE_WINDOW", self.hide)

    def poll(self) -> None:
        drained = False
        while True:
            try:
                record = self._queue.get_nowait()
            except queue.Empty:
                break
            drained = True
            msg = self._format_record(record)
            self._text.configure(state="normal")
            self._text.insert("end", msg + "\n")
            self._text.see("end")
            self._text.configure(state="disabled")
        if drained:
            # cap lines
            self._text.configure(state="normal")
            if int(self._text.index("end-1c").split(".")[0]) > 5000:
                self._text.delete("1.0", "1000.0")
            self._text.configure(state="disabled")

    @staticmethod
    def _format_record(record: logging.LogRecord) -> str:
        return f"{record.created:.3f} {record.levelname} {record.name}: {record.getMessage()}"

    def show(self) -> None:
        self._win.deiconify()
        self._win.lift()

    def hide(self) -> None:
        self._win.withdraw()
