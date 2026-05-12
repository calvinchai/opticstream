"""Tkinter log panels: drain a logging queue into a scrolling text widget."""

from __future__ import annotations

import logging
import queue
import tkinter as tk
from tkinter import scrolledtext, ttk

_MAX_LINES = 5000
_CULL_LINES = 1000


def format_log_record(record: logging.LogRecord) -> str:
    return f"{record.created:.3f} {record.levelname} {record.name}: {record.getMessage()}"


class LogPanel(ttk.Frame):
    """Single scrolling log view fed by an optional QueueHandler queue."""

    def __init__(
        self,
        master: tk.Misc,
        log_queue: queue.Queue[logging.LogRecord] | None = None,
        *,
        height: int = 12,
    ) -> None:
        super().__init__(master)
        self._queue = log_queue
        self._text = scrolledtext.ScrolledText(self, state="disabled", wrap="word", height=height)
        self._text.pack(fill="both", expand=True)

    def poll(self) -> None:
        if self._queue is None:
            return
        drained = False
        while True:
            try:
                record = self._queue.get_nowait()
            except queue.Empty:
                break
            drained = True
            msg = format_log_record(record)
            self._text.configure(state="normal")
            self._text.insert("end", msg + "\n")
            self._text.see("end")
            self._text.configure(state="disabled")
        if drained:
            self._text.configure(state="normal")
            if int(self._text.index("end-1c").split(".")[0]) > _MAX_LINES:
                self._text.delete("1.0", f"{_CULL_LINES}.0")
            self._text.configure(state="disabled")
