from __future__ import annotations

import logging
import sys
import threading
from dataclasses import dataclass

import psutil

logger = logging.getLogger(__name__)


def _windows_priority_by_name() -> dict[str, int]:
    """Windows-only priority classes (not all exist in ``psutil`` on POSIX)."""
    return {
        "idle": psutil.IDLE_PRIORITY_CLASS,
        "below_normal": psutil.BELOW_NORMAL_PRIORITY_CLASS,
        "normal": psutil.NORMAL_PRIORITY_CLASS,
        "above_normal": psutil.ABOVE_NORMAL_PRIORITY_CLASS,
        "high": psutil.HIGH_PRIORITY_CLASS,
        "realtime": psutil.REALTIME_PRIORITY_CLASS,
    }


def _posix_priority_by_name() -> dict[str, int]:
    """Rough nice-value equivalents for the same names as on Windows."""
    return {
        "idle": 10,
        "below_normal": 5,
        "normal": 0,
        "above_normal": -2,
        "high": -5,
        "realtime": -15,
    }


def resolve_priority_flag(value: str | None) -> int | None:
    """
    Map CLI ``--priority`` to a value for :func:`start_periodic_process_priority_thread`.

    * ``None`` — use the platform default (normal on Windows, nice 0 on POSIX).
    * Windows — one of: idle, below_normal, normal, above_normal, high, realtime.
    * POSIX — same names map to approximate nice values, or a decimal integer in ``-20..19``.
    """
    if value is None:
        return None
    stripped = value.strip()
    if sys.platform == "win32":
        table = _windows_priority_by_name()
        key = stripped.lower()
        if key not in table:
            valid = ", ".join(sorted(table))
            raise ValueError(
                f"Invalid Windows priority class {value!r}. Choose one of: {valid}"
            )
        return table[key]
    posix_named = _posix_priority_by_name()
    key = stripped.lower()
    if key in posix_named:
        return posix_named[key]
    try:
        n = int(stripped, 10)
    except ValueError as exc:
        raise ValueError(
            f"On POSIX, --priority must be a known name ({', '.join(sorted(posix_named))}) "
            f"or an integer nice value (-20..19), got {value!r}"
        ) from exc
    if not -20 <= n <= 19:
        raise ValueError(f"nice value must be in [-20, 19], got {n}")
    return n


def _default_priority() -> int:
    if sys.platform == "win32":
        return psutil.NORMAL_PRIORITY_CLASS
    return 0


def process_name_matches(proc_name: str | None, wanted: str) -> bool:
    if not proc_name or not wanted:
        return False
    a = proc_name.lower()
    b = wanted.lower()
    if a == b:
        return True
    if a.endswith(".exe") and a[:-4] == b:
        return True
    if b.endswith(".exe") and b[:-4] == a:
        return True
    return False


def _apply_process_priority(process_name: str, priority: int) -> None:
    for proc in psutil.process_iter(["pid", "name"]):
        info = proc.info
        name = info.get("name")
        if not process_name_matches(name, process_name):
            continue
        pid = info.get("pid")
        if pid is None:
            continue
        try:
            p = psutil.Process(pid)
            current = p.nice()
            if current != priority:
                p.nice(priority)
            logger.debug(
                "Set priority for pid=%s name=%r to %s (was %s)",
                pid,
                name,
                priority,
                current,
            )
        except psutil.AccessDenied:
            logger.warning(
                "Access denied setting priority for pid=%s name=%r",
                pid,
                name,
            )
        except psutil.NoSuchProcess:
            continue


def _priority_loop(
    process_name: str,
    priority: int,
    interval_seconds: float,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            _apply_process_priority(process_name, priority)
        except Exception:
            logger.exception(
                "Unexpected error while adjusting priority for %r",
                process_name,
            )
        if stop_event.wait(timeout=interval_seconds):
            break


@dataclass(frozen=True)
class PeriodicProcessPriorityHandle:
    """Handle for a background thread that periodically sets process priority."""

    thread: threading.Thread
    _stop: threading.Event

    def stop(self, timeout: float | None = 5.0) -> None:
        self._stop.set()
        self.thread.join(timeout=timeout)


def start_periodic_process_priority_thread(
    process_name: str,
    *,
    priority: int | None = None,
    interval_seconds: float = 60.0,
    daemon: bool = True,
) -> PeriodicProcessPriorityHandle:
    """
    Start a daemon thread that every ``interval_seconds`` finds processes whose
    executable name matches ``process_name`` and sets their scheduling priority.

    On Windows, ``priority`` should be a ``psutil`` priority class constant such as
    ``psutil.BELOW_NORMAL_PRIORITY_CLASS``. On POSIX, pass a nice value in ``-20..19``.

    Name matching is case-insensitive; ``"foo"`` matches ``"foo.exe"`` on Windows.
    """
    resolved = _default_priority() if priority is None else priority
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_priority_loop,
        args=(process_name, resolved, interval_seconds, stop_event),
        name=f"process-priority:{process_name}",
        daemon=daemon,
    )
    thread.start()
    return PeriodicProcessPriorityHandle(thread=thread, _stop=stop_event)
