from __future__ import annotations

"""
Cyclopts subapp for the periodic process-priority daemon.
"""

import logging
import sys
from typing import Annotated, Literal

import psutil
from cyclopts import App, Parameter

from opticstream.cli.root import app
from opticstream.utils.process_priority_thread import start_periodic_process_priority_thread

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)

process_priority_cli = app.command(
    App(
        name="process-priority",
        help="Periodically set CPU priority for processes matching a name.",
    )
)

WindowsPriority = Literal[
    "idle",
    "below_normal",
    "normal",
    "above_normal",
    "high",
    "realtime",
]


def _windows_priority_value(name: WindowsPriority) -> int:
    return {
        "idle": psutil.IDLE_PRIORITY_CLASS,
        "below_normal": psutil.BELOW_NORMAL_PRIORITY_CLASS,
        "normal": psutil.NORMAL_PRIORITY_CLASS,
        "above_normal": psutil.ABOVE_NORMAL_PRIORITY_CLASS,
        "high": psutil.HIGH_PRIORITY_CLASS,
        "realtime": psutil.REALTIME_PRIORITY_CLASS,
    }[name]


@process_priority_cli.default
def run(
    process_name: Annotated[
        str,
        Parameter(help="Executable name to match (e.g. notepad or notepad.exe)."),
    ],
    *,
    interval_seconds: Annotated[
        float,
        Parameter(
            name=["--interval-seconds", "-i"],
            help="Seconds between scans (default: 60).",
        ),
    ] = 60.0,
    windows_priority: Annotated[
        WindowsPriority | None,
        Parameter(
            name=["--windows-priority", "-w"],
            help="Windows priority class (POSIX: ignored). Default: normal.",
        ),
    ] = None,
    nice: Annotated[
        int | None,
        Parameter(
            name=["--nice", "-n"],
            help="POSIX nice value -20..19 (Windows: ignored). Default: 0.",
        ),
    ] = None,
) -> None:
    """
    Run in the foreground until interrupted (Ctrl+C).

    Matches processes by executable name (see process_priority_thread helpers)
    and applies priority on each interval.
    """
    if interval_seconds <= 0:
        raise ValueError("--interval-seconds must be positive")

    if sys.platform == "win32":
        if nice is not None:
            logger.warning("--nice is ignored on Windows; use --windows-priority")
        priority = (
            _windows_priority_value(windows_priority)
            if windows_priority is not None
            else None
        )
    else:
        if windows_priority is not None:
            logger.warning(
                "--windows-priority is ignored on this platform; use --nice"
            )
        if nice is not None and not -20 <= nice <= 19:
            raise ValueError("--nice must be between -20 and 19")
        priority = nice

    logger.info(
        "Starting process priority daemon | process_name=%r interval=%ss priority=%s",
        process_name,
        interval_seconds,
        priority if priority is not None else "(default for platform)",
    )
    handle = start_periodic_process_priority_thread(
        process_name,
        priority=priority,
        interval_seconds=interval_seconds,
        daemon=False,
    )
    try:
        handle.thread.join()
    except KeyboardInterrupt:
        logger.info("Stopping …")
    finally:
        handle.stop()
