from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from opticstream.cli.lsm.cli import lsm_cli
from opticstream.config import LSMScanConfig
from opticstream.config.lsm_scan_config import get_lsm_scan_config
from opticstream.events.lsm_event_emitters import emit_strip_lsm_event
from opticstream.events.lsm_events import STRIP_READY
from opticstream.flows.lsm.strip_process_flow import process_strip
from opticstream.state.lsm_project_state import LSMStripId, LSM_STATE_SERVICE
from opticstream.utils.filename_utils import (
    parse_lsm_run_folder_name
)
from opticstream.utils.polling_watcher import PollingStableWatcher
from opticstream.utils.process_priority_thread import (
    resolve_priority_flag,
    start_periodic_process_priority_thread,
)

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _configure_logging(verbose: bool) -> None:
    """Force log level on the root logger and ensure a StreamHandler is present.

    basicConfig() is a no-op when any library has already attached a handler,
    so we set the level explicitly here instead of relying on it.
    """
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        root.addHandler(handler)
    root.setLevel(level)
    # Ensure the opticstream namespace is not filtered out by a library-set level.
    logging.getLogger("opticstream").setLevel(level)


def _is_readable_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if not os.access(path, os.R_OK | os.X_OK):
        return False
    try:
        next(path.iterdir(), None)
    except (OSError, PermissionError):
        return False
    return True


def _can_snapshot_folder(folder: Path) -> bool:
    if not _is_readable_dir(folder):
        return False

    try:
        for p in folder.rglob("*"):
            if p.is_file():
                if not os.access(p, os.R_OK):
                    return False
                p.stat()
        return True
    except (OSError, PermissionError):
        return False


def _should_process_folder(path: Path) -> bool:
    return (
        (
            path.is_dir()
            and _is_readable_dir(path)
            and not path.name.lower().startswith("processed")
            and sum(1 for p in path.rglob("*") if p.is_file()) > 100
        )
    )


def _folder_fingerprint(folder: Path) -> tuple[tuple[str, float, int], ...]:
    items: list[tuple[str, float, int]] = []
    for p in folder.rglob("*"):
        if p.is_file():
            stat = p.stat()
            items.append((str(p.relative_to(folder)), stat.st_mtime, stat.st_size))
    items.sort()
    return tuple(items)


@dataclass(frozen=True)
class LSMFolderCandidate:
    folder: Path


class LSMWatcherService:
    def __init__(
        self,
        *,
        project_name: str,
        scan_config: LSMScanConfig,
        watch_dir: Path,
        slice_offset: int,
        direct: bool,
        force_resend: bool,
    ) -> None:
        self.project_name = project_name
        self.scan_config = scan_config
        self.watch_dir = watch_dir
        self.slice_offset = slice_offset
        self.direct = direct
        self.force_resend = force_resend
        self._seen_folders: set[Path] = set()

    def discover_candidates(self) -> list[LSMFolderCandidate]:
        if not _is_readable_dir(self.watch_dir):
            logger.warning("LSM watch directory is not readable: %s", self.watch_dir)
            return []

        candidates = [
            LSMFolderCandidate(folder=d)
            for d in self.watch_dir.iterdir()
            if _should_process_folder(d)
        ]

        for candidate in candidates:
            if candidate.folder not in self._seen_folders:
                self._seen_folders.add(candidate.folder)
                logger.info("New LSM folder detected: %s", candidate.folder)

        return candidates

    def candidate_key(self, candidate: LSMFolderCandidate) -> str:
        return str(candidate.folder.resolve())

    def fingerprint(self, candidate: LSMFolderCandidate) -> object:
        if not _can_snapshot_folder(candidate.folder):
            raise OSError(f"Folder is not readable/snapshot-able: {candidate.folder}")
        return _folder_fingerprint(candidate.folder)

    def process(self, candidate: LSMFolderCandidate) -> int:
        folder = candidate.folder

        if not _can_snapshot_folder(folder):
            logger.warning("Skipping unreadable LSM folder: %s", folder)
            return 0

        try:
            slice_index, strip_id, channel_index = parse_lsm_run_folder_name(folder.name)
        except Exception as exc:
            logger.warning(
                "Skipping folder %r: cannot parse indices (%s)", folder.name, exc
            )
            return 0

        slice_id = slice_index + self.slice_offset
        strip_ident = LSMStripId(
            project_name=self.project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            channel_id=channel_index,
        )

        existing = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
        if existing is not None and not self.force_resend:
            logger.info(
                "[SKIP] Strip state already exists for %s (use --force-resend to override)",
                strip_ident,
            )
            return 0

        if self.direct:
            return self._process_direct(strip_ident=strip_ident, folder=folder)

        return self._process_event(strip_ident=strip_ident, folder=folder)

    def _process_event(self, *, strip_ident: LSMStripId, folder: Path) -> int:
        extra_payload: dict = {"strip_path": str(folder)}
        if self.force_resend:
            extra_payload["force_rerun"] = True

        logger.info("Emitting STRIP_READY for %s from %s", strip_ident, folder)
        start = time.perf_counter()

        emit_strip_lsm_event(STRIP_READY, strip_ident, extra_payload=extra_payload)

        with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident):
            pass

        elapsed = time.perf_counter() - start
        logger.info("Emitted STRIP_READY for %s in %.2f s", strip_ident, elapsed)
        return 1

    def _process_direct(self, *, strip_ident: LSMStripId, folder: Path) -> int:
        logger.info("Direct-processing LSM strip %s from %s", strip_ident, folder)

        process_strip(
            strip_ident=strip_ident,
            scan_config=self.scan_config,
            strip_path=folder,
            force_rerun=self.force_resend,
        )
        return 1


def watch_lsm(
    *,
    project_name: str,
    watch_dir: Path,
    scan_config: LSMScanConfig,
    slice_offset: int = 0,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = False,
    force_resend: bool = False,
    process: str = "AndorSolis",
    priority: str | None = "high",
    no_pp: bool = False,
) -> None:
    priority_handle = None
    resolved_priority: int | None = None
    if not no_pp:
        resolved_priority = resolve_priority_flag(priority)
        priority_handle = start_periodic_process_priority_thread(
            process,
            priority=resolved_priority,
            daemon=False,
        )
    try:
        service = LSMWatcherService(
            project_name=project_name,
            scan_config=scan_config,
            watch_dir=watch_dir,
            slice_offset=slice_offset,
            direct=direct,
            force_resend=force_resend,
        )

        logger.info(
            "Starting LSM watcher | project=%s watch_dir=%s mode=%s "
            "poll_interval=%ss stability=%ss slice_offset=%s force_resend=%s "
            "process_priority=%s process_priority_process=%r process_priority_value=%s",
            project_name,
            watch_dir,
            "direct" if direct else "event",
            poll_interval,
            stability_seconds,
            slice_offset,
            force_resend,
            "off" if no_pp else "on",
            process,
            "n/a"
            if no_pp
            else (
                resolved_priority
                if resolved_priority is not None
                else "(default)"
            ),
        )

        watcher = PollingStableWatcher[LSMFolderCandidate, str](
            discover_candidates=service.discover_candidates,
            candidate_key=service.candidate_key,
            fingerprint=service.fingerprint,
            process=service.process,
            poll_interval=poll_interval,
            stability_seconds=stability_seconds,
            running_message=(
                f"LSM polling watcher running ({'direct' if direct else 'event'} mode)"
            ),
        )
        watcher.run()
    finally:
        if priority_handle is not None:
            priority_handle.stop()


@lsm_cli.command
def watch(
    project_name: str,
    watch_dir: str = ".",
    *,
    slice_offset: int = 0,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = False,
    force_resend: bool = False,
    process: Annotated[
        str,
        Parameter(
            name=["--process", "-p"],
            help="Executable name for periodic CPU priority adjustment (default: AndorSolis).",
        ),
    ] = "AndorSolis",
    priority: Annotated[
        str | None,
        Parameter(
            name=["--priority", "-l"],
            help=(
                "Target priority: same names on Windows and POSIX (high, normal, …); "
                "POSIX also accepts a nice integer -20..19. Default: high."
            ),
        ),
    ] = "high",
    no_pp: Annotated[
        bool,
        Parameter(
            name="--no-pp",
            help="Do not start the periodic process-priority background thread.",
        ),
    ] = False,
    verbose: bool = False,
) -> None:
    """
    Poll for new LSM run folders and dispatch each stable strip either by:
    - emitting STRIP_READY, or
    - running the strip flow directly.

    Only immediate subdirectories whose names start with 'Run' are considered.
    """
    _configure_logging(verbose)

    scan_config = get_lsm_scan_config(project_name)
    watch_path = Path(watch_dir)

    if not watch_path.exists():
        raise ValueError(f"Watch directory {watch_path} does not exist")
    if not _is_readable_dir(watch_path):
        raise ValueError(f"Watch directory {watch_path} is not readable")

    watch_lsm(
        project_name=project_name,
        watch_dir=watch_path,
        scan_config=scan_config,
        slice_offset=slice_offset,
        stability_seconds=stability_seconds,
        poll_interval=poll_interval,
        direct=direct,
        force_resend=force_resend,
        process=process,
        priority=priority,
        no_pp=no_pp,
    )
