from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

from opticstream.cli.lsm.cli import lsm_cli
from opticstream.config import LSMScanConfig
from opticstream.config.lsm_scan_config import get_lsm_scan_config
from opticstream.events.lsm_event_emitters import emit_strip_lsm_event
from opticstream.events.lsm_events import STRIP_READY
from opticstream.flows.lsm.strip_process_flow import process_strip
from opticstream.state.lsm_project_state import LSMStripId, LSM_STATE_SERVICE
from opticstream.utils.filename_utils import (
    parse_lsm_run_folder_name,
    parse_lsm_strip_index,
)
from opticstream.utils.polling_watcher import PollingStableWatcher

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)


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
            and path.name.lower().startswith("run")
            and _is_readable_dir(path)
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

    def discover_candidates(self) -> list[LSMFolderCandidate]:
        if not _is_readable_dir(self.watch_dir):
            logger.warning("LSM watch directory is not readable: %s", self.watch_dir)
            return []
        
        return [
            LSMFolderCandidate(folder=d)
            for d in self.watch_dir.iterdir()
            if _should_process_folder(d)
        ]

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
            slice_index, strip_id, channel_index = parse_lsm_strip_index(
                *parse_lsm_run_folder_name(folder.name)[1:],
                self.scan_config.strips_per_slice,
            )
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
) -> None:
    service = LSMWatcherService(
        project_name=project_name,
        scan_config=scan_config,
        watch_dir=watch_dir,
        slice_offset=slice_offset,
        direct=direct,
        force_resend=force_resend,
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
) -> None:
    """
    Poll for new LSM run folders and dispatch each stable strip either by:
    - emitting STRIP_READY, or
    - running the strip flow directly.

    Only immediate subdirectories whose names start with 'Run' are considered.
    """
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
    )
