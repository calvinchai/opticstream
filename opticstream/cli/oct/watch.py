from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from opticstream.cli.oct import oct_cli
from opticstream.config.project_config import get_project_config_block
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.events import BATCH_READY
from opticstream.events.psoct_event_emitters import emit_batch_psoct_event
from opticstream.flows.psoct.tile_batch_process_flow import process_tile_batch
from opticstream.flows.psoct.utils import oct_batch_ident
from opticstream.state.oct_project_state import OCT_STATE_SERVICE
from opticstream.utils.polling_watcher import PollingStableWatcher
from opticstream.utils.refresh_disk import RefreshHook, resolve_refresh_hook

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


def _is_readable_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if not os.access(path, os.R_OK):
        return False
    try:
        path.stat()
    except (OSError, PermissionError):
        return False
    return True


def _all_files_readable(files: tuple[Path, ...]) -> bool:
    return all(_is_readable_file(p) for p in files)


def _is_oct_spectral_file(path: Path) -> bool:
    return (
        path.is_file()
        and path.suffix == ".nii"
        and "cropped_focus" in path.name
        and _is_readable_file(path)
    )


def _parse_mosaic_ranges(mosaic_ranges_str: str) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for range_str in mosaic_ranges_str.split(","):
        parts = range_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid mosaic range format: {range_str!r}. Expected 'min:max'")
        min_id, max_id = int(parts[0]), int(parts[1])
        out.append((min_id, max_id))
    return out


def _parse_mosaic_id(filename: str) -> int:
    return int(filename.split("_")[1])


def _parse_tile_index(filename: str) -> int:
    return int(filename.split("_")[3])


def _batch_fingerprint(
    files: tuple[Path, ...],
    root: Path,
) -> tuple[tuple[str, float, int], ...]:
    items: list[tuple[str, float, int]] = []
    for p in files:
        stat = p.stat()
        items.append((str(p.relative_to(root)), stat.st_mtime, stat.st_size))
    items.sort()
    return tuple(items)


@dataclass(frozen=True)
class OCTBatchCandidate:
    source_mosaic_id: int
    mosaic_id: int
    logical_batch: int
    files: tuple[Path, ...]


class OCTWatcherService:
    def __init__(
        self,
        *,
        project_name: str,
        folder_path: Path,
        project_base_path: str,
        mosaic_ranges: list[tuple[int, int]],
        mosaic_offset: int,
        batch_size: int,
        scan_config: PSOCTScanConfigModel,
        direct: bool,
        force_resend: bool,
        refresh_hook: RefreshHook | None = None,
    ) -> None:
        self.project_name = project_name
        self.folder_path = folder_path
        self.project_base_path = project_base_path
        self.mosaic_ranges = mosaic_ranges
        self.mosaic_offset = mosaic_offset
        self.batch_size = batch_size
        self.scan_config = scan_config
        self.direct = direct
        self.force_resend = force_resend
        self.refresh_hook = refresh_hook

    def discover_candidates(self) -> list[OCTBatchCandidate]:
        if not _is_readable_dir(self.folder_path):
            logger.warning("OCT watch directory is not readable: %s", self.folder_path)
            return []

        spectral_files = [
            p for p in self.folder_path.glob("*cropped_focus*.nii")
            if _is_oct_spectral_file(p)
        ]

        files_by_source_mosaic: dict[int, list[Path]] = defaultdict(list)
        for f in spectral_files:
            try:
                source_mosaic_id = _parse_mosaic_id(f.name)
                files_by_source_mosaic[source_mosaic_id].append(f)
            except (ValueError, IndexError):
                logger.warning("Could not parse mosaic_id from %s, skipping", f.name)

        out: list[OCTBatchCandidate] = []

        for min_source_mosaic_id, max_source_mosaic_id in self.mosaic_ranges:
            for source_mosaic_id in range(min_source_mosaic_id, max_source_mosaic_id + 1):
                mosaic_files = files_by_source_mosaic.get(source_mosaic_id)
                if not mosaic_files:
                    continue

                valid_files: list[Path] = []
                for f in mosaic_files:
                    try:
                        _parse_tile_index(f.name)
                        valid_files.append(f)
                    except (ValueError, IndexError):
                        logger.warning("Could not parse tile_index from %s, skipping", f.name)

                valid_files.sort(key=lambda p: _parse_tile_index(p.name))

                batches: dict[int, list[Path]] = {}
                for f in valid_files:
                    tile_index = _parse_tile_index(f.name)
                    logical_batch = (tile_index - 1) // self.batch_size + 1
                    batches.setdefault(logical_batch, []).append(f)

                for logical_batch, batch_files in sorted(batches.items()):
                    if len(batch_files) < self.batch_size:
                        logger.warning(
                            "Incomplete batch source_mosaic=%s logical_batch=%s (%s files, need %s)",
                            source_mosaic_id,
                            logical_batch,
                            len(batch_files),
                            self.batch_size,
                        )
                        continue

                    candidate_files = tuple(sorted(batch_files))
                    if not _all_files_readable(candidate_files):
                        continue

                    out.append(
                        OCTBatchCandidate(
                            source_mosaic_id=source_mosaic_id,
                            mosaic_id=source_mosaic_id + self.mosaic_offset,
                            logical_batch=logical_batch,
                            files=candidate_files,
                        )
                    )

        return out

    def candidate_key(self, candidate: OCTBatchCandidate) -> tuple[int, int]:
        return (candidate.mosaic_id, candidate.logical_batch)

    def fingerprint(self, candidate: OCTBatchCandidate) -> object:
        if not _all_files_readable(candidate.files):
            raise OSError(
                f"Batch contains unreadable files: {[str(p) for p in candidate.files if not _is_readable_file(p)]}"
            )
        return _batch_fingerprint(candidate.files, self.folder_path)

    def process(self, candidate: OCTBatchCandidate) -> int:
        if not _all_files_readable(candidate.files):
            logger.warning(
                "Skipping unreadable OCT batch source_mosaic=%s mosaic=%s logical_batch=%s",
                candidate.source_mosaic_id,
                candidate.mosaic_id,
                candidate.logical_batch,
            )
            return 0

        batch_ident = oct_batch_ident(
            self.project_name,
            candidate.mosaic_id,
            candidate.logical_batch,
        )

        existing = OCT_STATE_SERVICE.peek_batch(batch_ident=batch_ident)
        if existing is not None and not self.force_resend:
            logger.info(
                "[SKIP] batch state exists for %s (use --force-resend to override)",
                batch_ident,
            )
            return 0

        if self.direct:
            return self._process_direct(candidate, batch_ident)

        return self._process_event(candidate, batch_ident)

    def _process_direct(self, candidate: OCTBatchCandidate, batch_ident: Any) -> int:
        logger.info(
            "Direct-processing OCT batch source_mosaic=%s mosaic=%s logical_batch=%s files=%s",
            candidate.source_mosaic_id,
            candidate.mosaic_id,
            candidate.logical_batch,
            len(candidate.files),
        )

        process_tile_batch(
            batch_id=batch_ident,
            config=self.scan_config,
            file_list=list(candidate.files),
            force_rerun=self.force_resend,
        )

        if self.refresh_hook is not None:
            self.refresh_hook()

        return 1

    def _process_event(self, candidate: OCTBatchCandidate, batch_ident: Any) -> int:
        logger.info(
            "Emitting BATCH_READY source_mosaic=%s mosaic=%s logical_batch=%s files=%s",
            candidate.source_mosaic_id,
            candidate.mosaic_id,
            candidate.logical_batch,
            len(candidate.files),
        )

        extra: dict[str, Any] = {
            "file_list": [str(p) for p in candidate.files],
            "project_base_path": self.project_base_path,
        }
        if self.force_resend:
            extra["force_rerun"] = True

        emit_batch_psoct_event(BATCH_READY, batch_ident, extra_payload=extra)
        with OCT_STATE_SERVICE.open_batch(batch_ident=batch_ident):
            pass
        return 1


def watch_oct(
    *,
    project_name: str,
    folder_path: Path,
    project_base_path: str,
    mosaic_ranges: list[tuple[int, int]],
    mosaic_offset: int,
    batch_size: int,
    scan_config: PSOCTScanConfigModel,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = True,
    force_resend: bool = False,
    refresh_hook: RefreshHook | None = None,
) -> None:
    service = OCTWatcherService(
        project_name=project_name,
        folder_path=folder_path,
        project_base_path=project_base_path,
        mosaic_ranges=mosaic_ranges,
        mosaic_offset=mosaic_offset,
        batch_size=batch_size,
        scan_config=scan_config,
        direct=direct,
        force_resend=force_resend,
        refresh_hook=refresh_hook,
    )

    watcher = PollingStableWatcher[OCTBatchCandidate, tuple[int, int]](
        discover_candidates=service.discover_candidates,
        candidate_key=service.candidate_key,
        fingerprint=service.fingerprint,
        process=service.process,
        poll_interval=poll_interval,
        stability_seconds=stability_seconds,
        running_message=(
            f"OCT polling watcher running "
            f"({'direct' if direct else 'event'} mode)"
        ),
    )
    watcher.run()


@oct_cli.command
def watch(
    project_name: str,
    folder_path: str,
    mosaic_ranges: str,
    *,
    mosaic_offset: int = 0,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = True,
    refresh: str | None = None,
    force_resend: bool = False,
) -> None:
    """
    Poll for complete OCT spectral batches in one folder and dispatch each stable batch either by:
    - running process_tile_batch directly, or
    - emitting BATCH_READY.

    mosaic_ranges: comma-separated min:max ranges, e.g. "1:2,5:8"
    mosaic_offset: logical offset applied to parsed mosaic ids for state/event identity
    refresh: optional refresh hook name, e.g. "sas2"
    """
    project_config = get_project_config_block(project_name)
    if project_config is None:
        raise ValueError(
            f"Project config block for '{project_name}' not found. "
            "Run 'opticstream oct setup' first to create the config block."
        )

    scan_config = PSOCTScanConfigModel.model_validate(project_config.model_dump())
    project_base_path = str(project_config.project_base_path)
    batch_size = project_config.acquisition.grid_size_y
    watch_path = Path(folder_path)

    if not watch_path.exists():
        raise ValueError(f"Watch directory {watch_path} does not exist")
    if not _is_readable_dir(watch_path):
        raise ValueError(f"Watch directory {watch_path} is not readable")

    refresh_hook = resolve_refresh_hook(refresh)

    logger.info("Using batch_size (grid_size_y): %s", batch_size)

    watch_oct(
        project_name=project_name,
        folder_path=watch_path,
        project_base_path=project_base_path,
        mosaic_ranges=_parse_mosaic_ranges(mosaic_ranges),
        mosaic_offset=mosaic_offset,
        batch_size=batch_size,
        scan_config=scan_config,
        stability_seconds=stability_seconds,
        poll_interval=poll_interval,
        direct=direct,
        force_resend=force_resend,
        refresh_hook=refresh_hook,
    )