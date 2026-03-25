from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from opticstream.cli.oct import oct_cli
from opticstream.config.project_config import get_project_config_block
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel, TileSavingType
from opticstream.events import BATCH_READY
from opticstream.events.psoct_event_emitters import emit_batch_psoct_event
from opticstream.flows.psoct.tile_batch_process_flow import process_tile_batch
from opticstream.flows.psoct.utils import oct_batch_ident
from opticstream.flows.psoct.utils import (
    logical_first_mosaic_from_source_slice,
    logical_mosaic_from_source_mosaic,
    slice_from_mosaic,
)
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


_COMPLEX_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_processed(?:_cropped)?\.nii$",
    re.IGNORECASE,
)
_SPECTRAL_NII_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_spectral_(?P<k>\d+)\.nii$",
    re.IGNORECASE,
)
_SPECTRAL_RAW_RE = re.compile(
    r"^mosaic_(?P<mosaic>\d+)_image_(?P<image>\d+)_spectral_(?P<k>\d+)\.raw$",
    re.IGNORECASE,
)
_SLICE_RE = re.compile(
    r"^slice_(?P<slice>\d+).+\.nii$",
    re.IGNORECASE,
)


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


def _parse_mosaic_ranges(mosaic_ranges_str: str) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for range_str in mosaic_ranges_str.split(","):
        parts = range_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid mosaic range format: {range_str!r}. Expected 'min:max'"
            )
        out.append((int(parts[0]), int(parts[1])))
    return out


@dataclass(frozen=True)
class ParsedTileFile:
    path: Path
    source_mosaic_id: int
    image_index: int


@dataclass(frozen=True)
class ParsedSliceFile:
    path: Path
    source_slice_id: int


@dataclass(frozen=True)
class OCTBatchCandidate:
    source_slice_id: int
    logical_slice_id: int
    source_mosaic_id: int | None
    logical_mosaic_id: int
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
        slice_offset: int,
        batch_size: int,
        scan_config: PSOCTScanConfigModel,
        direct: bool,
        force_resend: bool,
        refresh_hook: RefreshHook | None = None,
        min_complex_file_size_bytes: int = 1,
        prefer_spectral_for_complex_with_spectral: bool = True,
    ) -> None:
        self.project_name = project_name
        self.folder_path = folder_path
        self.project_base_path = project_base_path
        self.mosaic_ranges = mosaic_ranges
        self.slice_offset = slice_offset
        self.batch_size = batch_size
        self.scan_config = scan_config
        self.direct = direct
        self.force_resend = force_resend
        self.refresh_hook = refresh_hook
        self.min_complex_file_size_bytes = min_complex_file_size_bytes
        self.prefer_spectral_for_complex_with_spectral = (
            prefer_spectral_for_complex_with_spectral
        )

    def _selected_tile_saving_type(self) -> TileSavingType:
        saving_type = self.scan_config.acquisition.tile_saving_type
        if saving_type is TileSavingType.COMPLEX_WITH_SPECTRAL:
            return (
                TileSavingType.SPECTRAL
                if self.prefer_spectral_for_complex_with_spectral
                else TileSavingType.COMPLEX
            )
        return saving_type

    def discover_candidates(self) -> list[OCTBatchCandidate]:
        if not _is_readable_dir(self.folder_path):
            logger.warning(
                "OCT watch directory is not readable right now: %s", self.folder_path
            )
            return []

        if self.scan_config.mosaics_per_slice == 3:
            return self._discover_slice_candidates()

        return self._discover_two_mosaic_candidates()

    def _discover_two_mosaic_candidates(self) -> list[OCTBatchCandidate]:
        parsed_files = self._discover_two_mosaic_files()

        files_by_mosaic_and_image: dict[tuple[int, int], list[Path]] = defaultdict(list)
        for parsed in parsed_files:
            files_by_mosaic_and_image[
                (parsed.source_mosaic_id, parsed.image_index)
            ].append(parsed.path)

        files_by_source_mosaic: dict[int, list[Path]] = defaultdict(list)
        for (
            source_mosaic_id,
            _image_index,
        ), paths in files_by_mosaic_and_image.items():
            # spectral k does not matter; pick one representative per tile
            files_by_source_mosaic[source_mosaic_id].append(sorted(paths)[0])

        out: list[OCTBatchCandidate] = []

        for min_source_mosaic_id, max_source_mosaic_id in self.mosaic_ranges:
            for source_mosaic_id in range(
                min_source_mosaic_id, max_source_mosaic_id + 1
            ):
                mosaic_files = files_by_source_mosaic.get(source_mosaic_id)
                if not mosaic_files:
                    continue

                mosaic_files.sort(key=self._image_index_from_file)

                batches: dict[int, list[Path]] = defaultdict(list)
                for file_path in mosaic_files:
                    image_index = self._image_index_from_file(file_path)
                    logical_batch = (image_index - 1) // self.batch_size + 1
                    batches[logical_batch].append(file_path)

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
                        # transiently unreadable; let later polling iterations retry
                        continue

                    source_slice_id = slice_from_mosaic(
                        source_mosaic_id,
                        self.scan_config.mosaics_per_slice,
                    )
                    logical_slice_id, logical_mosaic_id = (
                        logical_mosaic_from_source_mosaic(
                            source_mosaic_id,
                            mosaics_per_slice=self.scan_config.mosaics_per_slice,
                            slice_offset=self.slice_offset,
                        )
                    )

                    out.append(
                        OCTBatchCandidate(
                            source_slice_id=source_slice_id,
                            logical_slice_id=logical_slice_id,
                            source_mosaic_id=source_mosaic_id,
                            logical_mosaic_id=logical_mosaic_id,
                            logical_batch=logical_batch,
                            files=candidate_files,
                        )
                    )

        return out

    def _discover_two_mosaic_files(self) -> list[ParsedTileFile]:
        saving_type = self._selected_tile_saving_type()
        out: list[ParsedTileFile] = []

        for path in self.folder_path.iterdir():
            if not path.is_file():
                continue
            if not _is_readable_file(path):
                continue

            parsed: ParsedTileFile | None = None

            if saving_type is TileSavingType.COMPLEX:
                parsed = self._parse_complex_file(path)
            elif saving_type is TileSavingType.SPECTRAL:
                parsed = self._parse_spectral_nii_file(path)
            elif saving_type is TileSavingType.SPECTRAL_12bit:
                parsed = self._parse_spectral_raw_file(path)

            if parsed is not None:
                out.append(parsed)

        return out

    def _discover_slice_candidates(self) -> list[OCTBatchCandidate]:
        files_by_source_slice: dict[int, list[Path]] = defaultdict(list)

        for path in self.folder_path.iterdir():
            if not path.is_file():
                continue
            if not _is_readable_file(path):
                continue

            parsed = self._parse_slice_file(path)
            if parsed is None:
                continue

            files_by_source_slice[parsed.source_slice_id].append(parsed.path)

        out: list[OCTBatchCandidate] = []
        for source_slice_id, files in sorted(files_by_source_slice.items()):
            candidate_files = tuple(sorted(files))
            if not _all_files_readable(candidate_files):
                continue

            logical_slice_id, logical_mosaic_id = (
                logical_first_mosaic_from_source_slice(
                    source_slice_id,
                    mosaics_per_slice=self.scan_config.mosaics_per_slice,
                    slice_offset=self.slice_offset,
                )
            )

            out.append(
                OCTBatchCandidate(
                    source_slice_id=source_slice_id,
                    logical_slice_id=logical_slice_id,
                    source_mosaic_id=None,
                    logical_mosaic_id=logical_mosaic_id,
                    logical_batch=1,
                    files=candidate_files,
                )
            )

        return out

    def _parse_complex_file(self, path: Path) -> ParsedTileFile | None:
        match = _COMPLEX_RE.match(path.name)
        if match is None:
            return None

        try:
            if path.stat().st_size < self.min_complex_file_size_bytes:
                return None
        except (OSError, PermissionError):
            return None

        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _parse_spectral_nii_file(self, path: Path) -> ParsedTileFile | None:
        match = _SPECTRAL_NII_RE.match(path.name)
        if match is None:
            return None
        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _parse_spectral_raw_file(self, path: Path) -> ParsedTileFile | None:
        match = _SPECTRAL_RAW_RE.match(path.name)
        if match is None:
            return None
        return ParsedTileFile(
            path=path,
            source_mosaic_id=int(match.group("mosaic")),
            image_index=int(match.group("image")),
        )

    def _parse_slice_file(self, path: Path) -> ParsedSliceFile | None:
        match = _SLICE_RE.match(path.name)
        if match is None:
            return None
        return ParsedSliceFile(
            path=path,
            source_slice_id=int(match.group("slice")),
        )

    def _image_index_from_file(self, path: Path) -> int:
        for parser in (
            self._parse_complex_file,
            self._parse_spectral_nii_file,
            self._parse_spectral_raw_file,
        ):
            parsed = parser(path)
            if parsed is not None:
                return parsed.image_index
        raise ValueError(f"Could not parse image index from {path.name!r}")

    def candidate_key(self, candidate: OCTBatchCandidate) -> tuple[int, int]:
        return (candidate.logical_mosaic_id, candidate.logical_batch)

    def fingerprint(self, candidate: OCTBatchCandidate) -> object:
        if not _all_files_readable(candidate.files):
            raise OSError("Candidate files are temporarily unreadable")
        return _batch_fingerprint(candidate.files, self.folder_path)

    def process(self, candidate: OCTBatchCandidate) -> int:
        if not _all_files_readable(candidate.files):
            logger.info(
                "Candidate not ready yet; files unreadable logical_mosaic=%s logical_batch=%s",
                candidate.logical_mosaic_id,
                candidate.logical_batch,
            )
            return 0

        batch_ident = oct_batch_ident(
            self.project_name,
            candidate.logical_mosaic_id,
            candidate.logical_batch,
        )

        existing = OCT_STATE_SERVICE.peek_batch(batch_ident=batch_ident)
        if existing is not None and not self.force_resend:
            logger.info("[SKIP] batch state exists for %s", batch_ident)
            return 0

        if self.direct:
            return self._process_direct(candidate, batch_ident)

        return self._process_event(candidate, batch_ident)

    def _process_direct(self, candidate: OCTBatchCandidate, batch_ident: Any) -> int:
        logger.info(
            "Direct-processing OCT source_slice=%s logical_slice=%s source_mosaic=%s "
            "logical_mosaic=%s logical_batch=%s files=%s",
            candidate.source_slice_id,
            candidate.logical_slice_id,
            candidate.source_mosaic_id,
            candidate.logical_mosaic_id,
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
            "Emitting BATCH_READY source_slice=%s logical_slice=%s source_mosaic=%s "
            "logical_mosaic=%s logical_batch=%s files=%s",
            candidate.source_slice_id,
            candidate.logical_slice_id,
            candidate.source_mosaic_id,
            candidate.logical_mosaic_id,
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
    slice_offset: int,
    batch_size: int,
    scan_config: PSOCTScanConfigModel,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = True,
    force_resend: bool = False,
    refresh_hook: RefreshHook | None = None,
    min_complex_file_size_bytes: int = 1,
) -> None:
    service = OCTWatcherService(
        project_name=project_name,
        folder_path=folder_path,
        project_base_path=project_base_path,
        mosaic_ranges=mosaic_ranges,
        slice_offset=slice_offset,
        batch_size=batch_size,
        scan_config=scan_config,
        direct=direct,
        force_resend=force_resend,
        refresh_hook=refresh_hook,
        min_complex_file_size_bytes=min_complex_file_size_bytes,
    )

    watcher = PollingStableWatcher[OCTBatchCandidate, tuple[int, int]](
        discover_candidates=service.discover_candidates,
        candidate_key=service.candidate_key,
        fingerprint=service.fingerprint,
        process=service.process,
        poll_interval=poll_interval,
        stability_seconds=stability_seconds,
        running_message=f"OCT polling watcher running ({'direct' if direct else 'event'} mode)",
    )
    watcher.run()


@oct_cli.command
def watch(
    project_name: str,
    folder_path: str,
    mosaic_ranges: str = "1:999999",
    *,
    slice_offset: int = 0,
    stability_seconds: int = 15,
    poll_interval: int = 5,
    direct: bool = True,
    refresh: str | None = None,
    force_resend: bool = False,
    min_complex_file_size_bytes: int = 1,
) -> None:
    """
    Poll for complete OCT batches and dispatch each stable candidate either by:
    - running process_tile_batch directly, or
    - emitting BATCH_READY.

    For mosaics_per_slice == 2:
      - grouping is mosaic/image-based
      - mosaic_ranges applies to source mosaic ids parsed from filenames

    For mosaics_per_slice == 3:
      - discovery is slice-based from slice_<n>* files
      - slice_offset controls logical slice numbering
      - logical mosaic id becomes the first mosaic of that logical slice
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

    logger.info("Using batch_size=%s", batch_size)
    logger.info("Using mosaics_per_slice=%s", scan_config.mosaics_per_slice)
    logger.info("Using tile_saving_type=%s", scan_config.acquisition.tile_saving_type)

    watch_oct(
        project_name=project_name,
        folder_path=watch_path,
        project_base_path=project_base_path,
        mosaic_ranges=_parse_mosaic_ranges(mosaic_ranges),
        slice_offset=slice_offset,
        batch_size=batch_size,
        scan_config=scan_config,
        stability_seconds=stability_seconds,
        poll_interval=poll_interval,
        direct=direct,
        force_resend=force_resend,
        refresh_hook=refresh_hook,
        min_complex_file_size_bytes=min_complex_file_size_bytes,
    )
