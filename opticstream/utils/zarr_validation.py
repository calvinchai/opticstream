"""
Zarr store validation: existence, non-empty tree, and minimum on-disk size.

Shared by LSM strip/channel flows and PSOCT mosaic volume stitching.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class DirManifest:
    file_count: int
    total_bytes: int
    sizes: Dict[str, int]


@dataclass
class ValidationResult:
    ok: bool
    size_bytes: int
    reason: Optional[str] = None


def get_dir_manifest(path: str) -> DirManifest:
    """
    Compute total size, file count, and relative-path -> size mapping
    in a single walk.
    """
    total_bytes = 0
    file_count = 0
    sizes: Dict[str, int] = {}
    root = Path(path)

    for p in root.rglob("*"):
        if p.is_file():
            st = p.stat()
            rel = str(p.relative_to(root))
            sizes[rel] = st.st_size
            total_bytes += st.st_size
            file_count += 1

    return DirManifest(
        file_count=file_count,
        total_bytes=total_bytes,
        sizes=sizes,
    )


def compare_dir_manifests(
    source_manifest: DirManifest,
    dest_manifest: DirManifest,
    logger=None,
) -> bool:
    """Return True if both manifests have identical file sizes."""
    if source_manifest.sizes == dest_manifest.sizes:
        return True

    if logger:
        missing = source_manifest.sizes.keys() - dest_manifest.sizes.keys()
        extra = dest_manifest.sizes.keys() - source_manifest.sizes.keys()
        mismatched = {
            k
            for k in (source_manifest.sizes.keys() & dest_manifest.sizes.keys())
            if source_manifest.sizes[k] != dest_manifest.sizes[k]
        }
        if missing:
            logger.error(f"Missing files in destination: {sorted(list(missing))[:10]}")
        if extra:
            logger.error(f"Extra files in destination: {sorted(list(extra))[:10]}")
        if mismatched:
            logger.error(f"Size mismatches: {sorted(list(mismatched))[:10]}")

    return False


def validate_zarr_directory(
    logger,
    path: str,
    zarr_size_threshold: int,
    *,
    context: str,
    missing_reason: str,
    empty_reason: str,
    below_threshold_reason: str,
) -> ValidationResult:
    """
    Validate a zarr tree: exists, optional min file count when threshold<=0, else min total bytes.
    """
    if not os.path.exists(path):
        logger.error(f"Zarr path does not exist for {context}: {path}")
        return ValidationResult(ok=False, size_bytes=0, reason=missing_reason)
    manifest = get_dir_manifest(path)
    if zarr_size_threshold <= 0:
        if manifest.file_count < 1:
            logger.error(f"Zarr directory empty for {context}: {path}")
            return ValidationResult(ok=False, size_bytes=0, reason=empty_reason)
        logger.info(f"Zarr valid for {context} ({manifest.total_bytes} bytes)")
        return ValidationResult(ok=True, size_bytes=manifest.total_bytes)
    if manifest.total_bytes < zarr_size_threshold:
        logger.error(f"Zarr below size threshold for {context}: {path}")
        return ValidationResult(
            ok=False,
            size_bytes=manifest.total_bytes,
            reason=below_threshold_reason,
        )
    logger.info(f"Zarr valid for {context} ({manifest.total_bytes} bytes)")
    return ValidationResult(ok=True, size_bytes=manifest.total_bytes)


def validate_zarr(
    path: str,
    min_size_bytes: int,
    *,
    context: str = "zarr output",
    logger: Optional[Any] = None,
) -> ValidationResult:
    """
    Validate a zarr directory using the same rules as :func:`validate_zarr_directory`.

    Parameters
    ----------
    path
        Filesystem path to the zarr store (directory tree).
    min_size_bytes
        Minimum total size of all files under ``path``. Use ``<= 0`` to only require
        at least one file.
    context
        Short label for log messages and failure reasons.
    logger
        Logger with ``error`` and ``info``. Defaults to Prefect's run logger.

    Returns
    -------
    ValidationResult
        ``ok``, ``size_bytes`` (total bytes under the tree), and ``reason`` if not ok.
    """
    if logger is None:
        from prefect.logging import get_run_logger

        logger = get_run_logger()

    return validate_zarr_directory(
        logger,
        path,
        min_size_bytes,
        context=context,
        missing_reason=f"{context}: zarr path does not exist",
        empty_reason=f"{context}: zarr tree has no files",
        below_threshold_reason=(
            f"{context}: zarr total size below minimum ({min_size_bytes} bytes)"
        ),
    )
