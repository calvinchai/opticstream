"""
Validate processed outputs from spectral2processed_batch_indexed / complex2processed_batch_indexed.

Expected stems match ``+psoct/+file/+internal/processedModalities.m`` postfixes.
Prefix is ``processed_output_prefix(mosaic_id, ref.tile_number)``, aligned with MATLAB
``psoct.file.internal.buildProcessedOutputPrefix`` (not inferred from input basenames).

Which modalities to require follows ``PSOCTScanConfigModel.volume_modalities`` and
``enface_modalities`` (same basis as ``build_pipeline_opts`` EnfaceComputeFlags).
"""

from __future__ import annotations

from pathlib import Path

from opticstream.config.psoct_scan_config import (
    PSOCTScanConfigModel,
    VolumeModality,
)
from opticstream.flows.psoct.tile_file_reference import TileFileReference
from opticstream.flows.psoct.utils import processed_output_prefix

# VolumeModality -> NIfTI postfix (MATLAB ``OutputOpts.Paths`` keys)
_VOLUME_STEM_BY_MODALITY: dict[VolumeModality, str] = {
    VolumeModality.DBI: "dBI3D",
    VolumeModality.R3D: "R3D",
    VolumeModality.O3D: "O3D",
}

# Per-tile batch writes in processedModalities order; ``mus`` is not among those stems today.
_PER_TILE_ENFACE_SKIP = frozenset({"mus"})


def expected_processed_nifti_suffixes(config: PSOCTScanConfigModel) -> tuple[str, ...]:
    """
    Ordered, de-duplicated list of basename suffixes (e.g. ``dBI3D``, ``aip``) to validate.

    Mirrors ``build_pipeline_opts``: volumes from ``volume_modalities``; enface from
    ``enface_modalities`` (excluding postfixes not emitted by the MATLAB batch wrappers).
    """
    keys: list[str] = []
    for vm in config.volume_modalities:
        keys.append(_VOLUME_STEM_BY_MODALITY[vm])
    for em in config.enface_modalities:
        if em.value in _PER_TILE_ENFACE_SKIP:
            continue
        keys.append(em.value)
    seen: set[str] = set()
    ordered: list[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            ordered.append(k)
    return tuple(ordered)


def validate_output_files_exist_and_min_size(
    outputs: list[tuple[str, Path]],
    *,
    min_file_size_bytes: int,
    context: str,
) -> None:
    """
    Validate that each output file exists and is at least `min_file_size_bytes`.

    Raises `RuntimeError` with a summarized list of issues.
    """
    issues: list[str] = []
    for label, out_path in outputs:
        out_path = out_path.resolve()
        if not out_path.is_file():
            issues.append(f"missing output ({label}): {out_path}")
            continue
        size = out_path.stat().st_size
        if size < min_file_size_bytes:
            issues.append(f"output too small ({label}): {out_path} ({size} B)")

    if issues:
        raise RuntimeError(
            f"{context} validation failed for {len(issues)} issue(s):\n" + "\n".join(issues)
        )


def validate_processed_batch_outputs(
    processed_dir: Path,
    file_reference_list: list[TileFileReference],
    *,
    mosaic_id: int,
    config: PSOCTScanConfigModel,
    min_file_size_bytes: int = 50 * 1024 * 1024,
) -> None:
    """
    Ensure each tile has the configured processed NIfTIs under ``processed_dir``.

    Raises ``RuntimeError`` with a summary if any expected file is missing or too small.
    """
    processed_dir = processed_dir.resolve()
    output_suffixes = expected_processed_nifti_suffixes(config)

    outputs: list[tuple[str, Path]] = []
    for ref in file_reference_list:
        prefix = processed_output_prefix(mosaic_id, ref.tile_number)

        for key in output_suffixes:
            out_path = processed_dir / f"{prefix}_{key}.nii"
            outputs.append((f"tile_id={ref.tile_number}, key={key}", out_path))

    validate_output_files_exist_and_min_size(
        outputs,
        min_file_size_bytes=min_file_size_bytes,
        context="Processed tile outputs",
    )
