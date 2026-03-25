"""
MATLAB command construction for one-phase tile batch (spectral or complex → processed).
"""

from __future__ import annotations

from pathlib import Path

from psoct_toolbox.opts_models import PipelineOpts

from opticstream.config.pipeline_opts_builder import build_pipeline_opts
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.flows.psoct.tile_file_reference import TileFileReference
from opticstream.flows.psoct.utils import MosaicContext, get_slice_paths
from opticstream.state.oct_project_state import OCTBatchId
from opticstream.utils.matlab_execution import dict_to_matlab_literal


def _matlab_quote(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _mstruct(field: object | None) -> str:
    """PipelineOpts sub-struct → MATLAB ``struct(...)`` literal."""
    return dict_to_matlab_literal(
        field.to_matlab_struct() if field is not None else {}
    )


def _batch_preamble(
    batch_id: OCTBatchId,
    file_reference_list: list[TileFileReference],
    config: PSOCTScanConfigModel,
    mosaic_context: MosaicContext,
) -> tuple[Path, PipelineOpts, str, str, str, str, str, str]:
    """
    Shared: processed dir, ``PipelineOpts``, cell path list, MATLAB tile index vector literal,
    output dir literal, opts mat file, num workers, pool type.
    """
    _, processed_path, _, _ = get_slice_paths(str(config.project_base_path), batch_id.slice_id)
    processed_path.mkdir(parents=True, exist_ok=True)

    pipeline_opts = build_pipeline_opts(
        config=config,
        illumination=mosaic_context.config_illumination,
    )

    file_list_str = ",".join(_matlab_quote(str(ref.file_path)) for ref in file_reference_list)
    tile_indices_lit = "[" + ",".join(str(ref.tile_number) for ref in file_reference_list) + "]"
    output_dir_lit = _matlab_quote(str(processed_path))
    opts_mat_file_lit = (
        "''" if not pipeline_opts.opts_mat_file else _matlab_quote(pipeline_opts.opts_mat_file)
    )
    num_workers_lit = (
        "[]"
        if config.processing.matlab_num_workers is None
        else str(config.processing.matlab_num_workers)
    )
    pool_type_lit = _matlab_quote(config.processing.matlab_pool_type)

    return (
        processed_path,
        pipeline_opts,
        file_list_str,
        tile_indices_lit,
        output_dir_lit,
        opts_mat_file_lit,
        num_workers_lit,
        pool_type_lit,
    )


def build_spectral_to_processed_command(
    batch_id: OCTBatchId,
    file_reference_list: list[TileFileReference],
    *,
    config: PSOCTScanConfigModel,
    mosaic_context: MosaicContext,
) -> tuple[Path, str]:
    """Return ``(processed_path, matlab_command)`` for ``psoct.file.spectral2processed_batch_indexed``."""
    (
        processed_path,
        pipeline_opts,
        file_list_str,
        tile_indices_lit,
        output_dir_lit,
        opts_mat_file_lit,
        num_workers_lit,
        pool_type_lit,
    ) = _batch_preamble(batch_id, file_reference_list, config, mosaic_context)

    mid = ", ".join(
        [
            _mstruct(pipeline_opts.spectral),
            _mstruct(pipeline_opts.surface),
            _mstruct(pipeline_opts.enface),
            _mstruct(pipeline_opts.acquisition),
            _mstruct(pipeline_opts.output),
            _mstruct(pipeline_opts.volume),
        ]
    )
    mosaic_id_lit = str(batch_id.mosaic_id)
    cmd = (
        f"psoct.file.spectral2processed_batch_indexed("
        f"{{{file_list_str}}}, {output_dir_lit}, {mosaic_id_lit}, {tile_indices_lit}, {mid}, "
        f"{opts_mat_file_lit}, {num_workers_lit}, {pool_type_lit})"
    )
    return processed_path, cmd


def build_complex_to_processed_command(
    batch_id: OCTBatchId,
    file_reference_list: list[TileFileReference],
    *,
    config: PSOCTScanConfigModel,
    mosaic_context: MosaicContext,
) -> tuple[Path, str]:
    """Return ``(processed_path, matlab_command)`` for ``psoct.file.complex2processed_batch_indexed``."""
    (
        processed_path,
        pipeline_opts,
        file_list_str,
        tile_indices_lit,
        output_dir_lit,
        opts_mat_file_lit,
        num_workers_lit,
        pool_type_lit,
    ) = _batch_preamble(batch_id, file_reference_list, config, mosaic_context)

    mid = ", ".join(
        [
            _mstruct(pipeline_opts.surface),
            _mstruct(pipeline_opts.enface),
            _mstruct(pipeline_opts.acquisition),
            _mstruct(pipeline_opts.output),
            _mstruct(pipeline_opts.volume),
        ]
    )
    mosaic_id_lit = str(batch_id.mosaic_id)
    cmd = (
        f"psoct.file.complex2processed_batch_indexed("
        f"{{{file_list_str}}}, {output_dir_lit}, {mosaic_id_lit}, {tile_indices_lit}, {mid}, "
        f"{opts_mat_file_lit}, {num_workers_lit}, {pool_type_lit})"
    )
    return processed_path, cmd
