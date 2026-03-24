from __future__ import annotations

from pathlib import Path

from prefect import get_run_logger, task

from opticstream.config.pipeline_opts_builder import build_pipeline_opts
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.flows.psoct.utils import get_slice_paths, mosaic_context_from_ids
from opticstream.state.oct_project_state import OCTBatchId
from opticstream.utils.matlab_execution import (
    dict_to_matlab_literal,
    run_matlab_batch_command,
)


def _matlab_quote(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


@task(task_run_name="spectral-to-processed-{batch_id}")
def tile_batch(
    batch_id: OCTBatchId,
    file_list: list[Path],
    *,
    config: PSOCTScanConfigModel,
) -> None:
    logger = get_run_logger()
    processed_path, _, _ = get_slice_paths(str(config.project_base_path), batch_id.slice_id)
    processed_path.mkdir(parents=True, exist_ok=True)

    mosaic_context = mosaic_context_from_ids(
        slice_id=batch_id.slice_id,
        mosaic_id=batch_id.mosaic_id,
        mosaics_per_slice=config.mosaics_per_slice,
    )
    pipeline_opts = build_pipeline_opts(
        config=config,
        illumination=mosaic_context.config_illumination,
    )

    spectral_opts = (
        pipeline_opts.spectral.to_matlab_struct() if pipeline_opts.spectral is not None else {}
    )
    surface_opts = (
        pipeline_opts.surface.to_matlab_struct() if pipeline_opts.surface is not None else {}
    )
    enface_opts = (
        pipeline_opts.enface.to_matlab_struct() if pipeline_opts.enface is not None else {}
    )
    acquisition_opts = (
        pipeline_opts.acquisition.to_matlab_struct()
        if pipeline_opts.acquisition is not None
        else {}
    )
    output_opts = (
        pipeline_opts.output.to_matlab_struct() if pipeline_opts.output is not None else {}
    )
    volume_opts = (
        pipeline_opts.volume.to_matlab_struct() if pipeline_opts.volume is not None else {}
    )

    file_list_str = ",".join(_matlab_quote(str(file_path)) for file_path in file_list)
    output_dir_lit = _matlab_quote(str(processed_path))
    opts_mat_file_lit = "''" if not pipeline_opts.opts_mat_file else _matlab_quote(pipeline_opts.opts_mat_file)
    num_workers_lit = (
        "[]"
        if config.processing.matlab_num_workers is None
        else str(config.processing.matlab_num_workers)
    )
    pool_type_lit = _matlab_quote(config.processing.matlab_pool_type)

    cmd = (
        f"psoct.file.spectral2processed_batch("
        f"{{{file_list_str}}}, "
        f"{output_dir_lit}, "
        f"{dict_to_matlab_literal(spectral_opts)}, "
        f"{dict_to_matlab_literal(surface_opts)}, "
        f"{dict_to_matlab_literal(enface_opts)}, "
        f"{dict_to_matlab_literal(acquisition_opts)}, "
        f"{dict_to_matlab_literal(output_opts)}, "
        f"{dict_to_matlab_literal(volume_opts)}, "
        f"{opts_mat_file_lit}, "
        f"{num_workers_lit}, "
        f"{pool_type_lit})"
    )

    logger.info("Running MATLAB command: %s", cmd)
    run_matlab_batch_command(cmd, matlab_script_path=str(config.matlab_root) if config.matlab_root else None)