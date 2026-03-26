"""
PSOCT coordinate determination subflow for mosaic stitching.

This module contains the tasks + subflow that compute stitching coordinates and
generate the Jinja2 tile-info template used by mosaic stitching flows.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

import jinja2
import yaml
from prefect import flow, task
from prefect.logging import get_run_logger

from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.data_processing.stitch import fiji, fit_coord_files
from opticstream.data_processing.stitch.coords import (
    process_tile_coordinate,
)
from opticstream.flows.psoct.utils import (
    MosaicContext,
    get_mosaic_fiji_file_template,
    get_mosaic_tile_coords_export_path,
    get_slice_paths,
)
from opticstream.state.oct_project_state import OCTMosaicId


@task(task_run_name="generate-tile-info-{modality}")
def generate_tile_info_file(
    template_path: Path,
    output_path: Path,
    base_dir: Path,
    modality: str,
    mosaic_id: int,
    mask: Optional[Path] = None,
    scan_resolution: Optional[List[float]] = None,
) -> None:
    """
    Generate a tile_info_file for a specific modality using the Jinja2 template.
    """
    logger = get_run_logger()
    logger.info(
        f"Generating tile_info_file for mosaic {mosaic_id}, modality {modality}"
    )

    with open(template_path, "r") as f:
        template = jinja2.Template(f.read())

    template_vars = {
        "modality": modality,
        "mosaic_id_str": f"mosaic_{mosaic_id:03d}",
        "base_dir": str(base_dir),
    }

    if scan_resolution is not None:
        template_vars["scan_resolution"] = scan_resolution

    if mask is not None:
        template_vars["mask"] = str(mask)

    rendered = template.render(**template_vars)
    rendered_data = yaml.safe_load(rendered)

    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path_obj, "w") as f:
        yaml.dump(rendered_data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Generated tile_info_file: {output_path_obj}")
    return None


@task
def fiji_stitch_task(
    directory: Path,
    file_template: str,
    grid_size_x: int,
    grid_size_y: int,
    tile_overlap: float = 20.0,
    first_file_index: int = 1,
    output_textfile_name: str = "TileConfiguration.txt",
) -> Path:
    """
    Run Fiji stitching to generate TileConfiguration files.

    Returns the path to the generated `TileConfiguration*.txt`.
    """
    logger = get_run_logger()

    logger.info(
        f"Running Fiji stitch for {file_template} with grid {grid_size_x}x{grid_size_y}"
    )

    fiji.main(
        directory=directory,
        file_template=file_template,
        grid_size_x=grid_size_x,
        grid_size_y=grid_size_y,
        tile_overlap=tile_overlap,
        first_file_index=first_file_index,
        output_textfile_name=output_textfile_name,
    )

    output_path = directory / output_textfile_name
    logger.info(f"Generated TileConfiguration file: {output_path}")
    return output_path


@task
def process_tile_coordinate_task(
    ideal_coord_file: Path,
    stitched_coord_file: Path,
    image_dir: Path,
    export: Path,
    threshold: float = 55.0,
) -> None:
    """Process tile coordinates and export to YAML."""
    logger = get_run_logger()
    logger.info(f"Processing tile coordinates from {ideal_coord_file}")

    process_tile_coordinate(
        ideal_coord_file=ideal_coord_file,
        stitched_coord_file=stitched_coord_file,
        image_dir=image_dir,
        export=export,
        threshold=threshold,
        verbose=True,
    )

    logger.info(f"Exported tile coordinates to {export}")
    return None


@task
def generate_coordinate_template(
    tile_coords_export_path: Path,
    template_output_path: Path,
    base_dir: Path,
    mosaic_id: int,
    scan_resolution: Optional[List[float]] = None,
) -> None:
    """
    Generate a Jinja2 template from tile coordinates using `fit_coord_files`.

    The output template is reused across mosaics of the same illumination type.
    """
    logger = get_run_logger()
    logger.info(f"Generating coordinate template from {tile_coords_export_path}")

    temp_output = Path(template_output_path).with_suffix(".temp.yaml")
    temp_output.parent.mkdir(parents=True, exist_ok=True)

    fit_coord_files.main(
        input=tile_coords_export_path,
        output=temp_output,
        base_dir=base_dir,
        scan_resolution=scan_resolution,
        replace=[
            "aip:{{ modality }}",
            "mip:{{ modality }}",
            f"mosaic_{mosaic_id:03d}:{{{{ mosaic_id_str }}}}",
        ],
    )

    with open(temp_output, "r") as f:
        template_yaml = f.read()

    template_yaml = template_yaml.replace(
        f"base_dir: {base_dir}", "base_dir: {{ base_dir }}"
    )

    template_yaml = template_yaml.replace(
        f"scan_resolution: {scan_resolution}",
        "{% if scan_resolution %}\n  scan_resolution: {{ scan_resolution }}\n{% endif %}",
    )

    if "mask:" not in template_yaml:
        template_yaml = template_yaml.replace(
            "base_dir: {{ base_dir }}",
            "base_dir: {{ base_dir }}\n{% if mask %}\n  mask: {{ mask }}\n{% endif %}",
        )

    mosaic_pattern = f"mosaic_{mosaic_id:03d}"
    if (
        mosaic_pattern in template_yaml
        and "{{{{ mosaic_id_str }}}}" not in template_yaml
    ):
        template_yaml = re.sub(
            rf"\b{mosaic_pattern}\b", r"{{{{ mosaic_id_str }}}}", template_yaml
        )

    template_path = Path(template_output_path)
    with open(template_path, "w") as f:
        f.write(template_yaml)

    temp_output.unlink()

    logger.info(f"Generated template at {template_path}")
    return None


@flow(flow_run_name="process-coords-{mosaic_ident}")
def process_stitching_coordinates(
    mosaic_ident: OCTMosaicId,
    config: PSOCTScanConfigModel,
    ctx: MosaicContext,
) -> tuple[Path, Path]:
    """
    Determine stitching coordinates and generate the coordinate template.

    Returns (template_path, tile_coords_export_path).
    """
    logger = get_run_logger()
    mosaic_id = mosaic_ident.mosaic_id
    illumination = ctx.config_illumination
    _, base_processed_path, base_stitched_path, _ = get_slice_paths(
        str(config.project_base_path), mosaic_ident.slice_id
    )
    mask_threshold = ctx.mask_threshold(config)

    logger.info(
        f"Processing coordinate determination for {illumination} illumination "
        f"(mosaic {mosaic_id}) with mask_threshold={mask_threshold}"
    )

    file_template = get_mosaic_fiji_file_template(mosaic_id)
    output_textfile_name = f"TileConfiguration_{illumination}.txt"

    ideal_coord_file = fiji_stitch_task(
        directory=base_processed_path,
        file_template=file_template,
        grid_size_x=ctx.grid_size_x(config),
        grid_size_y=ctx.grid_size_y(config),
        tile_overlap=config.acquisition.tile_overlap,
        output_textfile_name=output_textfile_name,
    )

    stitched_coord_file = base_processed_path / output_textfile_name.replace(
        ".txt", ".registered.txt"
    )

    if not stitched_coord_file.exists():
        raise FileNotFoundError(
            f"Expected registered coordinate file not found: {stitched_coord_file}"
        )

    tile_coords_export = get_mosaic_tile_coords_export_path(
        base_stitched_path, mosaic_id
    )

    process_tile_coordinate_task(
        ideal_coord_file=ideal_coord_file,
        stitched_coord_file=stitched_coord_file,
        image_dir=base_processed_path,
        export=tile_coords_export,
        threshold=mask_threshold,
    )

    template_path = Path(config.project_base_path) / ctx.template_name

    generate_coordinate_template(
        tile_coords_export_path=tile_coords_export,
        template_output_path=template_path,
        base_dir=base_processed_path,
        mosaic_id=mosaic_id,
    )

    logger.info(
        f"Generated template for {illumination} illumination at {template_path}"
    )
    return template_path, tile_coords_export
