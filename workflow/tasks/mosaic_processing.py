"""
Tasks for mosaic processing including coordinate determination, 
template generation, and stitching.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import jinja2
import yaml
from linc_convert.modalities.psoct.mosaic import mosaic
from prefect import get_run_logger, task

from data_processing.stitch import fiji_stitch, fit_coord_files, generate_mask
from data_processing.stitch.process_tile_coord import process_tile_coord


@task(name="fiji_stitch_task")
def fiji_stitch_task(
    directory: str,
    file_template: str,
    grid_size_x: int,
    grid_size_y: int,
    tile_overlap: float = 20.0,
    first_file_index: int = 1,
    output_textfile_name: str = "TileConfiguration.txt",
) -> str:
    """
    Run Fiji stitching to generate TileConfiguration files.
    
    Parameters
    ----------
    directory : str
        Directory containing tile files
    file_template : str
        Template for tile filenames (e.g., "mosaic_013_image_{iiii}_aip.nii")
    grid_size_x : int
        Number of columns in grid
    grid_size_y : int
        Number of rows in grid
    tile_overlap : float
        Overlap between tiles in pixels
    first_file_index : int
        First file index (usually 0 or 1)
    output_textfile_name : str
        Name of output TileConfiguration file
        
    Returns
    -------
    str
        Path to generated TileConfiguration file
    """
    logger = get_run_logger()
    directory_path = Path(directory)
    output_file = directory_path / output_textfile_name

    logger.info(
        f"Running Fiji stitch for {file_template} "
        f"with grid {grid_size_x}x{grid_size_y}"
    )

    fiji_stitch.main(
        directory=directory_path,
        file_template=file_template,
        grid_size_x=grid_size_x,
        grid_size_y=grid_size_y,
        tile_overlap=tile_overlap,
        first_file_index=first_file_index,
        output_textfile_name=output_textfile_name,
    )

    logger.info(f"Generated TileConfiguration file: {output_file}")
    return str(output_file)


@task(name="process_tile_coord_task")
def process_tile_coord_task(
    ideal_coord_file: str,
    stitched_coord_file: str,
    image_dir: str,
    export: str,
    threshold: float = 55.0,
) -> str:
    """
    Process tile coordinates and export to YAML.
    
    Parameters
    ----------
    ideal_coord_file : str
        Path to ideal coordinate file (TileConfiguration.txt)
    stitched_coord_file : str
        Path to stitched coordinate file (TileConfiguration.registered.txt)
    image_dir : str
        Directory containing image files
    export : str
        Path to export YAML file
    threshold : float
        Signal threshold for reliable tiles
        
    Returns
    -------
    str
        Path to exported YAML file
    """
    logger = get_run_logger()
    logger.info(f"Processing tile coordinates from {ideal_coord_file}")

    grid, tiles = process_tile_coord(
        ideal_coord_file=ideal_coord_file,
        stitched_coord_file=stitched_coord_file,
        image_dir=image_dir,
        export=export,
        threshold=threshold,
        verbose=True,
    )

    logger.info(f"Exported tile coordinates to {export}")
    return export


@task(name="generate_coord_template_task")
def generate_coord_template_task(
    tile_coords_export_path: str,
    template_output_path: str,
    base_dir: str,
    scan_resolution: List[float],
    mosaic_id: int,
) -> str:
    """
    Generate a Jinja2 template from tile coordinates using fit_coord_files.
    This template can be reused for all modalities and mosaics of the same
    illumination type.
    
    Parameters
    ----------
    tile_coords_export_path : str
        Path to tile_coords_export.yaml file
    template_output_path : str
        Path to save Jinja2 template
    base_dir : str
        Base directory for tile files
    scan_resolution : List[float]
        Scan resolution [x, y] or [x, y, z]
    mosaic_id : int
        Base mosaic ID (1 for normal, 2 for tilted) used in template
        
    Returns
    -------
    str
        Path to generated template file
    """
    logger = get_run_logger()
    logger.info(f"Generating coordinate template from {tile_coords_export_path}")

    # Create a temporary output file first
    temp_output = Path(template_output_path).with_suffix('.temp.yaml')
    temp_output.parent.mkdir(parents=True, exist_ok=True)

    # Use fit_coord_files to generate the initial structure
    # We'll use placeholder values that we'll replace with Jinja2 template variables
    fit_coord_files.main(
        input=tile_coords_export_path,
        output=str(temp_output),
        base_dir=base_dir,
        scan_resolution=scan_resolution,
        replace=[
            f"aip:{{{{ modality }}}}",  # Replace aip with Jinja2 template variable
            f"mosaic_{mosaic_id:03d}:{{{{ mosaic_id_str }}}}",
            # Replace mosaic ID with template variable
        ],
    )

    # Load the generated YAML
    with open(temp_output, 'r') as f:
        template_yaml = f.read()

    # Convert to Jinja2 template by replacing hardcoded values with template variables
    # 1. Replace base_dir with Jinja2 variable
    template_yaml = template_yaml.replace(
        f"base_dir: {base_dir}",
        "base_dir: {{ base_dir }}"
    )

    # 2. Replace scan_resolution with Jinja2 variable
    scan_res_str = str(scan_resolution)
    template_yaml = template_yaml.replace(
        f"scan_resolution: {scan_res_str}",
        "{% if scan_resolution %}\n  scan_resolution: {{ scan_resolution }}\n{% endif "
        "%}"
    )

    # 3. Add mask as optional Jinja2 variable (insert after base_dir)
    if "mask:" not in template_yaml:
        template_yaml = template_yaml.replace(
            "base_dir: {{ base_dir }}",
            "base_dir: {{ base_dir }}\n{% if mask %}\n  mask: {{ mask }}\n{% endif %}"
        )

    # 4. The filepath replacements from fit_coord_files should already have the template variables
    # But ensure mosaic ID pattern is correct (handle any remaining instances)
    mosaic_pattern = f"mosaic_{mosaic_id:03d}"
    if mosaic_pattern in template_yaml and "{{{{ mosaic_id_str }}}}" not in template_yaml:
        template_yaml = re.sub(
            rf"\b{mosaic_pattern}\b",
            r"{{{{ mosaic_id_str }}}}",
            template_yaml
        )

    # Save as Jinja2 template
    template_path = Path(template_output_path)
    with open(template_path, 'w') as f:
        f.write(template_yaml)

    # Clean up temp file
    temp_output.unlink()

    logger.info(f"Generated template at {template_output_path}")

    return str(template_path)


@task(name="generate_tile_info_file_task")
def generate_tile_info_file_task(
    template_path: str,
    output_path: str,
    base_dir: str,
    modality: str,
    mosaic_id: int,
    mask: Optional[str] = None,
    scan_resolution: Optional[List[float]] = None,
) -> str:
    """
    Generate a tile_info_file for a specific modality using the Jinja2 template.
    
    The template contains Jinja2 variables that are replaced with actual values:
    - {{ modality }} -> target modality (e.g., "aip", "ret", "ori")
    - {{ mosaic_id_str }} -> mosaic ID string (e.g., "mosaic_001")
    - {{ base_dir }} -> base directory for tile files
    - {{ scan_resolution }} -> scan resolution array
    - {{ mask }} -> mask file path (optional)
    
    Parameters
    ----------
    template_path : str
        Path to Jinja2 template file (e.g., tile_info_normal.j2)
    tile_coords_export_path : str
        Path to tile_coords_export.yaml file (for loading tile coordinates)
    output_path : str
        Path to output tile_info_file
    base_dir : str
        Base directory for tile files (for current mosaic)
    modality : str
        Target modality (e.g., "aip", "ret", "ori", "biref", "mip", "surf", "dBI")
    mosaic_id : int
        Target mosaic ID (e.g., 1, 2, 3, ...)
    mask : str, optional
        Path to mask file
    scan_resolution : List[float], optional
        Scan resolution [x, y] or [x, y, z]
        
    Returns
    -------
    str
        Path to generated tile_info_file
    """
    logger = get_run_logger()
    logger.info(
        f"Generating tile_info_file for mosaic {mosaic_id}, modality {modality}")

    # Load template
    with open(template_path, 'r') as f:
        template_content = f.read()

    template = jinja2.Template(template_content)

    # Prepare template variables for Jinja2 rendering
    mosaic_id_str = f"mosaic_{mosaic_id:03d}"

    template_vars = {
        'modality': modality,
        'mosaic_id_str': mosaic_id_str,
        'base_dir': base_dir,
    }

    if scan_resolution is not None:
        template_vars['scan_resolution'] = scan_resolution

    if mask is not None:
        template_vars['mask'] = mask

    # Render template - Jinja2 will replace all {{ variable }} with actual values
    rendered = template.render(**template_vars)

    # Parse rendered YAML to validate and format properly
    rendered_data = yaml.safe_load(rendered)

    # Write output
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path_obj, 'w') as f:
        yaml.dump(rendered_data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Generated tile_info_file: {output_path}")
    return str(output_path)


@task(name="stitch_mosaic2d_task")
def stitch_mosaic2d_task(
    tile_info_file: str,
    nifti_output: str,
    jpeg_output: Optional[str] = None,
    tiff_output: Optional[str] = None,
    circular_mean: bool = False,
) -> Dict[str, str]:
    """
    Stitch mosaic using mosaic2d.
    
    Parameters
    ----------
    tile_info_file : str
        Path to tile_info_file YAML
    nifti_output : str
        Path to output NIfTI file
    jpeg_output : str, optional
        Path to output JPEG preview
    tiff_output : str, optional
        Path to output TIFF file
    circular_mean : bool
        Whether to use circular mean for orientation data
        
    Returns
    -------
    Dict[str, str]
        Dictionary with output file paths
    """
    logger = get_run_logger()
    logger.info(f"Stitching mosaic from {tile_info_file}")

    mosaic(
        tile_info_file=tile_info_file,
        nifti_output=nifti_output,
        jpeg_output=jpeg_output,
        tiff_output=tiff_output,
        circular_mean=circular_mean,
    )

    outputs = {'nifti': nifti_output}
    if jpeg_output:
        outputs['jpeg'] = jpeg_output
    if tiff_output:
        outputs['tiff'] = tiff_output

    logger.info(f"Stitched mosaic saved to {nifti_output}")
    return outputs


@task(name="generate_mask_task")
def generate_mask_task(
    input_image: str,
    output_mask: str,
    threshold: float = 50.0,
) -> str:
    """
    Generate mask from input image.
    
    Parameters
    ----------
    input_image : str
        Path to input image (e.g., AIP or MIP)
    output_mask : str
        Path to output mask file
    threshold : float
        Threshold for mask generation
        
    Returns
    -------
    str
        Path to generated mask file
    """
    logger = get_run_logger()
    logger.info(f"Generating mask from {input_image} with threshold {threshold}")

    generate_mask.main(
        input=input_image,
        output=output_mask,
        threshold=threshold,
    )

    logger.info(f"Generated mask: {output_mask}")
    return output_mask

