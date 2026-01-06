"""
Tasks for mosaic processing including coordinate determination,
template generation, and stitching.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import jinja2
import yaml
from linc_convert.modalities.psoct.mosaic import mosaic2d
from prefect import get_run_logger, task

from data_processing.stitch import fiji_stitch, fit_coord_files, generate_mask
from data_processing.stitch.process_tile_coord import process_tile_coord


@task
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
        f"Running Fiji stitch for {file_template} with grid {grid_size_x}x{grid_size_y}"
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


@task
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


@task
def generate_coord_template_task(
    tile_coords_export_path: str,
    template_output_path: str,
    base_dir: str,
    mosaic_id: int,
    scan_resolution: Optional[List[float]] = None,
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
    temp_output = Path(template_output_path).with_suffix(".temp.yaml")
    temp_output.parent.mkdir(parents=True, exist_ok=True)

    # Use fit_coord_files to generate the initial structure
    # We'll use placeholder values that we'll replace with Jinja2 template variables
    fit_coord_files.main(
        input=tile_coords_export_path,
        output=str(temp_output),
        base_dir=base_dir,
        scan_resolution=scan_resolution,
        replace=[
            "aip:{{ modality }}",  # Replace aip with Jinja2 template variable
            f"mosaic_{mosaic_id:03d}:{{{{ mosaic_id_str }}}}",
            # Replace mosaic ID with template variable
        ],
    )

    # Load the generated YAML
    with open(temp_output, "r") as f:
        template_yaml = f.read()

    # Convert to Jinja2 template by replacing hardcoded values with template variables
    # 1. Replace base_dir with Jinja2 variable
    template_yaml = template_yaml.replace(
        f"base_dir: {base_dir}", "base_dir: {{ base_dir }}"
    )

    # 2. Replace scan_resolution with Jinja2 variable
    scan_res_str = str(scan_resolution)
    template_yaml = template_yaml.replace(
        f"scan_resolution: {scan_res_str}",
        "{% if scan_resolution %}\n  scan_resolution: {{ scan_resolution }}\n{% endif "
        "%}",
    )

    # 3. Add mask as optional Jinja2 variable (insert after base_dir)
    if "mask:" not in template_yaml:
        template_yaml = template_yaml.replace(
            "base_dir: {{ base_dir }}",
            "base_dir: {{ base_dir }}\n{% if mask %}\n  mask: {{ mask }}\n{% endif %}",
        )

    # 4. The filepath replacements from fit_coord_files should already have the template variables
    # But ensure mosaic ID pattern is correct (handle any remaining instances)
    mosaic_pattern = f"mosaic_{mosaic_id:03d}"
    if (
        mosaic_pattern in template_yaml
        and "{{{{ mosaic_id_str }}}}" not in template_yaml
    ):
        template_yaml = re.sub(
            rf"\b{mosaic_pattern}\b", r"{{{{ mosaic_id_str }}}}", template_yaml
        )

    # Save as Jinja2 template
    template_path = Path(template_output_path)
    with open(template_path, "w") as f:
        f.write(template_yaml)

    # Clean up temp file
    temp_output.unlink()

    logger.info(f"Generated template at {template_output_path}")

    return str(template_path)


@task(
    task_run_name="mosaic-{mosaic_id}-generate-tile-info-{modality}"
)
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
        f"Generating tile_info_file for mosaic {mosaic_id}, modality {modality}"
    )

    # Load template
    with open(template_path, "r") as f:
        template_content = f.read()

    template = jinja2.Template(template_content)

    # Prepare template variables for Jinja2 rendering
    mosaic_id_str = f"mosaic_{mosaic_id:03d}"

    template_vars = {
        "modality": modality,
        "mosaic_id_str": mosaic_id_str,
        "base_dir": base_dir,
    }

    if scan_resolution is not None:
        template_vars["scan_resolution"] = scan_resolution

    if mask is not None:
        template_vars["mask"] = mask

    # Render template - Jinja2 will replace all {{ variable }} with actual values
    rendered = template.render(**template_vars)

    # Parse rendered YAML to validate and format properly
    rendered_data = yaml.safe_load(rendered)

    # Write output
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path_obj, "w") as f:
        yaml.dump(rendered_data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Generated tile_info_file: {output_path}")
    return str(output_path)


@task
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

    mosaic2d(
        tile_info_file=tile_info_file,
        nifti_output=nifti_output,
        jpeg_output=jpeg_output,
        tiff_output=tiff_output,
        circular_mean=circular_mean,
    )

    outputs = {"nifti": nifti_output}
    if jpeg_output:
        outputs["jpeg"] = jpeg_output
    if tiff_output:
        outputs["tiff"] = tiff_output

    logger.info(f"Stitched mosaic saved to {nifti_output}")
    return outputs


@task
def generate_mask_task(
    input_image: str,
    output_mask: str,
    threshold: float = 60.0,
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
    logger.info(f"Input image: {input_image}, {Path(input_image).suffix}")
    generate_mask.main(
        input=input_image,
        output=output_mask,
        threshold=threshold,
    )

    logger.info(f"Generated mask: {output_mask}")
    return output_mask


@task
def stitch_mosaic3d_task(
    tile_info_file: str,
    output_path: str,
    zarr_config=None,
    kwargs: Dict[str, Any] = None,
) -> str:
    """
    Stitch mosaic using mosaic3d.

    Parameters
    ----------
    tile_info_file: str
        Path to tile_info_file YAML
    output_path: str
        Path to output zarr file
    zarr_config: ZarrConfig, optional
        Zarr configuration object
    kwargs: Dict[str, Any], optional
        Additional keyword arguments for mosaic3d
    """
    from linc_convert.utils.zarr_config import ZarrConfig

    logger = get_run_logger()
    logger.info(f"Stitching mosaic 3D from {tile_info_file}")

    # Prepare arguments for mosaic2d
    mosaic_kwargs = (kwargs or {}).copy()
    if zarr_config is not None:
        mosaic_kwargs["zarr_config"] = zarr_config

    mosaic2d(tile_info_file=tile_info_file, out=output_path, **mosaic_kwargs)
    logger.info(f"Stitched mosaic 3D saved to {output_path}")
    return output_path


@task(
    task_run_name="{project_name}-mosaic-{mosaic_id}-symlink-enface-to-dandi"
)
def symlink_enface_to_dandi_task(
    enface_outputs: Dict[str, Dict[str, str]],
    dandi_slice_path: Path,
    project_name: str,
    mosaic_id: int,
    mosaic_enface_format: str,
) -> Dict[str, Path]:
    """
    Create symlinks from stitched enface nifti files to DANDI slice directory.

    Uses mosaic_enface_format template to generate target filenames.
    Target directory: {dandiset_path}/sample-slice{slice_id:03d}/

    Parameters
    ----------
    enface_outputs : Dict[str, Dict[str, str]]
        Dictionary mapping modality to output file paths (with 'nifti' key)
    dandi_slice_path : Path
        Path to DANDI slice directory
    project_name : str
        Project identifier
    mosaic_id : int
        Mosaic identifier
    mosaic_enface_format : str
        Template string for enface filename format

    Returns
    -------
    Dict[str, Path]
        Dictionary mapping modality to symlink target path
    """
    from workflow.tasks.utils import mosaic_id_to_slice_number

    logger = get_run_logger()
    logger.info(f"Creating symlinks for enface files to DANDI directory: {dandi_slice_path}")

    # Ensure DANDI slice directory exists
    dandi_slice_path.mkdir(parents=True, exist_ok=True)

    slice_number = mosaic_id_to_slice_number(mosaic_id)
    acq = "tilted" if mosaic_id % 2 == 0 else "normal"
    symlink_targets = {}

    for modality, outputs in enface_outputs.items():
        if not isinstance(outputs, dict) or "nifti" not in outputs:
            logger.warning(f"Skipping modality {modality}: missing 'nifti' key in outputs")
            continue

        source_path = Path(outputs["nifti"])
        if not source_path.exists():
            logger.warning(f"Skipping modality {modality}: source file does not exist: {source_path}")
            continue

        # Generate target filename using template
        try:
            target_filename = mosaic_enface_format.format(
                project_name=project_name,
                slice_id=slice_number,
                acq=acq,
                modality=modality,
            )
        except KeyError as e:
            logger.error(f"Error formatting template for modality {modality}: {e}")
            continue

        target_path = dandi_slice_path / target_filename

        # Create symlink
        try:
            if target_path.exists() or target_path.is_symlink():
                if target_path.is_symlink():
                    target_path.unlink()
                else:
                    logger.warning(f"Target already exists (not a symlink), skipping: {target_path}")
                    continue

            target_path.symlink_to(source_path)
            symlink_targets[modality] = target_path
            logger.info(f"Created symlink: {target_path} -> {source_path}")
        except OSError as e:
            logger.error(f"Failed to create symlink for {modality}: {e}")
            continue

    logger.info(f"Created {len(symlink_targets)} symlinks for enface modalities")
    return symlink_targets


@task(
    task_run_name="mosaic-{mosaic_id}-find-focus-plane-{illumination}"
)
def find_focus_plane_task(
    project_base_path: str,
    mosaic_id: int,
    stitched_surface_path: str,
    illumination: str,
) -> str:
    """
    Find optimal focus plane for 3D volume stitching (Section 3.3).

    Per design document Section 3.3, focus finding:
    - Determines optimal focus plane for 3D volume stitching
    - Uses unfiltered surface data
    - Generates QC validation: verify focus finding overlap with intensity images
    - Output saved as focus-{illumination}.nii in project base path

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier (first slice: 1 for normal, 2 for tilted)
    stitched_surface_path : str
        Path to stitched surface map (unfiltered version)
    illumination : str
        Illumination type ("normal" or "tilted")

    Returns
    -------
    str
        Path to generated focus plane file (focus-{illumination}.nii)

    Notes
    -----
    Detailed algorithms are described in Section 15 of design document.
    This is a placeholder implementation that can be expanded with actual
    focus finding algorithms.
    """
    logger = get_run_logger()
    logger.info(
        f"Finding focus plane for {illumination} illumination (mosaic {mosaic_id})"
    )

    # Output path per Section 4.1: {project_base_path}/focus-{illumination}.nii
    focus_output_path = Path(project_base_path) / f"focus-{illumination}.nii"
    focus_output_path.parent.mkdir(parents=True, exist_ok=True)

    # TODO: Implement actual focus finding algorithm (Section 15)
    # For now, this is a placeholder that:
    # 1. Loads the stitched surface map
    # 2. Processes it to determine optimal focus plane
    # 3. Saves the focus plane as a NIfTI file

    # Placeholder: Copy surface map as focus plane (to be replaced with actual algorithm)
    import nibabel as nib

    try:
        # Load stitched surface
        surface_img = nib.load(stitched_surface_path)
        surface_data = surface_img.get_fdata()

        # TODO: Apply focus finding algorithm here
        # For now, use surface data directly (this should be replaced with actual algorithm)
        focus_data = surface_data.copy()

        # Save focus plane
        focus_img = nib.Nifti1Image(focus_data, surface_img.affine, surface_img.header)
        nib.save(focus_img, str(focus_output_path))

        logger.info(f"Focus plane saved to {focus_output_path}")
        logger.warning(
            "Focus finding using placeholder implementation. "
            "Actual algorithm from Section 15 should be implemented."
        )

    except Exception as e:
        logger.error(f"Error in focus finding: {e}")
        raise

    return str(focus_output_path)
