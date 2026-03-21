"""
PSOCT event-driven mosaic processing flow.

Triggered by ``mosaic.ready`` when all batches in a mosaic are processed.
Handles coordinate determination (Fiji stitch + process_tile_coord),
template generation (Jinja2), and stitching for all modalities.

Event payloads must include ``mosaic_ident``. Stitching parameters come from the
project config block; the payload may override any of the keys listed in
:obj:`opticstream.flows.psoct.utils.PROCESS_MOSAIC_FLOW_KWARGS_KEYS` (same
pattern as LSM strip flows).
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import jinja2
import yaml
from prefect import flow, task
from prefect.logging import get_run_logger

from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.events.utils import get_event_trigger
from opticstream.flows.psoct.utils import (
    load_scan_config_for_payload,
    mosaic_ident_from_payload,
)
from opticstream.state.state_guards import (
    enter_flow_stage,
    force_rerun_from_payload,
    should_skip_run,
)
from opticstream.state.oct_project_state import OCTMosaicId, OCT_STATE_SERVICE
from opticstream.data_processing.stitch import (
    fiji_stitch,
    fit_coord_files,
    generate_mask,
)
from opticstream.data_processing.stitch.process_tile_coord import process_tile_coord
from opticstream.events import (
    MOSAIC_READY,
    MOSAIC_ENFACE_STITCHED,
)
from opticstream.events.psoct_event_emitters import emit_mosaic_psoct_event
from opticstream.utils.utils import (
    get_dandi_slice_path,
    get_illumination,
    get_mosaic_paths,
    mosaic_id_to_slice_id,
)


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
            "mip:{{ modality }}",  # Replace mip with Jinja2 template variable
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


@task(task_run_name="mosaic-{mosaic_id}-generate-tile-info-{modality}")
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

    from linc_convert.modalities.psoct.mosaic import mosaic2d
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


@task(task_run_name="{project_name}-mosaic-{mosaic_id}-symlink-enface-to-dandi")
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
    logger = get_run_logger()
    logger.info(
        f"Creating symlinks for enface files to DANDI directory: {dandi_slice_path}"
    )

    # Ensure DANDI slice directory exists
    dandi_slice_path.mkdir(parents=True, exist_ok=True)

    slice_id = mosaic_id_to_slice_id(mosaic_id)
    acq = "tilted" if mosaic_id % 2 == 0 else "normal"
    symlink_targets = {}

    for modality, outputs in enface_outputs.items():
        if not isinstance(outputs, dict) or "nifti" not in outputs:
            logger.warning(
                f"Skipping modality {modality}: missing 'nifti' key in outputs"
            )
            continue

        source_path = Path(outputs["nifti"])
        if not source_path.exists():
            logger.warning(
                f"Skipping modality {modality}: source file does not exist: {source_path}"
            )
            continue

        # Generate target filename using template
        try:
            target_filename = mosaic_enface_format.format(
                project_name=project_name,
                slice_id=slice_id,
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
                    logger.warning(
                        f"Target already exists (not a symlink), skipping: {target_path}"
                    )
                    continue

            target_path.symlink_to(source_path)
            symlink_targets[modality] = target_path
            logger.info(f"Created symlink: {target_path} -> {source_path}")
        except OSError as e:
            logger.error(f"Failed to create symlink for {modality}: {e}")
            continue

    logger.info(f"Created {len(symlink_targets)} symlinks for enface modalities")
    return symlink_targets


def stitch_2d_modality(
    modality: str,
    mosaic_id: int,
    template_path: Path,
    processed_path: Path,
    stitched_path: Path,
    scan_resolution_2d: List[float],
    mask_path: Optional[Path] = None,
    circular_mean: Optional[bool] = None,
) -> tuple[Any, Path, Path, Path]:
    """
    Generate tile info file and submit stitching task for a 2D modality.

    This helper function encapsulates the common pattern of generating a tile info
    file from a template and submitting a 2D stitching task. It handles:
    - Tile info file generation with optional mask
    - Automatic circular_mean detection for "ori" modality
    - Async task submission with proper dependencies

    Parameters
    ----------
    modality : str
        Modality name (e.g., "aip", "mip", "ret", "ori", "biref", "surf")
    mosaic_id : int
        Mosaic identifier
    template_path : Path
        Path to Jinja2 template for tile info files
    processed_path : Path
        Path to processed tiles directory
    stitched_path : Path
        Path to stitched output directory
    scan_resolution_2d : List[float]
        Scan resolution for 2D [x, y]
    mask_path : Path, optional
        Path to mask file (None for modalities without mask like AIP/MIP)
    circular_mean : bool, optional
        Whether to use circular mean for orientation data.
        If None, automatically determined from modality name ("ori" -> True)

    Returns
    -------
    tuple[Any, Path, Path, Path]
        Tuple of (stitch_future, tile_info_path, nifti_path, jpeg_path)
        - stitch_future: Prefect future for the stitching task
        - tile_info_path: Path to generated tile info YAML file
        - nifti_path: Path to output NIfTI file
        - jpeg_path: Path to output JPEG file
    """
    # Determine circular_mean if not provided
    if circular_mean is None:
        circular_mean = modality == "ori"

    # Generate output paths
    tile_info_path = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.yaml"
    nifti_path = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.nii.gz"
    jpeg_path = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.jpeg"

    # Generate tile_info_file using template
    tile_info_future = generate_tile_info_file_task.submit(
        template_path=str(template_path),
        output_path=str(tile_info_path),
        base_dir=str(processed_path),
        modality=modality,
        mosaic_id=mosaic_id,
        mask=str(mask_path) if mask_path is not None else None,
        scan_resolution=scan_resolution_2d,
    )

    # Submit stitch task asynchronously, waiting for tile info file generation
    stitch_future = stitch_mosaic2d_task.submit(
        tile_info_file=str(tile_info_path),
        nifti_output=str(nifti_path),
        jpeg_output=str(jpeg_path),
        circular_mean=circular_mean,
        wait_for=[tile_info_future],
    )

    return stitch_future, tile_info_path, nifti_path, jpeg_path


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}")
def process_stitching_coordinates(
    project_name: str,
    mosaic_id: int,
    project_base_path: str,
    grid_size_x: int,
    grid_size_y: int,
    tile_overlap: float,
    mask_threshold: float,
    illumination: str,
) -> tuple[Path, Path]:
    """
    Process coordinate determination for first slice (mosaic_001 or mosaic_002).
    This includes Fiji stitch, tile coordinate processing, and template generation.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Base mosaic ID (1 for normal, 2 for tilted)
    grid_size_x : int
        Number of columns (batches) in mosaic
    grid_size_y : int
        Number of rows (tiles per batch) in mosaic
    tile_overlap : float
        Overlap between tiles in pixels
    mask_threshold : float
        Threshold for coordinate processing
    scan_resolution_2d : List[float]
        Scan resolution for 2D [x, y]
    illumination : str
        Illumination type ("normal" or "tilted")

    Returns
    -------
    tuple[Path, Path]
        Tuple of (template_path, tile_coords_export_path)
    """
    logger = get_run_logger()
    # Use slice-based structure
    illumination = get_illumination(mosaic_id)
    base_processed_path, base_stitched_path, _, _ = get_mosaic_paths(
        project_base_path, mosaic_id
    )

    logger.info(
        f"Processing coordinate determination for {illumination} illumination "
        f"(mosaic {mosaic_id}) with mask_threshold={mask_threshold}"
    )

    # Step 1: Run Fiji stitch to generate TileConfiguration files
    file_template = f"mosaic_{mosaic_id:03d}_image_{{iiii}}_aip.nii"
    output_textfile_name = (
        "TileConfigurationTilted.txt"
        if illumination == "tilted"
        else "TileConfiguration.txt"
    )

    ideal_coord_file = fiji_stitch_task(
        directory=str(base_processed_path),
        file_template=file_template,
        grid_size_x=grid_size_x,
        grid_size_y=grid_size_y,
        tile_overlap=tile_overlap,
        output_textfile_name=output_textfile_name,
    )

    # Wait for registered file (Fiji generates this)
    stitched_coord_file = base_processed_path / output_textfile_name.replace(
        ".txt", ".registered.txt"
    )

    if not stitched_coord_file.exists():
        raise FileNotFoundError(
            f"Expected registered coordinate file not found: {stitched_coord_file}"
        )

    # Step 2: Process tile coordinates and export to YAML
    tile_coords_export = base_stitched_path / (
        f"mosaic_{mosaic_id:03d}_tile_coords_export.yaml"
    )

    process_tile_coord_task(
        ideal_coord_file=str(ideal_coord_file),
        stitched_coord_file=str(stitched_coord_file),
        image_dir=str(base_processed_path),
        export=str(tile_coords_export),
        threshold=mask_threshold,
    )

    # Step 3: Generate Jinja2 template (once per illumination type)
    template_filename = f"tile_info_{illumination}.j2"
    template_path = Path(project_base_path) / template_filename

    generate_coord_template_task(
        tile_coords_export_path=str(tile_coords_export),
        template_output_path=str(template_path),
        base_dir=str(base_processed_path),
        mosaic_id=mosaic_id,
    )

    logger.info(
        f"Generated template for {illumination} illumination at {template_path}"
    )

    return template_path, tile_coords_export


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}")
def stitch_enface_modalities_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    template_path: Path,
    processed_path: Path,
    stitched_path: Path,
    mask_path: Path,
    scan_resolution_2d: List[float],
    enface_modalities: List[str],
) -> Dict[str, Dict[str, str]]:
    """
    Subflow to stitch all 2D enface modalities.

    This subflow handles the stitching of all enface modalities (ret, ori, biref, surf)
    asynchronously and returns the outputs. Note: MIP is excluded as it's already
    stitched in step 4 without mask.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    template_path : Path
        Path to Jinja2 template for tile info files
    tiles_data_path : Path
        Path to tile coordinates export YAML
    processed_path : Path
        Path to processed tiles directory
    stitched_path : Path
        Path to stitched output directory
    mask_path : Path
        Path to mask file
    scan_resolution_2d : List[float]
        Scan resolution for 2D [x, y]

    Returns
    -------
    Dict[str, Dict[str, str]]
        Dictionary mapping modality to output file paths
    """
    logger = get_run_logger()
    logger.info(f"Stitching enface modalities for mosaic {mosaic_id}")

    # Stitch all enface modalities asynchronously (using template)
    # Exclude MIP and AIP since they're already stitched in step 4
    enface_futures = {}
    for modality in enface_modalities:
        if modality in ["mip", "aip"]:
            continue  # Skip MIP and AIP, already stitched

        # Use unified helper function for tile info generation and stitching
        future, _, _, _ = stitch_2d_modality(
            modality=modality,
            mosaic_id=mosaic_id,
            template_path=template_path,
            processed_path=processed_path,
            stitched_path=stitched_path,
            scan_resolution_2d=scan_resolution_2d,
            mask_path=mask_path,
            circular_mean=None,  # Auto-determined from modality name
        )
        enface_futures[modality] = future

    # Wait for all enface stitching to complete
    enface_outputs = {}
    for modality, future in enface_futures.items():
        outputs = future.result()
        enface_outputs[modality] = outputs

    logger.info(f"All enface modalities stitched for mosaic {mosaic_id}")

    return enface_outputs


@flow(flow_run_name="process-mosaic-{mosaic_ident}")
def process_mosaic(
    mosaic_ident: OCTMosaicId,
    config: PSOCTScanConfigModel,
    *,
    force_refresh_coords: bool = False,
    force_rerun: bool = False,
) -> Dict[str, Any]:
    """
    Event-driven flow triggered by 'mosaic.processed' event.
    Processes a complete mosaic: determines coordinates, generates template,
    and stitches all modalities.

    Note:
    - This flow expects all parameters to be provided. Event flows handle loading
      config blocks and providing defaults.
    - Coordinate determination (Fiji stitch + process_tile_coord) only runs for
      mosaic_001 (normal) and mosaic_002 (tilted) unless force_refresh_coords=True.
    - Jinja2 template is generated once per illumination type and reused for all
      mosaics of that type.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    grid_size_x : int
        Number of columns (batches) in mosaic
    grid_size_y : int
        Number of rows (tiles per batch) in mosaic
    tile_overlap : float
        Overlap between tiles in pixels
    mask_threshold : float
        Threshold for mask generation and coordinate processing
    scan_resolution_3d : Optional[List[float]], optional
        Scan resolution for 3D volumes [x, y, z]
        First 2 elements are used for 2D modalities
        Default: [0.01, 0.01, 0.0025]
    enface_modalities : Optional[List[str]], optional
        List of enface modalities to stitch
        Default: ["ret", "ori", "biref", "mip", "surf"]
    volume_modalities : Optional[List[str]], optional
        List of volume modalities to stitch
        Default: ["dBI", "R3D", "O3D"]
    force_refresh_coords : bool, optional
        Force coordinate determination even if not first slice. Default: False
    stitch_3d_volumes : bool, optional
        Whether to stitch 3D volume modalities. Default: True

    Returns
    -------
    Dict[str, Any]
        Dictionary with output paths and status
    """
    logger = get_run_logger()

    if should_skip_run(
        enter_flow_stage(
            OCT_STATE_SERVICE.peek_mosaic(mosaic_ident=mosaic_ident),
            force_rerun=force_rerun,
            skip_if_running=False,
            item_ident=mosaic_ident,
        )
    ):
        return

    # Note: Event flows handle loading config blocks and providing all required values.
    # Defaults are provided in the function signature for optional parameters.

    scan_resolution_2d = config.acquisition.scan_resolution_3d[:2]

    processed_path, stitched_path, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    stitched_path.mkdir(parents=True, exist_ok=True)

    # Determine if this is normal or tilted illumination
    # Normal: odd mosaic_id (1, 3, 5, ...), Tilted: even mosaic_id (2, 4, 6, ...)
    is_tilted = mosaic_id % 2 == 0
    illumination = "tilted" if is_tilted else "normal"

    logger.info(
        f"Processing mosaic {mosaic_id} ({illumination} illumination) "
        f"with grid {grid_size_x}x{grid_size_y}, mask_threshold={mask_threshold}"
    )

    # Determine base mosaic ID for this illumination type
    # Normal: mosaic_001, Tilted: mosaic_002
    # Both belong to slice 1
    base_mosaic_id = 2 if is_tilted else 1
    base_processed_path, base_stitched_path, _, _ = get_mosaic_paths(
        project_base_path, base_mosaic_id
    )

    # Get template path and tile coordinates export path
    template_filename = f"tile_info_{illumination}.j2"
    template_path = Path(project_base_path) / template_filename

    # Check if we need to run coordinate determination
    # Only run for mosaic_001 (normal) or mosaic_002 (tilted) unless overridden
    first_slice_run = (mosaic_ident.slice_id == 1) or force_refresh_coords
    if first_slice_run:
        # Process first slice coordinates (Fiji stitch, coordinate processing, template generation)
        template_path, tile_coords_export = process_stitching_coordinates(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            grid_size_x=grid_size_x,
            grid_size_y=grid_size_y,
            tile_overlap=tile_overlap,
            mask_threshold=mask_threshold,
            illumination=illumination,
        )
    else:
        logger.info(
            f"Skipping coordinate determination for mosaic {mosaic_id}. "
            f"Using template from mosaic {base_mosaic_id})"
        )
        # Construct tile_coords_export path from base mosaic
        _tile_coords_export = (
            base_stitched_path / f"mosaic_{base_mosaic_id:03d}_tile_coords_export.yaml"
        )

    if not template_path.exists():
        raise FileNotFoundError(
            f"Template not found: {template_path}. "
            f"Coordinate determination must be run for mosaic {base_mosaic_id} first."
        )

    # Step 4: Stitch AIP and MIP in parallel (both without mask) - asynchronous
    # Use unified helper function for both modalities
    aip_future, _, aip_nifti, aip_jpeg = stitch_2d_modality(
        modality="aip",
        mosaic_id=mosaic_id,
        template_path=template_path,
        processed_path=processed_path,
        stitched_path=stitched_path,
        scan_resolution_2d=scan_resolution_2d,
        mask_path=None,  # AIP doesn't use mask
        circular_mean=False,
    )

    mip_future, _, mip_nifti, mip_jpeg = stitch_2d_modality(
        modality="mip",
        mosaic_id=mosaic_id,
        template_path=template_path,
        processed_path=processed_path,
        stitched_path=stitched_path,
        scan_resolution_2d=scan_resolution_2d,
        mask_path=None,  # MIP doesn't use mask
        circular_mean=False,
    )

    # Wait for both AIP and MIP stitching to complete
    aip_future.wait()
    mip_future.wait()

    # Step 5: Generate mask from MIP (asynchronous, depends on MIP stitching)
    mask_path = stitched_path / f"mosaic_{mosaic_id:03d}_mask.nii.gz"

    mask_future = generate_mask_task.submit(
        input_image=str(aip_nifti),
        output_mask=str(mask_path),
        threshold=mask_threshold,
        wait_for=[mip_future, aip_future],
    )

    # Wait for mask generation to complete
    mask_future.wait()

    # Step 6: Stitch all enface modalities using subflow (MIP excluded, already stitched)
    enface_outputs = stitch_enface_modalities_flow(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        template_path=template_path,
        processed_path=processed_path,
        stitched_path=stitched_path,
        mask_path=mask_path,
        scan_resolution_2d=scan_resolution_2d,
        enface_modalities=enface_modalities,
    )
    enface_outputs["aip"] = {"nifti": str(aip_nifti), "jpeg": str(aip_jpeg)}
    enface_outputs["mip"] = {"nifti": str(mip_nifti), "jpeg": str(mip_jpeg)}

    # Create symlinks to DANDI directory if configured
    if dandiset_path and mosaic_enface_format:
        slice_id = mosaic_id_to_slice_id(mosaic_id)
        dandi_slice_path = get_dandi_slice_path(dandiset_path, slice_id)
        symlink_targets = symlink_enface_to_dandi_task(
            enface_outputs=enface_outputs,
            dandi_slice_path=dandi_slice_path,
            project_name=project_name,
            mosaic_id=mosaic_id,
            mosaic_enface_format=mosaic_enface_format,
        )
        logger.info(
            f"Created {len(symlink_targets)} symlinks for enface files to DANDI"
        )
    else:
        # Use the original NIfTI path as the symlink target, not the standardized DANDI format
        symlink_targets = {
            modality: Path(outputs["nifti"])
            for modality, outputs in enface_outputs.items()
            if isinstance(outputs, dict) and "nifti" in outputs
        }
        if dandiset_path is None:
            logger.debug("dandiset_path not configured, skipping enface symlinking")
        if mosaic_enface_format is None:
            logger.debug(
                "mosaic_enface_format not provided, skipping enface symlinking"
            )
        symlink_targets = {}
    emit_mosaic_psoct_event(
        MOSAIC_ENFACE_STITCHED,
        mosaic_ident,
        extra_payload={
            "enface_outputs": enface_outputs,
            "symlink_targets": {
                k: str(v) for k, v in symlink_targets.items()
            }
            if symlink_targets
            else {},
        },
    )

    # Update OCT project state for this mosaic
    with OCT_STATE_SERVICE.open_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    ) as mosaic_state:
        mosaic_state.mark_completed()
        mosaic_state.set_enface_stitched(True)

    logger.info(f"Mosaic {mosaic_id} enface stitching complete")

    return {
        "mosaic_id": mosaic_id,
        "aip_path": str(aip_nifti),
        "mip_path": str(mip_nifti),
        "mask_path": str(mask_path),
        "enface_outputs": enface_outputs,
    }


@flow
def process_mosaic_event_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Event wrapper for :func:`process_mosaic`.

    Expects ``mosaic_ident`` and optional parameter overrides in ``payload`` (see
    :data:`opticstream.flows.psoct.utils.PROCESS_MOSAIC_FLOW_KWARGS_KEYS`).
    """
    mosaic_ident = mosaic_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    return process_mosaic(
        mosaic_ident,
        cfg,
        force_refresh_coords=bool(payload.get("force_refresh_coords", False)),
        force_rerun=force_rerun_from_payload(payload),
        flow_payload=payload,
    )

def to_deployment(project_name: Optional[str] = None):
    return process_mosaic_event_flow.to_deployment(
        name="process_mosaic_event_flow",
        tags=["event-driven", "mosaic-processing", "stitching"],
        triggers=[
            get_event_trigger(MOSAIC_READY, project_name=project_name),
        ],
    ),
    
