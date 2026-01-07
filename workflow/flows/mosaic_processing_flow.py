"""
Event-driven mosaic processing flow.

This flow is triggered by the 'mosaic.ready' event when all batches
in a mosaic are processed. It handles:
1. Coordinate determination (Fiji stitch + process_tile_coord)
2. Template generation (once, using Jinja2)
3. Stitching for all modalities (using template)
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.events import emit_event
from prefect.logging import get_run_logger

from workflow.events import (
    MOSAIC_READY,
    MOSAIC_STITCHED,
    MOSAIC_VOLUME_STITCHED,
    get_event_trigger,
)
from workflow.tasks.mosaic_processing import (
    fiji_stitch_task,
    find_focus_plane_task,
    generate_coord_template_task,
    generate_mask_task,
    generate_tile_info_file_task,
    process_tile_coord_task,
    stitch_mosaic2d_task,
    stitch_mosaic3d_task,
    symlink_enface_to_dandi_task,
)
from workflow.tasks.utils import (
    get_dandi_slice_path,
    get_illumination,
    get_mosaic_paths,
    mosaic_id_to_slice_number,
)
from workflow.config.project_config import (
    get_grid_size_x,
    get_project_config_block,
    resolve_config_param,
)
from linc_convert.utils.zarr_config import ZarrConfig


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
        f"(mosaic {mosaic_id})"
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


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}")
def stitch_volume_modalities_flow(
    project_name: str,
    mosaic_id: int,
    project_base_path: str,
    template_path: Path,
    tiles_data_path: Path,
    processed_path: Path,
    stitched_path: Path,
    mask_path: Path,
    scan_resolution_3d: List[float],
    volume_modalities: List[str],
    dandiset_path: Optional[str] = None,
    mosaic_volume_format: Optional[str] = None,
    zarr_config: Optional[ZarrConfig] = None,
    kwargs: Dict[str, Any] = {},
) -> Dict[str, Path]:
    """
    Subflow to stitch all 3D volume modalities.

    Parameters
    ----------
    project_name : str
        Project identifier
    mosaic_id : int
        Mosaic identifier
    project_base_path : str
        Base path for the project
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
    scan_resolution_3d : List[float]
        Scan resolution for 3D volumes [x, y, z]
    volume_modalities : List[str]
        List of volume modalities to stitch
    dandiset_path : str, optional
        Path to DANDI derivatives directory (already includes derivatives/sub-{subject}/)
    mosaic_volume_format : str, optional
        Template string for volume filename format
    zarr_config : ZarrConfig, optional
        Zarr configuration object for zarr file creation
    kwargs : Dict[str, Any], optional
        Additional keyword arguments for 3D stitching task

    Returns
    -------
    Dict[str, Path]
        Dictionary mapping modality to output file paths
    """
    logger = get_run_logger()
    logger.info(f"Stitching volume modalities for mosaic {mosaic_id}")
    volume_futures = {}
    volume_outputs = {}
    slice_number = mosaic_id_to_slice_number(mosaic_id)
    acq = "tilted" if mosaic_id % 2 == 0 else "normal"

    # Determine output directory: use DANDI if configured, otherwise use processed_path
    if dandiset_path and mosaic_volume_format:
        dandi_slice_path = get_dandi_slice_path(dandiset_path, slice_number)
        dandi_slice_path.mkdir(parents=True, exist_ok=True)
        output_dir = dandi_slice_path
        logger.info(f"Writing volumes to DANDI directory: {dandi_slice_path}")
    else:
        output_dir = processed_path
        if dandiset_path is None:
            logger.warning(
                f"dandiset_path not configured, writing volumes to processed directory: {processed_path}"
            )
        if mosaic_volume_format is None:
            logger.warning(
                "mosaic_volume_format not provided, using default filename format"
            )

    # Use kwargs parameter, but ensure circular_mean and voxel_size_xyz are set per modality
    stitching_kwargs = kwargs.copy()
    stitching_kwargs.setdefault("voxel_size_xyz", scan_resolution_3d)
    for modality in volume_modalities:
        modality_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.yaml"

        # Set circular_mean based on modality
        stitching_kwargs["circular_mean"] = True if modality == "O3D" else False

        generate_tile_info_file_task(
            template_path=str(template_path),
            output_path=str(modality_tile_info),
            base_dir=str(processed_path),
            modality=modality,
            mosaic_id=mosaic_id,
            scan_resolution=scan_resolution_3d,
        )

        # Generate filename using template if provided, otherwise use default format
        if mosaic_volume_format:
            filename = mosaic_volume_format.format(
                project_name=project_name,
                slice_id=slice_number,
                acq=acq,
                modality=modality,
            )
        else:
            # Fallback to default format
            filename = f"{project_name}_sample-slice{slice_number:02d}_acq-{acq}_proc-{modality}_OCT.ome.zarr"

        output_path = output_dir / filename
        future = stitch_mosaic3d_task.submit(
            str(modality_tile_info),
            str(output_path),
            zarr_config=zarr_config,
            kwargs=stitching_kwargs,
        )
        volume_futures[modality] = future
        volume_outputs[modality] = output_path

    # Wait for all volume stitching to complete
    for modality, future in volume_futures.items():
        future.wait()

    logger.info(f"All volume modalities stitched for mosaic {mosaic_id}")

    # Emit MOSAIC_VOLUME_STITCHED event
    emit_event(
        event=MOSAIC_VOLUME_STITCHED,
        resource={
            "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "volume_outputs": {mod: str(path) for mod, path in volume_outputs.items()},
        },
    )

    return volume_outputs


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}")
def process_mosaic_flow(
    project_name: str,
    mosaic_id: int,
    project_base_path: str,
    grid_size_x: int,
    grid_size_y: int,
    tile_overlap: float,
    mask_threshold: float,
    scan_resolution_3d: List[float],
    enface_modalities: List[str],
    volume_modalities: List[str],
    force_refresh_coords: bool = False,
    stitch_3d_volumes: bool = True,
    dandiset_path: Optional[str] = None,
    mosaic_enface_format: Optional[str] = None,
    mosaic_volume_format: Optional[str] = None,
    zarr_config: Optional[ZarrConfig] = None,
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
    scan_resolution_3d : List[float]
        Scan resolution for 3D volumes [x, y, z]
        First 2 elements are used for 2D modalities
    enface_modalities : List[str]
        List of enface modalities to stitch
    volume_modalities : List[str]
        List of volume modalities to stitch
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

    # Note: Event flows handle loading config blocks and providing all required values.
    # All parameters are required and should be provided by the caller.

    scan_resolution_2d = scan_resolution_3d[:2]

    processed_path, stitched_path, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    stitched_path.mkdir(parents=True, exist_ok=True)

    # Determine if this is normal or tilted illumination
    # Normal: odd mosaic_id (1, 3, 5, ...), Tilted: even mosaic_id (2, 4, 6, ...)
    is_tilted = mosaic_id % 2 == 0
    illumination = "tilted" if is_tilted else "normal"

    logger.info(
        f"Processing mosaic {mosaic_id} ({illumination} illumination) "
        f"with grid {grid_size_x}x{grid_size_y}"
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
    first_slice_run = (mosaic_id <= 2) or force_refresh_coords
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
        tile_coords_export = (
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
    aip_outputs = aip_future.wait()
    mip_outputs = mip_future.wait()

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
        slice_number = mosaic_id_to_slice_number(mosaic_id)
        dandi_slice_path = get_dandi_slice_path(dandiset_path, slice_number)
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
    emit_event(
        event=MOSAIC_STITCHED,
        resource={
            "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "enface_outputs": enface_outputs,
            "symlink_targets": symlink_targets,
        },
    )
    # Step 6.5: Focus finding for first slice (Section 3.3)
    # Focus finding requires unfiltered surface data, so it runs after surface stitching
    if first_slice_run:
        # This is the first slice for this illumination type
        # Find focus plane using stitched surface (unfiltered)
        # Surface is stitched as part of enface modalities
        surf_output = enface_outputs.get("surf")
        if surf_output:
            # surf_output is a dict with 'nifti', 'jpeg', 'tiff' keys from stitch_mosaic2d_task
            surface_nifti = (
                surf_output.get("nifti")
                if isinstance(surf_output, dict)
                else str(surf_output)
            )
            surface_path = Path(surface_nifti) if surface_nifti else None
            if surface_path and surface_path.exists():
                focus_path = find_focus_plane_task(
                    project_base_path=project_base_path,
                    mosaic_id=mosaic_id,
                    stitched_surface_path=str(surface_path),
                    illumination=illumination,
                )
                logger.info(
                    f"Focus plane determined for {illumination} illumination: {focus_path}"
                )
            else:
                logger.warning(
                    f"Surface map NIfTI not found or doesn't exist for focus finding in mosaic {mosaic_id}. "
                    f"Path: {surface_path}. Focus finding skipped."
                )
        else:
            logger.warning(
                f"Surface map not found in enface outputs for mosaic {mosaic_id}. "
                f"Focus finding skipped."
            )
    # Step 7: Stitch volume modalities if enabled
    volume_outputs = {}
    if stitch_3d_volumes and volume_modalities:
        logger.info(f"Stitching volume modalities for mosaic {mosaic_id}")
        volume_outputs = stitch_volume_modalities_flow(
            project_name=project_name,
            mosaic_id=mosaic_id,
            project_base_path=project_base_path,
            template_path=template_path,
            tiles_data_path=tile_coords_export,
            processed_path=processed_path,
            stitched_path=stitched_path,
            mask_path=mask_path,
            scan_resolution_3d=scan_resolution_3d,
            volume_modalities=volume_modalities,
            dandiset_path=dandiset_path,
            mosaic_volume_format=mosaic_volume_format,
            zarr_config=zarr_config,
            kwargs={
                "overwrite": True,
                "focus_plane": str(focus_path),
                "normalize_focus_plane": True,
                "crop_focus_plane_depth": 500,
                "crop_focus_plane_offset": 30,
            },  # Can be extended to load from config if needed
        )
        logger.info(f"All volume modalities stitched for mosaic {mosaic_id}")
    else:
        if not stitch_3d_volumes:
            logger.info(
                f"Volume stitching disabled for mosaic {mosaic_id} (stitch_3d_volumes=False)"
            )
        elif not volume_modalities:
            logger.info(f"No volume modalities to stitch for mosaic {mosaic_id}")

    logger.info(f"Mosaic {mosaic_id} processing complete")

    return {
        "mosaic_id": mosaic_id,
        "aip_path": str(aip_nifti),
        "mip_path": str(mip_nifti),
        "mask_path": str(mask_path),
        "enface_outputs": enface_outputs,
        "volume_outputs": volume_outputs,
    }


@flow
def process_mosaic_event_flow(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering.
    Loads config block and provides defaults using priority: payload → block → class defaults.
    Extracts parameters from the event payload and calls process_mosaic_flow.

    The payload is passed as JSON from the event, so types should be preserved.
    However, we ensure proper type conversion for safety.
    """
    logger = get_run_logger()
    project_name = payload["project_name"]
    mosaic_id = int(payload["mosaic_id"])

    # Load config block
    project_config = get_project_config_block(project_name)
    if project_config is None:
        logger.warning(
            f"Project config block for '{project_name}' not found. "
            f"Using defaults from payload or class defaults."
        )

    # Build parameter dict with priority: payload → block → class defaults
    params = {
        "project_name": project_name,
        "mosaic_id": mosaic_id,
    }

    # Required field: project_base_path (must be in payload or block)
    project_base_path = resolve_config_param(
        payload,
        "project_base_path",
        project_config,
        config_attr="project_base_path",
    )
    if project_base_path is None:
        raise ValueError(
            f"project_base_path is required but not found in payload and config block "
            f"'{project_name}-config' is missing. Please provide project_base_path in event payload "
            f"or create the config block."
        )
    params["project_base_path"] = project_base_path

    # Required field: grid_size_x (payload → block → get_grid_size_x → error)
    # Special case: can come from "grid_size_x" or "total_batches"
    if "grid_size_x" in payload or "total_batches" in payload:
        params["grid_size_x"] = int(
            payload.get("grid_size_x") or payload.get("total_batches")
        )
    else:
        # Will raise error if config block not found
        params["grid_size_x"] = get_grid_size_x(project_name, mosaic_id)

    # Optional field: grid_size_y (payload → block → class default)
    grid_size_y = resolve_config_param(
        payload,
        "grid_size_y",
        project_config,
        converter=int,
        config_attr="grid_size_y",
    )
    if grid_size_y is None:
        raise ValueError(
            "grid_size_y is required but cannot be determined from config block or defaults"
        )
    params["grid_size_y"] = grid_size_y

    # Optional field: tile_overlap (payload → block → class default)
    params["tile_overlap"] = (
        resolve_config_param(
            payload,
            "tile_overlap",
            project_config,
            default=20.0,
            converter=float,
            config_attr="tile_overlap",
        )
        or 20.0
    )

    # Optional field: mask_threshold (payload → block → class default)
    params["mask_threshold"] = (
        resolve_config_param(
            payload,
            "mask_threshold",
            project_config,
            default=50.0,
            converter=float,
            config_attr="mask_threshold",
        )
        or 50.0
    )

    # Optional field: scan_resolution_3d (payload → block → class default)
    scan_resolution_3d = resolve_config_param(
        payload,
        "scan_resolution_3d",
        project_config,
        config_attr="scan_resolution_3d",
    )
    if scan_resolution_3d is not None:
        # Ensure it's a list of floats (handles both list and tuple from config)
        params["scan_resolution_3d"] = [float(x) for x in scan_resolution_3d]
    else:
        params["scan_resolution_3d"] = None

    # Optional field: force_refresh_coords (payload → default False)
    force_refresh_coords = payload.get("force_refresh_coords", False)
    if isinstance(force_refresh_coords, str):
        force_refresh_coords = force_refresh_coords.lower() == "true"
    params["force_refresh_coords"] = force_refresh_coords

    # Optional field: enface_modalities (payload → block → class default)
    enface_modalities = resolve_config_param(
        payload,
        "enface_modalities",
        project_config,
        default=["ret", "ori", "biref", "mip", "surf"],
        config_attr="enface_modalities",
    )
    params["enface_modalities"] = (
        list(enface_modalities)
        if enface_modalities is not None
        else ["ret", "ori", "biref", "mip", "surf"]
    )

    # Optional field: volume_modalities (payload → block → class default)
    volume_modalities = resolve_config_param(
        payload,
        "volume_modalities",
        project_config,
        default=["dBI", "R3D", "O3D"],
        config_attr="volume_modalities",
    )
    params["volume_modalities"] = (
        list(volume_modalities)
        if volume_modalities is not None
        else ["dBI", "R3D", "O3D"]
    )

    # Optional field: stitch_3d_volumes (payload → block → default True)
    stitch_3d_volumes = resolve_config_param(
        payload,
        "stitch_3d_volumes",
        project_config,
        default=True,
        config_attr="stitch_3d_volumes",
    )
    if stitch_3d_volumes is None:
        stitch_3d_volumes = True
    elif isinstance(stitch_3d_volumes, str):
        stitch_3d_volumes = stitch_3d_volumes.lower() == "true"
    params["stitch_3d_volumes"] = bool(stitch_3d_volumes)

    # Optional field: dandiset_path (payload → block → None)
    params["dandiset_path"] = resolve_config_param(
        payload,
        "dandiset_path",
        project_config,
        default=None,
        config_attr="dandiset_path",
    )

    # Optional field: mosaic_enface_format (payload → block → class default)
    params["mosaic_enface_format"] = resolve_config_param(
        payload,
        "mosaic_enface_format",
        project_config,
        config_attr="mosaic_enface_format",
    )

    # Optional field: mosaic_volume_format (payload → block → class default)
    params["mosaic_volume_format"] = resolve_config_param(
        payload,
        "mosaic_volume_format",
        project_config,
        config_attr="mosaic_volume_format",
    )

    # Optional field: zarr_config (from block only, not from payload)
    if project_config:
        params["zarr_config"] = project_config.zarr_config
    else:
        params["zarr_config"] = None

    return process_mosaic_flow(**params)


# Deployment configuration for event-driven triggering
if __name__ == "__main__":
    process_mosaic_event_flow_deployment = process_mosaic_event_flow.to_deployment(
        name="process_mosaic_event_flow",
        tags=["event-driven", "mosaic-processing", "stitching"],
        triggers=[
            get_event_trigger(MOSAIC_READY),
        ],
    )
