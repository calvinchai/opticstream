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

from workflow.events import MOSAIC_READY, MOSAIC_STITCHED, get_event_trigger
from workflow.tasks.mosaic_processing import (fiji_stitch_task,
                                              find_focus_plane_task,
                                              generate_coord_template_task,
                                              generate_mask_task,
                                              generate_tile_info_file_task,
                                              process_tile_coord_task,
                                              stitch_mosaic2d_task, stitch_mosaic3d_task)
from workflow.tasks.utils import get_illumination, get_mosaic_paths, mosaic_id_to_slice_number
from workflow.config.project_config import get_grid_size_x, get_project_config_block
from workflow.config.blocks import PSOCTScanConfig

# Helper function to get field default from Pydantic model
def _get_field_default(model_class, field_name: str):
    """Get default value for a Pydantic model field, supporting both v1 and v2."""
    # Try Pydantic v2 first (model_fields)
    if hasattr(model_class, 'model_fields') and field_name in model_class.model_fields:
        field_info = model_class.model_fields[field_name]
        if hasattr(field_info, 'default'):
            return field_info.default
    # Try Pydantic v1 (__fields__)
    if hasattr(model_class, '__fields__') and field_name in model_class.__fields__:
        field_info = model_class.__fields__[field_name]
        if hasattr(field_info, 'default'):
            return field_info.default
    # Fallback: return None
    return None

# Default constants from PSOCTScanConfig class
ENFACE_MODALITIES = _get_field_default(PSOCTScanConfig, 'enface_modalities') or ["ret", "ori", "biref", "mip", "surf"]
VOLUME_MODALITIES = _get_field_default(PSOCTScanConfig, 'volume_modalities') or ["dBI", "R3D", "O3D"]
_scan_res_3d_default = _get_field_default(PSOCTScanConfig, 'scan_resolution_3d')
DEFAULT_SCAN_RESOLUTION_3D = list(_scan_res_3d_default) if _scan_res_3d_default is not None else [0.01, 0.01, 0.0025]

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
    base_processed_path, base_stitched_path, _, _ = get_mosaic_paths(project_base_path,
                                                                     mosaic_id)

    logger.info(
        f"Processing coordinate determination for {illumination} illumination "
        f"(mosaic {mosaic_id})"
    )

    # Step 1: Run Fiji stitch to generate TileConfiguration files
    file_template = f"mosaic_{mosaic_id:03d}_image_{{iiii}}_aip.nii"
    output_textfile_name = (
        "TileConfigurationTilted.txt" if illumination == "tilted"
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
    tile_coords_export = base_stitched_path / (f"mosaic_"
                                               f""
                                               f"{mosaic_id:03d}_tile_coords_export.yaml")

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
    # Exclude MIP since it's already stitched in step 4
    enface_futures = {}
    for modality in ENFACE_MODALITIES:
        if modality == "mip":
            continue  # Skip MIP, already stitched
        modality_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.yaml"
        modality_nifti = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.nii"
        modality_jpeg = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.jpeg"
        modality_tiff = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.tif"

        # Generate tile_info_file using template
        generate_tile_info_file_task(
            template_path=str(template_path),
            output_path=str(modality_tile_info),
            base_dir=str(processed_path),
            modality=modality,
            mosaic_id=mosaic_id,
            mask=str(mask_path),
            scan_resolution=scan_resolution_2d,
        )

        # Submit stitch task asynchronously
        future = stitch_mosaic2d_task.submit(
            tile_info_file=str(modality_tile_info),
            nifti_output=str(modality_nifti),
            jpeg_output=str(modality_jpeg),
            tiff_output=str(modality_tiff),
            circular_mean=(modality == "ori"),
        )
        enface_futures[modality] = future

    # Wait for all enface stitching to complete
    enface_outputs = {}
    for modality, future in enface_futures.items():
        outputs = future.wait()
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
    kwargs: Dict[str, Any] = {},
) -> Dict[str, Dict[str, str]]:
    """
    Subflow to stitch all 3D volume modalities.
    """
    logger = get_run_logger()
    logger.info(f"Stitching volume modalities for mosaic {mosaic_id}")
    volume_futures = {}
    volume_outputs = {}
    # temporary for one time run
    processed_path = Path('/autofs/space/zircon_005/users/data/dandi/000053/derivatives/sub-I80/micr/voi-slab2')
    slice_number = mosaic_id_to_slice_number(mosaic_id)
    acq = "tilted" if mosaic_id % 2 == 0 else "normal"
    kwargs = {
        "clip_x": 15,
        "focus_plane": "/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/focus_normal.nii" if acq == "normal" else "/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/focus_tilted.nii",
        "normalize_focus_plane": True,
        "crop_focus_plane_depth": 500,
        "crop_focus_plane_offset": 0,
        "voxel_size_xyz": scan_resolution_3d,
        "driver": "tensorstore",
        "shard" : (1024,),
        "chunk" : (128,),
        "overwrite": True,
        "nii": False
    }
    for modality in VOLUME_MODALITIES:

        modality_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.yaml"

        kwargs["circular_mean"] = True if modality == "O3D" else False
        
        generate_tile_info_file_task(
            template_path=str(template_path),
            output_path=str(modality_tile_info),
            base_dir=str(processed_path),
            modality=modality,
            mosaic_id=mosaic_id,
            scan_resolution=scan_resolution_3d,
        )

        output_path = processed_path / f"{project_name}_sample-slice{slice_number:02d}_acq-{acq}_proc-{modality}_OCT.ome.zarr"
        future = stitch_mosaic3d_task.submit(
            str(modality_tile_info),
            str(output_path),
            kwargs,
        )
        volume_futures[modality] = future
        volume_outputs[modality] = output_path
    for modality, future in volume_futures.items():
        future.wait()
    
    logger.info(f"All volume modalities stitched for mosaic {mosaic_id}")
    emit_event(
        event="tile_batch.upload_to_linc.ready",
        resource={
            "prefect.resource.id": "{{ event.resource.id }}",
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "batch_id": 0,
            "archived_file_paths": list(volume_outputs.values()),
        }
    )
@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}")
def process_mosaic_flow(
    project_name: str,
    mosaic_id: int,
    project_base_path: Optional[str] = None,
    grid_size_x: Optional[int] = None,
    grid_size_y: Optional[int] = None,
    tile_overlap: Optional[float] = None,
    mask_threshold: Optional[float] = None,
    scan_resolution_3d: Optional[List[float]] = None,
    force_refresh_coords: bool = False,
) -> Dict[str, Any]:
    """
    Event-driven flow triggered by 'mosaic.processed' event.
    Processes a complete mosaic: determines coordinates, generates template,
    and stitches all modalities.
    
    Note: 
    - grid_size_x and grid_size_y should ideally come from the event payload
      or be retrieved from configuration/Artifact table.
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
        Mosaic identifier (parameterized, not hardcoded)
    grid_size_x : int
        Number of columns (batches) in mosaic
    grid_size_y : int
        Number of rows (tiles per batch) in mosaic
    tile_overlap : float
        Overlap between tiles in pixels
    mask_threshold : float
        Threshold for mask generation and coordinate processing
    scan_resolution_3d : List[float], optional
        Scan resolution for 3D volumes [x, y, z]. Default: [0.01, 0.01, 0.0025]
        First 2 elements are used for 2D modalities
    force_refresh_coords : bool
        Force coordinate determination even if not first slice. Default: False
        
    Returns
    -------
    Dict[str, Any]
        Dictionary with output paths and status
    """
    logger = get_run_logger()
    
    # Note: Event flows handle loading config blocks and providing defaults.
    # These checks are minimal defensive programming for direct calls.
    
    # Handle project_base_path (required - event flow should always provide this)
    if project_base_path is None:
        project_config = get_project_config_block(project_name)
        if project_config is None:
            raise ValueError(
                f"project_base_path is required but not provided and config block "
                f"'{project_name}-config' is missing. Please provide project_base_path "
                f"or create the config block."
            )
        project_base_path = project_config.project_base_path
    
    # Handle grid_size_x (can be None from event flow when config block exists)
    if grid_size_x is None:
        try:
            grid_size_x = get_grid_size_x(project_name, mosaic_id)
        except ValueError as e:
            raise ValueError(
                f"grid_size_x is required but cannot be determined. {str(e)}"
            )
    
    # Note: grid_size_y, tile_overlap, mask_threshold, scan_resolution_3d are always
    # provided by event flows, so no None handling needed here.
    # If called directly, these should be provided explicitly.
    
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
    base_processed_path, base_stitched_path, _, _ = get_mosaic_paths(project_base_path,
                                                                     base_mosaic_id)

    # Get template path and tile coordinates export path
    template_filename = f"tile_info_{illumination}.j2"
    template_path = Path(project_base_path) / template_filename

    # Check if we need to run coordinate determination
    # Only run for mosaic_001 (normal) or mosaic_002 (tilted) unless overridden
    first_slice_run = (mosaic_id <=2) or force_refresh_coords
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
            scan_resolution_2d=scan_resolution_2d,
            illumination=illumination,
        )
    else:
        logger.info(
            f"Skipping coordinate determination for mosaic {mosaic_id}. "
            f"Using template from mosaic {base_mosaic_id})"
        )

    if not template_path.exists():
        raise FileNotFoundError(
            f"Template not found: {template_path}. "
            f"Coordinate determination must be run for mosaic {base_mosaic_id} first."
        )

    # Step 4: Stitch AIP and MIP in parallel (both without mask) - asynchronous
    aip_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_aip.yaml"
    aip_nifti = stitched_path / f"mosaic_{mosaic_id:03d}_aip.nii"
    aip_jpeg = stitched_path / f"mosaic_{mosaic_id:03d}_aip.jpeg"

    mip_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_mip.yaml"
    mip_nifti = stitched_path / f"mosaic_{mosaic_id:03d}_mip.nii"
    mip_jpeg = stitched_path / f"mosaic_{mosaic_id:03d}_mip.jpeg"

    # Generate tile_info files for AIP and MIP (both without mask)
    generate_tile_info_file_task(
        template_path=str(template_path),
        output_path=str(aip_tile_info),
        base_dir=str(processed_path),
        modality="aip",
        mosaic_id=mosaic_id,
        scan_resolution=scan_resolution_2d,
    )

    generate_tile_info_file_task(
        template_path=str(template_path),
        output_path=str(mip_tile_info),
        base_dir=str(processed_path),
        modality="mip",
        mosaic_id=mosaic_id,
        scan_resolution=scan_resolution_2d,
    )

    # Submit both AIP and MIP stitching in parallel
    aip_future = stitch_mosaic2d_task.submit(
        tile_info_file=str(aip_tile_info),
        nifti_output=str(aip_nifti),
        jpeg_output=str(aip_jpeg),
        circular_mean=False,
    )

    mip_future = stitch_mosaic2d_task.submit(
        tile_info_file=str(mip_tile_info),
        nifti_output=str(mip_nifti),
        jpeg_output=str(mip_jpeg),
        circular_mean=False,
    )

    # Step 5: Generate mask from MIP (asynchronous, depends on MIP stitching)
    mask_path = stitched_path / f"mosaic_{mosaic_id:03d}_mask.nii"

    mask_future = generate_mask_task.submit(
        input_image=str(mip_nifti),
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
    )
    enface_outputs["aip"] = aip_nifti
    enface_outputs["mip"] = mip_nifti
    
    # Step 6.5: Focus finding for first slice (Section 3.3)
    # Focus finding requires unfiltered surface data, so it runs after surface stitching
    if first_slice_run:
        # This is the first slice for this illumination type
        # Find focus plane using stitched surface (unfiltered)
        # Surface is stitched as part of enface modalities
        surf_output = enface_outputs.get("surf")
        if surf_output:
            # surf_output is a dict with 'nifti', 'jpeg', 'tiff' keys from stitch_mosaic2d_task
            surface_nifti = surf_output.get("nifti") if isinstance(surf_output, dict) else str(surf_output)
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
    # Step 7: Stitch all volume modalities asynchronously (using template, no mask)
    volume_futures = {}
    for modality in VOLUME_MODALITIES:
        modality_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.yaml"
        modality_nifti = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.nii"

        # Generate tile_info_file using template
        
    
        # Submit stitch task asynchronously (volumes don't need JPEG/TIFF)
        # future = stitch_mosaic2d_task.submit(
        #     tile_info_file=str(modality_tile_info),
        #     out = 
        #     circular_mean=False,
        # )
        # volume_futures[modality] = future

    # Note: Per design document Section 6.2, we emit a single mosaic.stitched event
    # after all modalities (both enface and volume) are stitched
    logger.info(
        f"All enface modalities stitched for mosaic {mosaic_id}. "
        f"Volume stitching in progress..."
    )

    # Wait for all volume stitching to complete
    volume_outputs = {}
    for modality, future in volume_futures.items():
        outputs = future.wait()
        volume_outputs[modality] = outputs

    # Emit event that mosaic stitching is complete (per Section 6.2)
    emit_event(
        event=MOSAIC_STITCHED,
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "aip_path": str(aip_nifti),
            "mask_path": str(mask_path),
            "enface_outputs": enface_outputs,
            "volume_outputs": volume_outputs,
        }
    )

    logger.info(f"Mosaic {mosaic_id} processing complete")

    return {
        "mosaic_id": mosaic_id,
        "aip_path": str(aip_nifti),
        "mip_path": str(mip_nifti),
        "mask_path": str(mask_path),
        "enface_outputs": enface_outputs,
        "volume_outputs": volume_outputs,
    }


@flow(name="process_mosaic_event_flow")
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
    if "project_base_path" in payload:
        params["project_base_path"] = payload["project_base_path"]
    elif project_config:
        params["project_base_path"] = project_config.project_base_path
    else:
        raise ValueError(
            f"project_base_path is required but not found in payload and config block "
            f"'{project_name}-config' is missing. Please provide project_base_path in event payload "
            f"or create the config block."
        )
    
    # Optional field: grid_size_x (payload → block → get_grid_size_x → error)
    if "grid_size_x" in payload or "total_batches" in payload:
        params["grid_size_x"] = int(payload.get("grid_size_x") or payload.get("total_batches"))
    elif project_config:
        # Will be resolved in main flow via get_grid_size_x
        params["grid_size_x"] = None
    else:
        # Leave as None, will raise error in main flow if needed
        params["grid_size_x"] = None
    
    # Optional field: grid_size_y (payload → block → class default)
    if "grid_size_y" in payload:
        params["grid_size_y"] = int(payload["grid_size_y"])
    elif project_config:
        params["grid_size_y"] = project_config.grid_size_y
    else:
        params["grid_size_y"] = _get_field_default(PSOCTScanConfig, 'grid_size_y')
        if params["grid_size_y"] is None:
            raise ValueError("grid_size_y is required but cannot be determined from config block or defaults")
    
    # Optional field: tile_overlap (payload → block → class default)
    if "tile_overlap" in payload:
        params["tile_overlap"] = float(payload["tile_overlap"])
    elif project_config:
        params["tile_overlap"] = project_config.tile_overlap
    else:
        params["tile_overlap"] = _get_field_default(PSOCTScanConfig, 'tile_overlap') or 20.0
    
    # Optional field: mask_threshold (payload → block → class default)
    if "mask_threshold" in payload:
        params["mask_threshold"] = float(payload["mask_threshold"])
    elif project_config:
        params["mask_threshold"] = project_config.mask_threshold
    else:
        params["mask_threshold"] = _get_field_default(PSOCTScanConfig, 'mask_threshold') or 50.0
    
    # Optional field: scan_resolution_3d (payload → block → class default)
    if "scan_resolution_3d" in payload:
        scan_resolution_3d = payload["scan_resolution_3d"]
        # Ensure it's a list of floats
        params["scan_resolution_3d"] = [float(x) for x in scan_resolution_3d]
    elif project_config:
        params["scan_resolution_3d"] = list(project_config.scan_resolution_3d)
    else:
        params["scan_resolution_3d"] = DEFAULT_SCAN_RESOLUTION_3D
    
    # Optional field: force_refresh_coords (payload → default False)
    force_refresh_coords = payload.get("force_refresh_coords", False)
    if isinstance(force_refresh_coords, str):
        force_refresh_coords = force_refresh_coords.lower() == "true"
    params["force_refresh_coords"] = force_refresh_coords

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
    