"""
Event-driven mosaic processing flow.

This flow is triggered by the 'mosaic.processed' event when all batches
in a mosaic are processed. It handles:
1. Coordinate determination (Fiji stitch + process_tile_coord)
2. Template generation (once, using Jinja2)
3. Stitching for all modalities (using template)
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.events import DeploymentEventTrigger, emit_event

from workflow.tasks.mosaic_processing import (fiji_stitch_task,
                                              generate_coord_template_task,
                                              generate_mask_task,
                                              generate_tile_info_file_task,
                                              process_tile_coord_task,
                                              stitch_mosaic2d_task)
from workflow.tasks.utils import get_mosaic_paths, mosaic_id_to_slice_number

logger = logging.getLogger(__name__)

# Modality configurations
ENFACE_MODALITIES = ["ret", "ori", "biref", "mip", "surf"]
VOLUME_MODALITIES = ["dBI", "R3D", "O3D"]

# Default scan resolution (3D: [x, y, z], 2D uses first 2 elements)
DEFAULT_SCAN_RESOLUTION_3D = [0.01, 0.01, 0.0025]


def process_first_slice_coordinates(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    grid_size_x: int,
    grid_size_y: int,
    tile_overlap: float,
    mask_threshold: float,
    scan_resolution_2d: List[float],
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
    # Use slice-based structure
    slice_number = mosaic_id_to_slice_number(mosaic_id)
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
        scan_resolution=scan_resolution_2d,
        mosaic_id=mosaic_id,
    )

    logger.info(
        f"Generated template for {illumination} illumination at {template_path}"
    )

    return template_path, tile_coords_export


@flow(name="stitch_enface_modalities_flow")
def stitch_enface_modalities_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    template_path: Path,
    tiles_data_path: Path,
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


@flow(name="process_mosaic_flow")
def process_mosaic_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    grid_size_x: int,
    grid_size_y: int,
    tile_overlap: float = 20.0,
    mask_threshold: float = 50.0,
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
    # Use slice-based structure
    slice_number = mosaic_id_to_slice_number(mosaic_id)
    processed_path, stitched_path, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    processed_path.mkdir(parents=True, exist_ok=True)
    stitched_path.mkdir(parents=True, exist_ok=True)

    # Determine if this is normal or tilted illumination
    # Normal: odd mosaic_id (1, 3, 5, ...), Tilted: even mosaic_id (2, 4, 6, ...)
    is_tilted = mosaic_id % 2 == 0
    illumination = "tilted" if is_tilted else "normal"

    # Set scan resolution (3D, 2D uses first 2 elements)
    if scan_resolution_3d is None:
        scan_resolution_3d = DEFAULT_SCAN_RESOLUTION_3D
    scan_resolution_2d = scan_resolution_3d[:2]

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

    # Check if we need to run coordinate determination
    # Only run for mosaic_001 (normal) or mosaic_002 (tilted) unless forced
    should_run_coords = (mosaic_id == base_mosaic_id) or force_refresh_coords

    # Get template path and tile coordinates export path
    template_filename = f"tile_info_{illumination}.j2"
    template_path = Path(project_base_path) / template_filename

    if should_run_coords:
        # Process first slice coordinates (Fiji stitch, coordinate processing, template generation)
        template_path, tile_coords_export = process_first_slice_coordinates(
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
        tiles_data_path = tile_coords_export
    else:
        logger.info(
            f"Skipping coordinate determination for mosaic {mosaic_id}. "
            f"Using template from mosaic {base_mosaic_id} ({illumination} illumination)"
        )
        # Use existing tile coordinates export from base mosaic
        tiles_data_path = base_stitched_path / f"mosaic_{base_mosaic_id:03d}_tile_coords_export.yaml"

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
    mip_tiff = stitched_path / f"mosaic_{mosaic_id:03d}_mip.tif"

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
        tiff_output=str(mip_tiff),
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
        tiles_data_path=tiles_data_path,
        processed_path=processed_path,
        stitched_path=stitched_path,
        mask_path=mask_path,
        scan_resolution_2d=scan_resolution_2d,
    )
    enface_outputs["aip"] = aip_nifti
    enface_outputs["mip"] = mip_nifti
    # Step 7: Stitch all volume modalities asynchronously (using template, no mask)
    volume_futures = {}
    for modality in VOLUME_MODALITIES:
        modality_tile_info = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.yaml"
        modality_nifti = stitched_path / f"mosaic_{mosaic_id:03d}_{modality}.nii"

        # Generate tile_info_file using template
        generate_tile_info_file_task(
            template_path=str(template_path),
            output_path=str(modality_tile_info),
            base_dir=str(processed_path),
            modality=modality,
            mosaic_id=mosaic_id,
            scan_resolution=scan_resolution_3d,
        )

        # Submit stitch task asynchronously (volumes don't need JPEG/TIFF)
        # future = stitch_mosaic2d_task.submit(
        #     tile_info_file=str(modality_tile_info),
        #     out = 
        #     circular_mean=False,
        # )
        # volume_futures[modality] = future

    # Emit event when 2D enface images are finished stitching
    emit_event(
        event="mosaic.enface_stitched",
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
        }
    )

    logger.info(
        f"All enface modalities stitched for mosaic {mosaic_id}. "
        f"Emitted mosaic.enface_stitched event."
    )

    # Wait for all volume stitching to complete
    volume_outputs = {}
    for modality, future in volume_futures.items():
        outputs = future.wait()
        volume_outputs[modality] = outputs

    # Emit event that mosaic stitching is complete
    emit_event(
        event="mosaic.stitched",
        resource={
            "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
        },
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
    Extracts parameters from the event payload and calls process_mosaic_flow.
    
    The payload is passed as JSON from the event, so types should be preserved.
    However, we ensure proper type conversion for safety.
    """
    # Extract and convert types explicitly to ensure correctness
    mosaic_id = int(payload["mosaic_id"])
    grid_size_x = int(payload.get("total_batches") or payload.get("grid_size_x", 14))
    grid_size_y = int(payload.get("grid_size_y", 31))
    tile_overlap = float(payload.get("tile_overlap", 20.0))
    mask_threshold = float(payload.get("mask_threshold", 50.0))

    # Handle scan_resolution_3d - could be list or need default
    scan_resolution_3d = payload.get("scan_resolution_3d")
    if scan_resolution_3d is None:
        scan_resolution_3d = DEFAULT_SCAN_RESOLUTION_3D
    else:
        # Ensure it's a list of floats
        scan_resolution_3d = [float(x) for x in scan_resolution_3d]

    # Handle boolean - could be string "true"/"false" or actual boolean
    force_refresh_coords = payload.get("force_refresh_coords", False)
    if isinstance(force_refresh_coords, str):
        force_refresh_coords = force_refresh_coords.lower() == "true"

    return process_mosaic_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=mosaic_id,
        grid_size_x=grid_size_x,
        grid_size_y=grid_size_y,
        tile_overlap=tile_overlap,
        mask_threshold=mask_threshold,
        scan_resolution_3d=scan_resolution_3d,
        force_refresh_coords=force_refresh_coords,
    )


# Deployment configuration for event-driven triggering
if __name__ == "__main__":
    process_mosaic_event_flow_deployment = process_mosaic_event_flow.to_deployment(
        name="process_mosaic_event_flow",
        tags=["event-driven", "mosaic-processing", "stitching"],
        triggers=[
            DeploymentEventTrigger(
                expect={"mosaic.processed"},
                parameters={
                    "payload": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload | tojson }}",
                        }
                    }
                },
            )
        ],
    )
    