"""
Event-driven volume stitching flow.

This flow is triggered by the 'mosaic.stitched' event after enface modalities
are stitched. It handles:
1. Focus finding for first slice (if needed)
2. 3D volume stitching for all volume modalities
3. Emits MOSAIC_VOLUME_STITCHED event
"""

from pathlib import Path
from typing import Any, Dict

from prefect import flow, task
from prefect.logging import get_run_logger

from opticstream.config.psoct_scan_config import PSOCTScanConfigModel
from opticstream.scripts import find_tile_plane, find_volume_surface
from opticstream.scripts.filter_tiles_by_signal import filter_tiles_by_signal
from opticstream.events import MOSAIC_VOLUME_STITCHED
from opticstream.events.psoct_event_emitters import emit_mosaic_psoct_event
from opticstream.flows.psoct.mosaic_process_flow import generate_tile_info_file_task
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.flows.psoct.utils import (
    load_scan_config_for_payload,
    mosaic_ident_from_payload,
    normalize_float_sequence,
)
from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTMosaicId
from opticstream.utils.utils import (
    get_dandi_slice_path,
    get_modality_stitching_filename,
    get_mosaic_paths,
    mosaic_id_to_slice_id,
)


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
    zarr_config: optional Zarr configuration object
        Zarr configuration object
    kwargs: Dict[str, Any], optional
        Additional keyword arguments for mosaic3d
    """

    logger = get_run_logger()
    logger.info(f"Stitching mosaic 3D from {tile_info_file}")

    from linc_convert.modalities.psoct.mosaic import mosaic2d
    # Prepare arguments for mosaic2d
    mosaic_kwargs = (kwargs or {}).copy()
    if zarr_config is not None:
        mosaic_kwargs["zarr_config"] = zarr_config

    mosaic2d(tile_info_file=tile_info_file, out=output_path, **mosaic_kwargs)
    logger.info(f"Stitched mosaic 3D saved to {output_path}")
    return output_path


@task(task_run_name="mosaic-{mosaic_id}-find-focus-plane-{illumination}")
def find_focus_plane_task(
    project_base_path: str,
    mosaic_id: int,
    illumination: str,
    signal_threshold: float = 60,
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
    project_base_path = Path(project_base_path)
    input_yaml = get_modality_stitching_filename(project_base_path, mosaic_id, "dBI")
    if not input_yaml.exists():
        raise FileNotFoundError(f"dBI stitching file not found: {input_yaml}")
    output_yaml = project_base_path / "focus_finding" / f"filtered_{illumination}.yaml"
    focus_output_path = Path(project_base_path) / f"focus-{illumination}.nii"
    focus_output_path.parent.mkdir(parents=True, exist_ok=True)
    # find_tile_region(
    #     input_yaml=str(input_yaml),
    #     output_yaml=str(output_yaml),
    #     signal_threshold=signal_threshold,
    #     grid_size=grid_size,
    # )
    filter_tiles_by_signal(input_yaml=str(input_yaml), output_yaml=str(output_yaml), signal_threshold=signal_threshold)
    find_volume_surface.batch(
        yaml_path=str(output_yaml),
        output_dir=str(project_base_path / "focus_finding"),
        output_yaml=str(
            project_base_path / "focus_finding" / f"surface_{illumination}.yaml"
        ),
        postfix_old="_dBI",
        postfix_new="_surf",
    )
    find_tile_plane.main(
        yaml_path=str(
            project_base_path / "focus_finding" / f"surface_{illumination}.yaml"
        ),
        output=str(focus_output_path),
        base_dir=str(project_base_path / "focus_finding"),
        subsample=1,
        avg_signal_threshold=signal_threshold,
        plot=str(project_base_path / "focus_finding" / f"plane_{illumination}.png"),
        outlier_method="iqr",
        outlier_iqr_factor=1.5,
        outlier_z_threshold=3.0,
        output_corrected_dir=None,
        crop_x=15,
        normalize_min=0,
        degree=2,
    )

    return str(focus_output_path)


@flow(flow_run_name="stitch-volume-{mosaic_ident}")
def stitch_volume_flow(
    mosaic_ident: OCTMosaicId,
    config: PSOCTScanConfigModel,
    *,
    apply_mask: bool = False,
    force_refresh_focus: bool = False,
    force_rerun: bool = False,
) -> Dict[str, Path]:
    """
    Flow to stitch 3D volume modalities, triggered by MOSAIC_ENFACE_STITCHED event.

    This flow handles:
    1. Focus finding for first slice (if needed)
    2. 3D volume stitching for all volume modalities
    3. Emits MOSAIC_VOLUME_STITCHED event

    Parameters
    ----------
    mosaic_ident : OCTMosaicId
        Mosaic identifier
    config : PSOCTScanConfigModel
        Configuration object
    apply_mask : bool, optional
        Apply mask to the volume
    force_refresh_focus : bool, optional
        Force refresh focus
    force_rerun : bool, optional
        Force rerun

    Returns
    -------
    Dict[str, Path]
        Dictionary mapping modality to output file paths
    """
    logger = get_run_logger()

    project_name = mosaic_ident.project_name
    mosaic_id = mosaic_ident.mosaic_id
    project_base_path = str(config.project_base_path)
    dandiset_path = str(config.dandiset_path) if config.dandiset_path else None
    mosaic_volume_format = config.mosaic_volume_format
    scan_resolution_3d = normalize_float_sequence(config.acquisition.scan_resolution_3d)
    volume_modalities = [m.value for m in config.volume_modalities]
    zarr_config = config.zarr_config
    crop_focus_plane_depth = config.crop_focus_plane_depth
    crop_focus_plane_offset = config.crop_focus_plane_offset

    # Determine if this is normal or tilted illumination
    is_tilted = mosaic_id % 2 == 0
    illumination = "tilted" if is_tilted else "normal"

    # Determine base mosaic ID for this illumination type
    base_mosaic_id = 2 if is_tilted else 1

    # Check if this is first slice
    run_focus_finding = (mosaic_id <= 2) or force_refresh_focus

    # Get paths
    processed_path, stitched_path, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    # Get template path
    template_filename = f"tile_info_{illumination}.j2"
    template_path = Path(project_base_path) / template_filename

    if not template_path.exists():
        raise FileNotFoundError(
            f"Template not found: {template_path}. "
            f"Coordinate determination must be run for mosaic {base_mosaic_id} first."
        )

    # Get mask path
    mask_path = stitched_path / f"mosaic_{mosaic_id:03d}_mask.nii.gz"
    focus_path = Path(project_base_path) / f"focus-{illumination}.nii"
    if run_focus_finding:
        logger.info(
            f"Finding focus plane for first slice (mosaic {mosaic_id}, {illumination} illumination)"
        )
        find_focus_plane_task(
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            illumination=illumination,
        )
        logger.info(
            f"Focus plane determined for {illumination} illumination: {focus_path}"
        )
    if not focus_path.exists():
        logger.warning(
            f"Focus plane not found for {illumination} illumination. "
            f"Using None for focus plane."
        )
        focus_path = None

    # Step 2: Stitch volume modalities
    logger.info(f"Stitching volume modalities for mosaic {mosaic_id}")
    volume_futures = {}
    volume_outputs = {}
    slice_id = mosaic_id_to_slice_id(mosaic_id)
    acq = "tilted" if mosaic_id % 2 == 0 else "normal"

    # Determine output directory: use DANDI if configured, otherwise use processed_path
    if dandiset_path and mosaic_volume_format:
        dandi_slice_path = get_dandi_slice_path(dandiset_path, slice_id)
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

    # Prepare stitching kwargs with focus plane
    stitching_kwargs = {
        "overwrite": True,
        "focus_plane": str(focus_path) if focus_path else None,
        "mask": str(mask_path) if apply_mask else None,
        "normalize_focus_plane": True,
        "crop_focus_plane_depth": crop_focus_plane_depth,
        "crop_focus_plane_offset": crop_focus_plane_offset,
        "voxel_size_xyz": scan_resolution_3d,
        "nii": False,
    }

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
                slice_id=slice_id,
                acq=acq,
                modality=modality,
            )
        else:
            # Fallback to default format
            filename = f"{project_name}_sample-slice{slice_id:02d}_acq-{acq}_proc-{modality}_OCT.ome.zarr"

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
    emit_mosaic_psoct_event(
        MOSAIC_VOLUME_STITCHED,
        mosaic_ident,
        extra_payload={
            "volume_outputs": {mod: str(path) for mod, path in volume_outputs.items()},
        },
    )

    # Update OCT project state for this mosaic
    with OCT_STATE_SERVICE.open_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    ) as mosaic_state:
        mosaic_state.set_volume_stitched(True)

    return volume_outputs


@flow
def stitch_volume_event_flow(payload: Dict[str, Any]) -> Dict[str, Path]:
    """Event wrapper for :func:`stitch_volume_flow` (expects ``mosaic_ident`` in payload)."""
    mosaic_ident = mosaic_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(payload)
    return stitch_volume_flow(
        mosaic_ident,
        cfg,
        apply_mask=bool(payload.get("apply_mask", False)),
        force_refresh_focus=bool(payload.get("force_refresh_focus", False)),
        force_rerun=force_rerun_from_payload(payload),
    )

