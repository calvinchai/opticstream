"""
Event-driven slice registration flow.

This flow is triggered by the 'slice.ready' event when both mosaics
(normal and tilted) for a slice are stitched. It performs:
1. Thruplane registration (aligns normal and tilted illuminations)
2. 3D axis generation (generates normalized 3D axis vectors and visualizations)

This flow uses the unified thruplane_from_files function that combines
registration and 3D axis generation in a single call.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from prefect import flow, task
from prefect.logging import get_run_logger

from opticstream.events import SLICE_READY, SLICE_REGISTERED, get_event_trigger
from opticstream.events.psoct_event_emitters import emit_slice_psoct_event
from opticstream.state.oct_project_state import OCTSliceId, OCT_STATE_SERVICE
from opticstream.utils.matlab_execution import (
    call_matlab_via_cli,
    get_matlab_engine,
    resolve_matlab_root,
)
from opticstream.utils.utils import get_mosaic_paths


@task
def thruplane_from_files_task(
    fixed_ori_path: str,
    moving_ori_path: str,
    fixed_biref_path: str,
    moving_biref_path: str,
    output_dir: str,
    gamma: float = -15.0,
    matlab_script_path: Optional[str] = None,
) -> Dict[str, Path]:
    """
    Perform thruplane registration and generate 3D axis from NIfTI files.

    This is a Python wrapper for the MATLAB function `thruplane_from_files()`.
    It registers normal and tilted illuminations and generates all outputs including
    3D axis vectors in a single call.

    Parameters
    ----------
    fixed_ori_path : str
        Path to fixed orientation image (.nii or .nii.gz)
    moving_ori_path : str
        Path to moving orientation image (.nii or .nii.gz)
    fixed_biref_path : str
        Path to fixed birefringence image (.nii or .nii.gz)
    moving_biref_path : str
        Path to moving birefringence image (.nii or .nii.gz)
    output_dir : str
        Directory to save output files
    gamma : float
        Tilt angle parameter for registration (default: -15.0)
    matlab_script_path : str, optional
        Path to MATLAB script directory. If None, uses default location
        or searches for thruplane_from_files.m

    Returns
    -------
    Dict[str, Path]
        Dictionary with output file paths:
        - 'data': Path to data.mat file
        - 'inplane_tiff': Path to inplane.tiff
        - 'inplane_jpg': Path to inplane.jpg
        - 'alpha_tiff': Path to alpha.tiff
        - 'alpha_jpg': Path to alpha.jpg
        - 'axis_nii': Path to 3daxis.nii (normalized)
        - 'axis_jpg': Path to 3daxis.jpg

    Notes
    -----
    The MATLAB function signature is:
        thruplane_from_files(fixed_ori_path, moving_ori_path, fixed_biref_path,
                            moving_biref_path, output_dir, gamma)

    All output files are saved to output_dir with simple names.
    """
    logger = get_run_logger()
    logger.info(
        f"Starting thruplane_from_files registration:\n"
        f"  Fixed orientation: {fixed_ori_path}\n"
        f"  Moving orientation: {moving_ori_path}\n"
        f"  Fixed birefringence: {fixed_biref_path}\n"
        f"  Moving birefringence: {moving_biref_path}\n"
        f"  Output directory: {output_dir}\n"
        f"  Gamma: {gamma}"
    )

    # Validate input files exist
    input_files = [
        fixed_ori_path,
        moving_ori_path,
        fixed_biref_path,
        moving_biref_path,
    ]
    for file_path in input_files:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Convert paths to absolute paths for MATLAB
    fixed_ori_abs = str(Path(fixed_ori_path).absolute())
    moving_ori_abs = str(Path(moving_ori_path).absolute())
    fixed_biref_abs = str(Path(fixed_biref_path).absolute())
    moving_biref_abs = str(Path(moving_biref_path).absolute())
    output_dir_abs = str(output_path.absolute())

    try:
        # Get MATLAB engine
        eng = get_matlab_engine()

        # Resolve MATLAB script path (explicit override or psoct_toolbox default)
        resolved_path = resolve_matlab_root(matlab_script_path)
        eng.addpath(resolved_path, nargout=0)
        # Also add registration subdirectory when present
        registration_path = Path(resolved_path) / "registration"
        if registration_path.exists():
            eng.addpath(str(registration_path), nargout=0)
            logger.info(f"Added MATLAB registration path: {registration_path}")

        # Call MATLAB function
        logger.info(
            f"Calling MATLAB thruplane_from_files with:\n"
            f"  fixed_ori: {fixed_ori_abs}\n"
            f"  moving_ori: {moving_ori_abs}\n"
            f"  fixed_biref: {fixed_biref_abs}\n"
            f"  moving_biref: {moving_biref_abs}\n"
            f"  output_dir: {output_dir_abs}\n"
            f"  gamma: {gamma}"
        )

        eng.thruplane_from_files(
            fixed_ori_abs,
            moving_ori_abs,
            fixed_biref_abs,
            moving_biref_abs,
            output_dir_abs,
            float(gamma),
            nargout=0,
        )

        logger.info("thruplane_from_files completed successfully")

        # Close MATLAB engine
        eng.quit()

    except ImportError:
        # Fallback: Try calling MATLAB via command line
        logger.warning("MATLAB Engine not available, attempting command-line call")
        call_matlab_via_cli(
            "thruplane_from_files",
            fixed_ori_abs,
            moving_ori_abs,
            fixed_biref_abs,
            moving_biref_abs,
            output_dir_abs,
            gamma,
            matlab_script_path=matlab_script_path,
        )
    except Exception as e:
        logger.error(f"Error in thruplane_from_files: {e}")
        raise

    # Collect output file paths
    outputs = {
        "data": output_path / "data_data.mat",
        "inplane_tiff": output_path / "data_inplane.tiff",
        "inplane_jpg": output_path / "data_inplane.jpg",
        "alpha_tiff": output_path / "data_alpha.tiff",
        "alpha_jpg": output_path / "data_alpha.jpg",
        "axis_nii": output_path / "3daxis.nii",
        "axis_jpg": output_path / "3daxis.jpg",
    }

    # Verify outputs exist
    for key, path in outputs.items():
        if not path.exists():
            logger.warning(f"Expected output file not found: {path}")

    return outputs


@flow(flow_run_name="{project_name}-slice-{slice_number}-register")
def register_slice_flow(
    project_name: str,
    project_base_path: str,
    slice_number: int,
    normal_mosaic_id: int,
    tilted_mosaic_id: int,
    gamma: float = -15.0,
    matlab_script_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Register a slice after both mosaics are stitched.

    This flow performs registration of normal and tilted illumination mosaics
    to combine orientations and generate 3D axis visualizations using the
    unified thruplane_from_files function.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
    normal_mosaic_id : int
        Normal illumination mosaic ID (2n-1)
    tilted_mosaic_id : int
        Tilted illumination mosaic ID (2n)
    gamma : float
        Tilt angle parameter for registration (default: -15.0)
    matlab_script_path : str, optional
        Optional override for MATLAB script directory. When omitted, the
        default root from psoct_toolbox is used.

    Returns
    -------
    Dict[str, Any]
        Dictionary with registration results and output paths
    """
    logger = get_run_logger()
    logger.info(
        f"Starting slice registration for slice {slice_number} "
        f"(mosaics {normal_mosaic_id} and {tilted_mosaic_id})"
    )

    processed_path_normal, stitched_path_normal, _, _ = get_mosaic_paths(
        project_base_path, normal_mosaic_id
    )
    processed_path_tilted, stitched_path_tilted, _, _ = get_mosaic_paths(
        project_base_path, tilted_mosaic_id
    )
    # Construct input file paths (stitched mosaics)
    fixed_ori_path = stitched_path_normal / f"mosaic_{normal_mosaic_id:03d}_ori.nii.gz"
    moving_ori_path = stitched_path_tilted / f"mosaic_{tilted_mosaic_id:03d}_ori.nii.gz"
    fixed_biref_path = (
        stitched_path_normal / f"mosaic_{normal_mosaic_id:03d}_biref.nii.gz"
    )
    moving_biref_path = (
        stitched_path_tilted / f"mosaic_{tilted_mosaic_id:03d}_biref.nii.gz"
    )

    # Check if input files exist
    input_files = [fixed_ori_path, moving_ori_path, fixed_biref_path, moving_biref_path]
    missing_files = [f for f in input_files if not f.exists()]
    if missing_files:
        raise FileNotFoundError(
            f"Missing input files for slice {slice_number}:\n"
            + "\n".join(f"  - {f}" for f in missing_files)
        )

    logger.info(
        f"Input files:\n"
        f"  Fixed orientation: {fixed_ori_path}\n"
        f"  Moving orientation: {moving_ori_path}\n"
        f"  Fixed birefringence: {fixed_biref_path}\n"
        f"  Moving birefringence: {moving_biref_path}"
    )

    # Run unified registration and 3D axis generation
    outputs = thruplane_from_files_task(
        fixed_ori_path=str(fixed_ori_path),
        moving_ori_path=str(moving_ori_path),
        fixed_biref_path=str(fixed_biref_path),
        moving_biref_path=str(moving_biref_path),
        output_dir=str(stitched_path_normal),
        gamma=gamma,
        matlab_script_path=matlab_script_path,
    )

    # Emit event that slice registration is complete
    emit_slice_psoct_event(
        SLICE_REGISTERED,
        slice_ident=(
            OCTSliceId(project_name=project_name, slice_number=slice_number)
        ),
        extra_payload={
            "normal_mosaic_id": normal_mosaic_id,
            "tilted_mosaic_id": tilted_mosaic_id,
            "processed_dir": str(stitched_path_normal),
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    # Update OCT project state for this slice
    with OCT_STATE_SERVICE.open_slice_by_parts(
        project_name=project_name,
        slice_number=slice_number,
    ) as slice_state:
        slice_state.mark_completed()
        slice_state.set_registered(True)

    logger.info(f"Slice {slice_number} registration complete")

    return {
        "slice_number": slice_number,
        "normal_mosaic_id": normal_mosaic_id,
        "tilted_mosaic_id": tilted_mosaic_id,
        "processed_dir": str(stitched_path_normal),
        "outputs": outputs,
    }


@flow
def register_slice_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of slice registration.

    Triggered by the 'linc.oct.slice.ready' event when both mosaics are stitched.

    Parameters
    ----------
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - slice_number: int
        - normal_mosaic_id: int
        - tilted_mosaic_id: int
        - gamma: float (optional, default: -15.0)
        - matlab_script_path: str (optional, default: psoct-renew path)

    Returns
    -------
    Dict[str, Any]
        Result from register_slice_flow
    """
    # Extract and convert types explicitly
    slice_number = int(payload["slice_number"])
    normal_mosaic_id = int(payload["normal_mosaic_id"])
    tilted_mosaic_id = int(payload["tilted_mosaic_id"])
    gamma = float(payload.get("gamma", -15.0))
    matlab_script_path = payload.get("matlab_script_path")

    return register_slice_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        slice_number=slice_number,
        normal_mosaic_id=normal_mosaic_id,
        tilted_mosaic_id=tilted_mosaic_id,
        gamma=gamma,
        matlab_script_path=matlab_script_path,
    )


# Deployment configuration for event-driven triggering
if __name__ == "__main__":
    register_slice_event_flow_deployment = register_slice_event_flow.to_deployment(
        name="register_slice_event_flow",
        tags=["event-driven", "slice-registration"],
        triggers=[
            get_event_trigger(SLICE_READY),
        ],
    )
