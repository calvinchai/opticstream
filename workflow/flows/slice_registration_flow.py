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
from typing import Any, Dict

from prefect import flow
from prefect.events import emit_event
from prefect.logging import get_run_logger

from workflow.events import SLICE_READY, SLICE_REGISTERED, get_event_trigger
from workflow.tasks.slice_registration import thruplane_from_files_task
from workflow.tasks.utils import get_slice_paths


@flow(name="register_slice_flow")
def register_slice_flow(
    project_name: str,
    project_base_path: str,
    slice_number: int,
    normal_mosaic_id: int,
    tilted_mosaic_id: int,
    gamma: float = -15.0,
    matlab_script_path: str = "/space/megaera/1/users/kchai/code/psoct-renew",
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
    matlab_script_path : str
        Path to MATLAB script directory (default: psoct-renew path)

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

    # Get slice paths
    processed_path, stitched_path, _, _ = get_slice_paths(
        project_base_path, slice_number
    )

    # Construct input file paths (stitched mosaics)
    fixed_ori_path = stitched_path / f"mosaic_{normal_mosaic_id:03d}_ori.nii"
    moving_ori_path = stitched_path / f"mosaic_{tilted_mosaic_id:03d}_ori.nii"
    fixed_biref_path = stitched_path / f"mosaic_{normal_mosaic_id:03d}_biref.nii"
    moving_biref_path = stitched_path / f"mosaic_{tilted_mosaic_id:03d}_biref.nii"

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
        output_dir=str(processed_path),
        gamma=gamma,
        matlab_script_path=matlab_script_path,
    )

    # Emit event that slice registration is complete
    emit_event(
        event=SLICE_REGISTERED,
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "slice_number": slice_number,
            "normal_mosaic_id": normal_mosaic_id,
            "tilted_mosaic_id": tilted_mosaic_id,
            "processed_dir": str(processed_path),
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    logger.info(f"Slice {slice_number} registration complete")

    return {
        "slice_number": slice_number,
        "normal_mosaic_id": normal_mosaic_id,
        "tilted_mosaic_id": tilted_mosaic_id,
        "processed_dir": str(processed_path),
        "outputs": outputs,
    }


@flow(name="register_slice_event_flow")
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
    matlab_script_path = payload.get(
        "matlab_script_path",
        "/space/megaera/1/users/kchai/code/psoct-renew",
    )

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
