"""
Event-driven slice registration flow.

This flow is triggered by the 'slice.ready' event when both mosaics
(normal and tilted) for a slice are stitched. It performs:
1. Thruplane registration (aligns normal and tilted illuminations)
2. RGB_3Daxis visualization (generates 3D axis images)
"""

from typing import Any, Dict

from prefect import flow
from prefect.events import DeploymentEventTrigger, emit_event
from prefect.logging import get_run_logger

from workflow.tasks.slice_registration import (rgb_3daxis_task,
                                               thruplane_registration_task)


@flow(name="register_slice_flow")
def register_slice_flow(
    project_name: str,
    project_base_path: str,
    slice_number: int,
    normal_mosaic_id: int,
    tilted_mosaic_id: int,
    gamma: float = -15.0,
    mask_file: str = "",
    mask_threshold: float = 55.0,
) -> Dict[str, Any]:
    """
    Register a slice after both mosaics are stitched.
    
    This flow performs registration of normal and tilted illumination mosaics
    to combine orientations and generate 3D axis visualizations.
    
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
    mask_file : str
        Mask file type ("aip", "mip", or "" for no mask)
    mask_threshold : float
        Threshold for mask generation (default: 55.0)
        
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

    # Step 1: Run thruplane registration
    processed_dir = thruplane_registration_task(
        project_base_path=project_base_path,
        slice_number=slice_number,
        normal_mosaic_id=normal_mosaic_id,
        tilted_mosaic_id=tilted_mosaic_id,
        gamma=gamma,
        mask_file=mask_file,
        mask_threshold=mask_threshold,
        matlab_script_path="/space/megaera/1/users/kchai/code/psoct-renew",
    )

    # Step 2: Run RGB_3Daxis visualization
    axis_outputs = rgb_3daxis_task(
        processed_dir=processed_dir,
        slice_number=slice_number,
        matlab_script_path="/space/megaera/1/users/kchai/code/psoct-renew",
    )

    # Emit event that slice registration is complete
    emit_event(
        event="slice.registered",
        resource={
            "prefect.resource.id": f"slice:{project_name}:slice-{slice_number}",
            "project_name": project_name,
            "slice_number": str(slice_number),
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "slice_number": slice_number,
            "normal_mosaic_id": normal_mosaic_id,
            "tilted_mosaic_id": tilted_mosaic_id,
            "processed_dir": str(processed_dir),
            "axis_outputs": {k: str(v) for k, v in axis_outputs.items()},
        }
    )

    logger.info(f"Slice {slice_number} registration complete")

    return {
        "slice_number": slice_number,
        "normal_mosaic_id": normal_mosaic_id,
        "tilted_mosaic_id": tilted_mosaic_id,
        "processed_dir": str(processed_dir),
        "axis_outputs": axis_outputs,
    }


@flow(name="register_slice_event_flow")
def register_slice_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of slice registration.
    
    Triggered by the 'slice.ready' event when both mosaics are stitched.
    
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
        - mask_file: str (optional, default: "")
        - mask_threshold: float (optional, default: 55.0)
        
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
    mask_file = payload.get("mask_file", "")
    mask_threshold = float(payload.get("mask_threshold", 55.0))

    return register_slice_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        slice_number=slice_number,
        normal_mosaic_id=normal_mosaic_id,
        tilted_mosaic_id=tilted_mosaic_id,
        gamma=gamma,
        mask_file=mask_file,
        mask_threshold=mask_threshold,
    )


# Deployment configuration for event-driven triggering
if __name__ == "__main__":
    register_slice_event_flow_deployment = register_slice_event_flow.to_deployment(
        name="register_slice_event_flow",
        tags=["event-driven", "slice-registration"],
        triggers=[
            DeploymentEventTrigger(
                expect={"slice.ready"},
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
