"""
Python snippet to emit the 'slice.ready' event for triggering slice registration.

This event triggers the register_slice_event_flow which handles:
- Thruplane registration (aligns normal and tilted illuminations)
- RGB_3Daxis visualization (generates 3D axis images)

The event should be emitted after both mosaics (normal and tilted) for a slice
have been stitched.
"""

from prefect.events import emit_event


def emit_slice_ready_event(
    project_name: str,
    project_base_path: str,
    slice_number: int,
    normal_mosaic_id: int,
    tilted_mosaic_id: int,
    gamma: float = -15.0,
    mask_file: str = "",
    mask_threshold: float = 55.0,
    triggered_by: str = None,
):
    """
    Emit the 'slice.ready' event to trigger slice registration flow.
    
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
    gamma : float, optional
        Tilt angle parameter for registration. Default: -15.0
    mask_file : str, optional
        Mask file type ("aip", "mip", or "" for no mask). Default: ""
    mask_threshold : float, optional
        Threshold for mask generation. Default: 55.0
    triggered_by : str, optional
        Optional identifier for what triggered this event (e.g., "mosaic_stitching_complete")
    """
    payload = {
        "project_name": project_name,
        "project_base_path": project_base_path,
        "slice_number": slice_number,
        "normal_mosaic_id": normal_mosaic_id,
        "tilted_mosaic_id": tilted_mosaic_id,
        "gamma": gamma,
        "mask_file": mask_file,
        "mask_threshold": mask_threshold,
    }
    
    if triggered_by:
        payload["triggered_by"] = triggered_by
    
    emit_event(
        event="slice.ready",
        resource={
            "prefect.resource.id": f"slice:{project_name}:slice-{slice_number}",
            "project_name": project_name,
            "slice_number": str(slice_number),
        },
        payload=payload,
    )
    
    print(
        f"Emitted slice.ready event for slice {slice_number} "
        f"(mosaics {normal_mosaic_id} and {tilted_mosaic_id}) "
        f"in project {project_name}"
    )


# Example usage: Emit events for slices 1 through 5
if __name__ == "__main__":
    # Configuration - update these values for your project
    project_name = "sub-I80_voi-slab2"
    project_base_path = "/space/zircon/5/users/data/sub-I80_voi-slab2/"
    
    # Default parameters
    gamma = -15.0
    mask_file = ""
    mask_threshold = 50.0
    
    # Emit events for slices 1-5
    # Slice 1: mosaics 1 (normal) and 2 (tilted)
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=1,
    #     normal_mosaic_id=1,
    #     tilted_mosaic_id=2,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    
    # # Slice 2: mosaics 3 (normal) and 4 (tilted)
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=2,
    #     normal_mosaic_id=3,
    #     tilted_mosaic_id=4,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    
    # # Slice 3: mosaics 5 (normal) and 6 (tilted)
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=3,
    #     normal_mosaic_id=5,
    #     tilted_mosaic_id=6,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    
    # # Slice 4: mosaics 7 (normal) and 8 (tilted)
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=4,
    #     normal_mosaic_id=7,
    #     tilted_mosaic_id=8,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    
    # # Slice 5: mosaics 9 (normal) and 10 (tilted)
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=5,
    #     normal_mosaic_id=9,
    #     tilted_mosaic_id=10,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=6,
    #     normal_mosaic_id=11,
    #     tilted_mosaic_id=12,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    # emit_slice_ready_event(
    #     project_name=project_name,
    #     project_base_path=project_base_path,
    #     slice_number=7,
    #     normal_mosaic_id=13,
    #     tilted_mosaic_id=14,
    #     gamma=gamma,
    #     mask_file=mask_file,
    #     mask_threshold=mask_threshold,
    #     triggered_by="manual_trigger",
    # )
    emit_slice_ready_event(
        project_name=project_name,
        project_base_path=project_base_path,
        slice_number=8,
        normal_mosaic_id=15,
        tilted_mosaic_id=16,
        gamma=gamma,
        mask_file=mask_file,
        mask_threshold=mask_threshold,
        triggered_by="manual_trigger",
    )
    
    print("\nAll slice.ready events emitted successfully!")
