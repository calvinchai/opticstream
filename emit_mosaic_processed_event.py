"""
Python snippet to emit the 'mosaic.processed' event for running the mosaic.

This event triggers the process_mosaic_event_flow which handles:
- Coordinate determination (Fiji stitch + process_tile_coord)
- Template generation (once, using Jinja2)
- Stitching for all modalities (using template)
"""

from prefect.events import emit_event


def emit_mosaic_processed_event(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    total_batches: int,
    grid_size_y: int = None,
    tile_overlap: float = None,
    mask_threshold: float = None,
    scan_resolution_3d: list = None,
    force_refresh_coords: bool = None,
    triggered_by: str = None,
):
    """
    Emit the 'mosaic.processed' event to trigger mosaic processing flow.
    
    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    total_batches : int
        Total number of batches in the mosaic (used as grid_size_x)
    grid_size_y : int, optional
        Number of rows (tiles per batch) in mosaic. Default: 31
    tile_overlap : float, optional
        Overlap between tiles in pixels. Default: 20.0
    mask_threshold : float, optional
        Threshold for mask generation and coordinate processing. Default: 50.0
    scan_resolution_3d : list, optional
        Scan resolution for 3D volumes [x, y, z]. Default: [0.01, 0.01, 0.0025]
    force_refresh_coords : bool, optional
        Force coordinate determination even if not first slice. Default: False
    triggered_by : str, optional
        Optional identifier for what triggered this event (e.g.,
        "state_management_flow")
    """
    payload = {
        "project_name": project_name,
        "project_base_path": project_base_path,
        "mosaic_id": mosaic_id,
        "grid_size_x": total_batches,
        "grid_size_y": grid_size_y,
        "tile_overlap": tile_overlap,
        "mask_threshold": mask_threshold,
        "scan_resolution_3d": scan_resolution_3d,
        "force_refresh_coords": force_refresh_coords
    }

    # Add optional fields if provided
    if grid_size_y is not None:
        payload["grid_size_y"] = grid_size_y
    if tile_overlap is not None:
        payload["tile_overlap"] = tile_overlap
    if mask_threshold is not None:
        payload["mask_threshold"] = mask_threshold
    if scan_resolution_3d is not None:
        payload["scan_resolution_3d"] = scan_resolution_3d
    if force_refresh_coords is not None:
        payload["force_refresh_coords"] = force_refresh_coords
    if triggered_by:
        payload["triggered_by"] = triggered_by

    emit_event(
        event="mosaic.processed",
        resource={
            "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
        },
        payload=payload,
    )

    print(
        f"Emitted mosaic.processed event for mosaic {mosaic_id} in project {project_name}")


# Example usage:
if __name__ == "__main__":
    # Example: Emit event for mosaic 1 with 14 batches
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=1,
    #     total_batches=21,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=2,
    #     total_batches=35,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=9,
    #     total_batches=21,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=7,
    #     total_batches=21,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=5,
    #     total_batches=21,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=3,
    #     total_batches=21,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=10,
    #     total_batches=35,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=8,
    #     total_batches=35,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=6,
    #     total_batches=35,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=4,
    #     total_batches=35,
    #     grid_size_y=28,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=11,
    #     total_batches=21,
    #     grid_size_y=27,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=True,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=12,
    #     total_batches=35,
    #     grid_size_y=27,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=True,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=13,
    #     total_batches=21,
    #     grid_size_y=27,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    # emit_mosaic_processed_event(
    #     project_name="sub-I80_voi-slab2",
    #     project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
    #     mosaic_id=14,
    #     total_batches=35,
    #     grid_size_y=27,
    #     tile_overlap=20.0,
    #     mask_threshold=50.0,
    #     scan_resolution_3d=[0.01, 0.01, 0.0025],
    #     force_refresh_coords=False,
    #     triggered_by="manual_trigger",
    # )
    emit_mosaic_processed_event(
        project_name="sub-I80_voi-slab2",
        project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
        mosaic_id=15,
        total_batches=21,
        grid_size_y=27,
        tile_overlap=20.0,
        mask_threshold=50.0,
        scan_resolution_3d=[0.01, 0.01, 0.0025],
        force_refresh_coords=False,
        triggered_by="manual_trigger",
    )
    emit_mosaic_processed_event(
        project_name="sub-I80_voi-slab2",
        project_base_path="/space/zircon/5/users/data/sub-I80_voi-slab2/",
        mosaic_id=16,
        total_batches=35,
        grid_size_y=27,
        tile_overlap=20.0,
        mask_threshold=50.0,
        scan_resolution_3d=[0.01, 0.01, 0.0025],
        force_refresh_coords=False,
        triggered_by="manual_trigger",
    )