# Events Documentation

This document lists all events emitted in the oct-pipe project and their payload schemas.

## Event List

### 1. `tile_batch.complex2processed.ready`

**Emitted by:** `workflow/flows/tile_batch_flow.py` - `spectral_to_complex_batch_task`

**When:** After a batch of tiles has been converted from spectral to complex format.

**Resource:**
```python
{
    "prefect.resource.id": f"batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}",
    "project_name": project_name,
    "mosaic_id": str(mosaic_id),
    "batch_id": str(batch_id),
}
```

**Payload Schema:**
```python
{
    "project_name": str,          # Project identifier
    "project_base_path": str,      # Base path for the project
    "mosaic_id": int,             # Mosaic identifier
    "batch_id": int,              # Batch identifier
}
```

**Triggers:** `complex_to_processed_batch_event_flow` (in `tile_batch_flow.py`)

---

### 2. `tile_batch.archived.ready`

**Emitted by:** `workflow/flows/tile_batch_flow.py` - `process_tile_batch_flow`

**When:** After a batch of tiles has been archived.

**Resource:**
```python
{
    "prefect.resource.id": f"batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}",
    "project_name": project_name,
    "mosaic_id": str(mosaic_id),
    "batch_id": str(batch_id),
}
```

**Payload Schema:**
```python
{
    "project_name": str,          # Project identifier
    "project_base_path": str,      # Base path for the project
    "mosaic_id": int,             # Mosaic identifier
    "batch_id": int,              # Batch identifier
}
```

**Triggers:** `manage_mosaic_batch_state_event_flow` (in `state_management_flow.py`)

---

### 3. `tile_batch.upload_to_linc.ready`

**Emitted by:** `workflow/flows/tile_batch_flow.py` - `archive_tile_batch_task`

**When:** After a batch of tiles has been archived and is ready for upload to LINC.

**Resource:**
```python
{
    "prefect.resource.id": "{{ event.resource.id }}",  # Inherits from previous event
}
```

**Payload Schema:**
```python
{
    "project_name": str,              # Project identifier
    "project_base_path": str,         # Base path for the project
    "mosaic_id": int,                 # Mosaic identifier
    "batch_id": int,                  # Batch identifier
    "archived_file_paths": List[str], # List of paths to archived files
}
```

**Triggers:** `upload_to_linc_batch_event_flow` (in `upload_flow.py`)

---

### 4. `mosaic.processed`

**Emitted by:** 
- `workflow/flows/state_management_flow.py` - `manage_mosaic_batch_state_flow` (when all batches are complete)
- `workflow/flows/tile_batch_flow.py` - `complex_to_processed_batch_event_flow` (when all batches are processed)

**When:** When all batches in a mosaic have been processed.

**Resource:**
```python
{
    "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
    "project_name": project_name,
    "mosaic_id": str(mosaic_id),
}
```

**Payload Schema:**
```python
{
    "project_name": str,          # Project identifier
    "project_base_path": str,      # Base path for the project
    "mosaic_id": int,             # Mosaic identifier
    "total_batches": int,         # Total number of batches in the mosaic
    "triggered_by": str,          # Optional: "state_management_flow" or omitted
}
```

**Triggers:** `process_mosaic_event_flow` (in `mosaic_processing_flow.py`)

---

### 5. `mosaic.enface_stitched`

**Emitted by:** `workflow/flows/mosaic_processing_flow.py` - `process_mosaic_flow`

**When:** After all 2D enface modalities (ret, ori, biref, mip, surf) have been stitched.

**Resource:**
```python
{
    "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
    "project_name": project_name,
    "mosaic_id": str(mosaic_id),
}
```

**Payload Schema:**
```python
{
    "project_name": str,                    # Project identifier
    "project_base_path": str,                # Base path for the project
    "mosaic_id": int,                       # Mosaic identifier
    "enface_outputs": Dict[str, Dict[str, str]],  # Mapping of modality to output paths
    # Example enface_outputs structure:
    # {
    #     "ret": {
    #         "nifti": "/path/to/mosaic_001_ret.nii",
    #         "jpeg": "/path/to/mosaic_001_ret.jpeg",
    #         "tiff": "/path/to/mosaic_001_ret.tif"
    #     },
    #     "ori": { ... },
    #     "biref": { ... },
    #     "mip": { ... },
    #     "surf": { ... }
    # }
}
```

**Triggers:** `slack_enface_notification_flow` (in `slack_notification_flow.py`)

---

### 6. `mosaic.stitched`

**Emitted by:** `workflow/flows/mosaic_processing_flow.py` - `process_mosaic_flow`

**When:** After all modalities (both enface and volume) have been stitched for a mosaic.

**Resource:**
```python
{
    "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
    "project_name": project_name,
    "mosaic_id": str(mosaic_id),
}
```

**Payload Schema:**
```python
{
    "project_name": str,                    # Project identifier
    "project_base_path": str,                # Base path for the project
    "mosaic_id": int,                       # Mosaic identifier
    "aip_path": str,                        # Path to AIP (Average Intensity Projection) NIfTI file
    "mask_path": str,                       # Path to mask NIfTI file
    "enface_outputs": Dict[str, Dict[str, str]],  # Mapping of enface modality to output paths
    # Same structure as mosaic.enface_stitched event
    "volume_outputs": Dict[str, Dict[str, str]],  # Mapping of volume modality to output paths
    # Example volume_outputs structure:
    # {
    #     "dBI": {
    #         "nifti": "/path/to/mosaic_001_dBI.nii"
    #     },
    #     "R3D": {
    #         "nifti": "/path/to/mosaic_001_R3D.nii"
    #     },
    #     "O3D": {
    #         "nifti": "/path/to/mosaic_001_O3D.nii"
    #     }
    # }
}
```

**Triggers:** `manage_slice_state_event_flow` (in `state_management_flow.py`)

---

### 7. `slice.ready`

**Emitted by:** `workflow/flows/state_management_flow.py` - `manage_slice_state_flow`

**When:** When both mosaics (normal and tilted) in a slice have been stitched.

**Resource:**
```python
{
    "prefect.resource.id": f"slice:{project_name}:slice-{slice_number}",
    "project_name": project_name,
    "slice_number": str(slice_number),
}
```

**Payload Schema:**
```python
{
    "project_name": str,          # Project identifier
    "project_base_path": str,      # Base path for the project
    "slice_number": int,          # Slice number (1-indexed)
    "normal_mosaic_id": int,      # Mosaic ID for normal illumination (odd: 1, 3, 5, ...)
    "tilted_mosaic_id": int,       # Mosaic ID for tilted illumination (even: 2, 4, 6, ...)
    "triggered_by": str,          # "state_management_flow"
}
```

**Triggers:** None currently configured (can be used for downstream slice-level processing)

---

## Legacy Events (workflow_old)

The following events are defined in the legacy `workflow_old` directory and may not be actively used:

### 8. `spectral.file.created`

**Emitted by:** `workflow_old/spectral_watch.py` - File system watcher

**When:** When a new spectral file is detected in the watched directory.

**Payload Schema:**
```python
{
    "file_path": str,           # Full path to the created file
    "file_pattern": str,        # File pattern that matched
}
```

### 9. `spectral.file.processed`

**Emitted by:** `workflow_old/spectral_watch.py` - File processing handler

**When:** After a spectral file has been processed.

**Payload Schema:**
```python
{
    "gz_path": str,             # Path to compressed file
    "processed_path": str,      # Path to processed file
}
```

---

## Event Flow Diagram

```
tile_batch.complex2processed.ready
    └─> complex_to_processed_batch_event_flow
            └─> (if all batches processed) mosaic.processed

tile_batch.archived.ready
    └─> manage_mosaic_batch_state_event_flow
            └─> (if all batches complete) mosaic.processed

tile_batch.upload_to_linc.ready
    └─> upload_to_linc_batch_event_flow

mosaic.processed
    └─> process_mosaic_event_flow
            ├─> mosaic.enface_stitched
            │       └─> slack_enface_notification_flow
            └─> mosaic.stitched
                    └─> manage_slice_state_event_flow
                            └─> (if both mosaics stitched) slice.ready
```

---

## Notes

1. **Resource IDs:** All events include a `prefect.resource.id` field that follows a hierarchical pattern:
   - Batch: `batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}`
   - Mosaic: `mosaic:{project_name}:mosaic-{mosaic_id}`
   - Slice: `slice:{project_name}:slice-{slice_number}`

2. **Mosaic ID Convention:**
   - Normal illumination: odd mosaic IDs (1, 3, 5, ...)
   - Tilted illumination: even mosaic IDs (2, 4, 6, ...)
   - Slice number: `(mosaic_id + 1) // 2` for normal, `mosaic_id // 2` for tilted

3. **Event Timing:**
   - `tile_batch.complex2processed.ready` and `tile_batch.archived.ready` are emitted in parallel
   - `mosaic.enface_stitched` is emitted before `mosaic.stitched` (enface stitching completes before volume stitching)

4. **Type Safety:**
   - All numeric IDs (`mosaic_id`, `batch_id`, `slice_number`) are integers in the payload
   - Resource IDs use string representations for consistency with Prefect conventions

