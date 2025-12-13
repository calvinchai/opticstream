# State Management Flows

This document describes the event-driven state management system for tracking processing progress and updating Prefect Artifacts.

## Overview

The state management system consists of:
1. **Batch State Management**: Monitors batch completion within a mosaic
2. **Slice State Management**: Monitors both mosaics within a slice
3. **Artifact Updates**: Creates/updates Prefect Artifacts with progress information

## Architecture

### Event-Driven Design

State management flows are triggered by events, not called as subflows:

- **Batch State Flow**: Triggered by `tile_batch.complex2processed.ready` and `tile_batch.archived.ready`
- **Slice State Flow**: Triggered by `mosaic.stitched`

### State Tracking

State is tracked via:
1. **Flag Files**: Source of truth for batch/mosaic completion
   - Location: `{project_base_path}/mosaic-{mosaic_id}/state/`
   - Files: `batch-{batch_id}.started`, `batch-{batch_id}.archived`, `batch-{batch_id}.processed`, `batch-{batch_id}.uploaded`
2. **Prefect Artifacts**: Human-readable progress reports
   - Updated automatically as batches complete
   - Visible in Prefect UI

## Flows

### 1. Mosaic Batch State Management Flow

**Flow Name**: `manage_mosaic_batch_state_flow`

**Purpose**: Monitor batch completion for a mosaic and update Artifacts.

**Triggered By**:
- `tile_batch.complex2processed.ready` (after each batch completes processing)
- `tile_batch.archived.ready` (after each batch is archived)

**What It Does**:
1. Scans flag files to count batch states
2. Updates Prefect Artifact with progress (markdown table)
3. Checks if all batches are processed
4. Emits `mosaic.processed` event if all batches complete (and not already stitched)

**Artifact Key**: `{project_name}_mosaic_{mosaic_id}_progress`

**Artifact Content**:
- Progress table showing counts for each batch state
- Progress percentage
- Milestone tracking (25%, 50%, 75%, 100%)

### 2. Slice State Management Flow

**Flow Name**: `manage_slice_state_flow`

**Purpose**: Monitor both mosaics in a slice and update Artifacts.

**Triggered By**:
- `mosaic.stitched` (after each mosaic is stitched)

**What It Does**:
1. Checks state of both mosaics (normal and tilted)
2. Updates Prefect Artifact with slice progress
3. Checks if both mosaics are stitched
4. Emits `slice.ready` event if both mosaics are stitched

**Artifact Key**: `{project_name}_slice_{slice_number}_progress`

**Artifact Content**:
- Progress table for normal illumination mosaic
- Progress table for tilted illumination mosaic
- Overall slice status

## Deployment

### Deploy State Management Flows

```bash
# Deploy both state management flows
python deploy_state_management.py
```

Or deploy individually:

```bash
# Deploy batch state management
prefect deploy workflow/flows/state_management_flow.py:manage_mosaic_batch_state_event_flow_deployment

# Deploy slice state management
prefect deploy workflow/flows/state_management_flow.py:manage_slice_state_event_flow_deployment
```

### Integration with Existing Flows

The state management flows integrate with existing flows:

1. **Tile Batch Flow** (`process_tile_batch_flow`):
   - Emits `tile_batch.archived.ready` after archiving
   - Emits `tile_batch.complex2processed.ready` after complex conversion

2. **Complex to Processed Flow** (`complex_to_processed_batch_event_flow`):
   - Already emits `mosaic.processed` when all batches complete
   - State management flow also emits this as a safety net

3. **Mosaic Processing Flow** (`process_mosaic_flow`):
   - Emits `mosaic.stitched` after stitching completes
   - Triggers slice state management

## Event Flow

```
Tile Batch Completes
  ↓
tile_batch.archived.ready
tile_batch.complex2processed.ready
  ↓
manage_mosaic_batch_state_flow
  ↓
Updates Artifact
Checks if all batches complete
  ↓
If complete: mosaic.processed
  ↓
process_mosaic_flow (stitching)
  ↓
mosaic.stitched
  ↓
manage_slice_state_flow
  ↓
Updates Artifact
Checks if both mosaics stitched
  ↓
If both complete: slice.ready
```

## Artifacts

### Viewing Artifacts

Artifacts are visible in the Prefect UI:
1. Navigate to a flow run
2. Click on "Artifacts" tab
3. View markdown progress reports

### Artifact Keys

- Mosaic: `{project_name}_mosaic_{mosaic_id}_progress`
- Slice: `{project_name}_slice_{slice_number}_progress`

## State File Structure

```
{project_base_path}/
├── mosaic-{mosaic_id}/
│   ├── state/
│   │   ├── batch-0.started
│   │   ├── batch-0.archived
│   │   ├── batch-0.processed
│   │   ├── batch-0.uploaded
│   │   ├── batch-1.started
│   │   └── ...
│   └── stitched/
│       └── mosaic_{mosaic_id:03d}_aip.nii
```

## Benefits

1. **Decoupled**: State management doesn't block processing flows
2. **Observable**: Progress visible in Prefect UI via Artifacts
3. **Resilient**: Flag files persist across flow runs
4. **Event-Driven**: Automatic updates as processing progresses
5. **Safety Net**: Multiple flows can check completion (redundancy)

## Troubleshooting

### Artifacts Not Updating

- Check that state management flows are deployed and running
- Verify events are being emitted (check Prefect UI → Events)
- Check flag files exist in `{project_base_path}/mosaic-{mosaic_id}/state/`

### Duplicate Events

- Both `complex_to_processed_batch_event_flow` and `manage_mosaic_batch_state_flow` can emit `mosaic.processed`
- This is intentional for redundancy
- The mosaic processing flow should handle duplicate events gracefully (idempotent)

### State Not Reflecting Progress

- Verify flag files are being created correctly
- Check state management flow logs for errors
- Manually check flag files: `ls {project_base_path}/mosaic-{mosaic_id}/state/`

