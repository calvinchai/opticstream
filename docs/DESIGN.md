# OCT Pipeline Workflow Design Document

## Executive Summary

This document describes the design of a Prefect-based workflow orchestration system for processing tiled OCT (Optical Coherence Tomography) scan data. The system processes spectral raw data through multiple stages: conversion to complex data, generation of 3D volumes and enface images, tile stitching, registration of dual-illumination scans, and final stacking across slices.

### Key Design Features

- **Batch Processing**: Tiles are processed in batches (grid_size_y tiles per batch) to optimize resource usage and avoid overwhelming the system
- **Event-Driven Architecture**: Flows communicate via Prefect events rather than direct subflow calls, enabling decoupled and scalable processing
- **State Management**: Flag files track batch and mosaic completion state, allowing independent flows to check progress
- **Separate Storage**: Compressed spectral data saved to separate directory/disk to avoid I/O contention
- **Artifact Tracking**: Mosaic-level flows update Artifact tables to track batch progress and trigger downstream processing
- **Slack Integration**: Real-time notifications for processing milestones (25%, 50%, 75%, 100%) and automatic sharing of stitched images

## 1. System Architecture

### 1.1 Infrastructure Overview

- **Two Hosts**: 
  - **Acquisition Host**: Performs data acquisition, generates spectral raw data
  - **Processing Host**: Performs all data processing tasks
- **Data Synchronization**: Syncthing synchronizes data between hosts
- **Cloud Storage**: Processed data (compressed spectral, stitched volumes) uploaded to cloud

### 1.2 Data Organization

```
Data Structure:
├── Slice N
│   ├── Mosaic 2N-1 (normal illumination)
│   │   ├── Tile files (spectral raw)
│   │   └── state/ (flag files for batch tracking)
│   │       ├── batch-0.started
│   │       ├── batch-0.archived
│   │       ├── batch-0.processed
│   │       └── ...
│   └── Mosaic 2N (tilted illumination)
│       ├── Tile files (spectral raw)
│       └── state/ (flag files for batch tracking)
```

**Naming Convention**:
- Slice `n` has mosaics `mosaic_2n-1` (normal) and `mosaic_2n` (tilted)
- Example: Slice 1 → `mosaic_001` (normal), `mosaic_002` (tilted)

**Tile Configuration**:
- Each scan has tile configuration: `grid_size_x` and `grid_size_y`
- Normal and tilted illuminations share the same `grid_size_y`
- Tiles are processed in batches of `grid_size_y` tiles per batch
- Total batches per mosaic = `grid_size_x` batches

### 1.3 Processing Pipeline Stages

1. **Tile Processing**: Spectral raw → Complex → 3D volumes (dBI, O3D, R3D) → Enface images
2. **Mosaic Processing**: Coordinate determination → Stitching of tiles
3. **Slice Processing**: Registration of normal and tilted illuminations
4. **Multi-Slice Processing**: Stacking of 2D images and 3D volumes

## 2. Prefect Flow Design

### 2.1 Flow Hierarchy

```
Main Flow (per experiment/sample)
├── Slice Flow (per slice)
│   ├── Mosaic Flow (per mosaic, 2 per slice)
│   │   ├── Tile Batch Processing Flow (per batch, grid_size_y tiles per batch)
│   │   │   ├── Archive Batch Task (parallel with spectral_to_complex)
│   │   │   ├── Spectral to Complex Batch Task (parallel with archive)
│   │   │   └── Emit Events (complex2processed.ready, upload_to_linc.ready)
│   │   ├── Complex to Processed Event Flow (triggered by event, not subflow)
│   │   ├── Upload to LINC Event Flow (triggered by event, not subflow)
│   │   ├── Mosaic Batch Monitor Flow (checks batch completion via flag files)
│   │   └── Mosaic Stitching Flow (after all batches complete)
│   └── Slice Registration Flow (after both mosaics complete)
└── Final Stacking Flow (after all slices complete)
```

**Key Architecture Points**:
- **Batch Processing**: One flow per batch (not per tile) to reduce resource overhead
- **Event-Driven**: Downstream flows triggered by Prefect events, not direct subflow calls
- **State Tracking**: Flag files in `mosaic-{id}/state/` directory track batch completion
- **Decoupled Processing**: Archive, complex conversion, processing, and upload run independently via events

### 2.2 Core Flows

#### 2.2.1 Tile Batch Processing Flow

**Purpose**: Process a batch of tiles (grid_size_y tiles) from spectral raw data. Only synchronous operations that involve direct I/O from source files run in this flow. After converting to complex data, events are emitted to trigger downstream processing flows (not subflows).

**Flow Name**: `process_tile_batch_flow`

**Tasks**:
1. `check_batch_started` - Check if batch already started (via flag file, prevents duplicate runs)
2. `archive_tile_batch_task` - Archive tiles in batch (synchronous, one by one, writes to separate directory)
3. `spectral_to_complex_batch_task` - Convert spectral raw to complex data for all tiles in batch (synchronous, runs in parallel with archive)
4. `emit_complex_ready_event` - Emit `tile_batch.complex2processed.ready` event (triggers downstream flow)
5. `emit_upload_ready_event` - Emit `tile_batch.upload_to_linc.ready` event (triggers upload flow)
6. `mark_batch_archived` - Create flag file `batch-{batch_id}.archived` to track completion

**Parameters**:
- `project_name`: Project identifier
- `project_base_path`: Base path for the project
- `mosaic_id`: Mosaic identifier (integer, e.g., 1, 2, 3)
- `batch_id`: Batch identifier (integer, 0-indexed)
- `file_list`: List of spectral raw tile file paths (grid_size_y files)

**Dependencies**:
- Tasks 2 and 3 run in parallel (both are synchronous I/O operations)
- Task 4 depends on Task 3 (emits event after complex conversion)
- Task 5 depends on Task 2 (emits event after archiving)
- Task 6 depends on both Tasks 2 and 3 completing
- Flow completes when both archive and spectral_to_complex are done

**State Management**:
- Creates flag file: `{mosaic_path}/state/batch-{batch_id}.started` at flow start
- Creates flag file: `{mosaic_path}/state/batch-{batch_id}.archived` when both tasks complete
- Flag files are checked by mosaic-level flows to determine batch completion

**Event Emissions**:
- `tile_batch.complex2processed.ready`: Emitted after complex conversion, triggers `complex_to_processed_batch_event_flow`
- `tile_batch.upload_to_linc.ready`: Emitted after archiving, triggers `upload_to_linc_batch_event_flow`

**Outputs**:
- Complex data: `{project_base_path}/mosaic-{mosaic_id}/complex/mosaic-{mosaic_id:03d}_image_{tile_index:04d}_complex.nii`
- Archived tiles: `{compressed_base_path}/{project_name}_sample-slice-{slice_id:03d}_chunk-{tile_index:04d}_acq-{acq}_OCT.nii.gz`
- Flag files: `{project_base_path}/mosaic-{mosaic_id}/state/batch-{batch_id}.{status}`

**Important Notes**:
- This flow only handles synchronous operations that require direct I/O from source files
- Downstream processing (complex to processed, uploads) are triggered by events, not called as subflows
- The flow returns immediately after emitting events and marking batch as archived

#### 2.2.2 Complex to Processed Event Flow

**Purpose**: Event-driven flow triggered by `tile_batch.complex2processed.ready` event. Processes complex data to generate 3D volumes and enface images. Not a subflow of tile batch flow.

**Flow Name**: `complex_to_processed_batch_event_flow`

**Trigger**: Prefect event `tile_batch.complex2processed.ready`

**Tasks**:
1. `check_batch_processed` - Check if batch already processed (via flag file)
2. `complex_to_processed_batch_task` - Process complex data to volumes and enface (synchronous)
3. `mark_batch_processed` - Create flag file `batch-{batch_id}.processed`
4. `check_all_batches_processed` - Check if all batches in mosaic are processed (via flag files)
5. `emit_mosaic_processed_event` - If all batches done, emit `mosaic.processed` event

**Parameters** (from event payload):
- `project_name`: Project identifier
- `project_base_path`: Base path for the project
- `mosaic_id`: Mosaic identifier
- `batch_id`: Batch identifier

**State Management**:
- Checks flag file: `{mosaic_path}/state/batch-{batch_id}.processed` to avoid reprocessing
- Creates flag file: `{mosaic_path}/state/batch-{batch_id}.processed` after processing
- Counts all `batch-*.processed` files to determine if mosaic is complete

**Event Emissions**:
- `mosaic.processed`: Emitted when all batches in mosaic are processed, triggers mosaic stitching flow

**Outputs**:
- 3D volumes: `{project_base_path}/mosaic-{mosaic_id}/processed/mosaic-{mosaic_id:03d}_image_{tile_index:04d}_dBI.nii`
- Enface images: `{project_base_path}/mosaic-{mosaic_id}/processed/mosaic-{mosaic_id:03d}_image_{tile_index:04d}_aip.nii`

#### 2.2.3 Upload to LINC Event Flow

**Purpose**: Event-driven flow triggered by `tile_batch.upload_to_linc.ready` event. Uploads archived tiles to LINC storage. Not a subflow of tile batch flow.

**Flow Name**: `upload_to_linc_batch_event_flow`

**Trigger**: Prefect event `tile_batch.upload_to_linc.ready`

**Tasks**:
1. `check_batch_uploaded` - Check if batch already uploaded (via flag file)
2. `upload_to_linc_batch_task` - Upload archived files to LINC (synchronous)
3. `mark_batch_uploaded` - Create flag file `batch-{batch_id}.uploaded`

**Parameters** (from event payload):
- `project_name`: Project identifier
- `project_base_path`: Base path for the project
- `mosaic_id`: Mosaic identifier
- `batch_id`: Batch identifier
- `archived_file_paths`: List of archived file paths to upload

**State Management**:
- Checks flag file: `{mosaic_path}/state/batch-{batch_id}.uploaded` to avoid re-uploading
- Creates flag file: `{mosaic_path}/state/batch-{batch_id}.uploaded` after upload

#### 2.2.4 Mosaic Batch Monitor Flow

**Purpose**: Monitors batch completion for a mosaic by checking flag files. Updates Artifact table with batch progress. Triggers mosaic stitching when all batches are complete.

**Flow Name**: `monitor_mosaic_batches_flow`

**Tasks**:
1. `check_batch_flags` - Scan `{mosaic_path}/state/` directory for batch flag files
2. `count_batch_states` - Count batches in each state (started, archived, processed, uploaded)
3. `update_artifact_table` - Update Artifact table with batch progress
4. `check_all_batches_processed` - Verify all batches have `.processed` flag files
5. `trigger_mosaic_stitching` - If all batches processed, trigger mosaic stitching flow

**State Management**:
- Reads flag files: `batch-*.started`, `batch-*.archived`, `batch-*.processed`, `batch-*.uploaded`
- Determines total batches from `batch-*.started` files
- Compares processed count to total to determine completion

**Artifact Table Updates**:
- Updates progress: `{processed_batches}/{total_batches}`
- Records batch states and timestamps
- Tracks processing milestones (25%, 50%, 75%, 100%)

#### 2.2.5 Mosaic Coordinate Determination Flow

**Purpose**: Determine stitching coordinates for a mosaic after all tiles are processed.

**Flow Name**: `determine_mosaic_coordinates_flow`

**Tasks**:
1. `collect_tile_aip_images` - Collect all AIP enface images for the mosaic
2. `determine_tile_coordinates` - Run coordinate determination script using AIP images
3. `save_coordinates` - Save coordinates to YAML/JSON file

**Parameters**:
- `mosaic_id`: Mosaic identifier
- `tile_paths`: List of processed tile paths
- `ideal_coord_file`: Path to ideal coordinate file (if available)
- `output_coord_file`: Path to save determined coordinates

**Dependencies**:
- Runs after all tiles in mosaic are processed
- Uses AIP enface images from tile processing

**Outputs**:
- Coordinate file: `{output_base_path}/coordinates/{mosaic_id}_coordinates.yaml`

#### 2.2.6 Mosaic Stitching Flow

**Purpose**: Stitch all tiles in a mosaic together using determined coordinates.

**Flow Name**: `stitch_mosaic_flow`

**Tasks**:
1. `load_coordinates` - Load coordinate file for mosaic (synchronous)
2. `create_mask_from_aip` - Create mask from stitched AIP (threshold-based, synchronous)
3. `stitch_enface_images` - Stitch all enface modalities (AIP, MIP, orientation, retardance, birefringence, synchronous)
4. `stitch_3d_volumes` - Stitch 3D volumes (dBI, O3D, R3D, synchronous)
5. `apply_mask` - Apply mask to all stitched outputs (synchronous)
6. `save_stitched_enface` - Save stitched enface images (synchronous)
7. `save_stitched_volumes` - Save stitched 3D volumes (synchronous)
8. `queue_upload_stitched_volumes` - Queue stitched 3D volumes for cloud upload (async, non-blocking)
9. `notify_stitched_complete` - Send stitched image to Slack channel (async, non-blocking)

**Parameters**:
- `mosaic_id`: Mosaic identifier
- `coordinate_file`: Path to coordinate file
- `tile_paths`: List of processed tile paths
- `mask_threshold`: Threshold for AIP mask creation
- `overlap`: Overlap between tiles in pixels
- `cloud_config`: Cloud upload configuration

**Dependencies**:
- Requires coordinate determination flow to complete
- Requires all tiles in mosaic to be processed

**Outputs**:
- Stitched enface: `{output_base_path}/stitched/{mosaic_id}_aip.nii`
- Stitched volumes: `{output_base_path}/stitched/{mosaic_id}_dBI.nii`

#### 2.2.7 Slice Registration Flow

**Purpose**: Register normal and tilted illumination mosaics to combine orientations.

**Flow Name**: `register_slice_flow`

**Tasks**:
1. `load_normal_mosaic` - Load normal illumination mosaic data
2. `load_tilted_mosaic` - Load tilted illumination mosaic data
3. `register_orientations` - Perform registration to align orientations
4. `compute_3d_orientation` - Compute 3D orientation (thru-plane, in-plane, 3D axis)
5. `save_registered_data` - Save registered orientation data

**Parameters**:
- `slice_number`: Slice number (n)
- `normal_mosaic_id`: Normal mosaic ID (2n-1)
- `tilted_mosaic_id`: Tilted mosaic ID (2n)
- `gamma`: Tilt angle parameter
- `mask_file`: Optional mask file for registration
- `mask_threshold`: Threshold for mask

**Dependencies**:
- Requires both mosaic stitching flows to complete

**Outputs**:
- Registered orientation: `{output_base_path}/registered/slice_{slice_number}_orientation.nii`
- 3D axis: `{output_base_path}/registered/slice_{slice_number}_3daxis.jpg`

#### 2.2.8 Slice Flow

**Purpose**: Orchestrate processing of a single slice (both mosaics).

**Flow Name**: `process_slice_flow`

**Tasks**:
1. `process_normal_mosaic` - Process normal illumination mosaic (subflow)
2. `process_tilted_mosaic` - Process tilted illumination mosaic (subflow)
3. `register_slice` - Register slice (subflow)

**Parameters**:
- `slice_number`: Slice number
- `normal_mosaic_id`: Normal mosaic ID
- `tilted_mosaic_id`: Tilted mosaic ID
- All parameters from tile/mosaic/registration flows

**Dependencies**:
- Task 1 and 2 can run in parallel (after tiles are available)
- Task 3 depends on Tasks 1 and 2

**Subflows**:
- `process_mosaic_flow` (called twice, once per mosaic)

#### 2.2.9 Mosaic Processing Flow

**Purpose**: Orchestrate batch processing for a mosaic. Triggers batch flows and monitors completion via flag files and events.

**Flow Name**: `process_mosaic_flow`

**Tasks**:
1. `discover_batches` - Discover all batches for mosaic (based on grid_size_x and grid_size_y)
2. `trigger_batch_flows` - Trigger tile batch processing flows for each batch (map task)
3. `monitor_batch_completion` - Monitor batch completion via flag files and events (background task)
4. `update_artifact_table` - Update Artifact table with batch progress
5. `wait_for_all_batches` - Wait for all batches to complete (via flag file checks)
6. `determine_coordinates` - Determine coordinates (subflow, after all batches processed)
7. `stitch_mosaic` - Stitch mosaic (subflow, after coordinates determined)

**Parameters**:
- `project_name`: Project identifier
- `project_base_path`: Base path for the project
- `mosaic_id`: Mosaic identifier
- `grid_size_x`: Number of batches (columns) in mosaic
- `grid_size_y`: Number of tiles per batch (rows, shared between normal and tilted)
- `tile_file_pattern`: Pattern to match tile files for this mosaic

**Dependencies**:
- Task 2 triggers batch flows (they run independently)
- Task 3 monitors flag files and events (runs in background)
- Task 4 updates based on Task 3 monitoring
- Task 5 waits for all `batch-*.processed` flag files
- Task 6 depends on Task 5
- Task 7 depends on Task 6

**Event Listeners**:
- Listens for `mosaic.processed` event (emitted by complex_to_processed_batch_event_flow)
- Can also poll flag files as backup mechanism

**State Management**:
- Monitors flag files in `{project_base_path}/mosaic-{mosaic_id}/state/`
- Tracks: `batch-*.started`, `batch-*.archived`, `batch-*.processed`, `batch-*.uploaded`
- Determines completion when all batches have `.processed` flag files

**Subflows**:
- `process_tile_batch_flow` (mapped over batches, triggered independently)
- `determine_mosaic_coordinates_flow`
- `stitch_mosaic_flow`

#### 2.2.10 Final Stacking Flow

**Purpose**: Stack all processed slices together.

**Flow Name**: `stack_all_slices_flow`

**Tasks**:
1. `collect_slice_data` - Collect all slice data paths
2. `stack_2d_images` - Stack all 2D enface images
3. `stack_3d_volumes` - Stack all 3D volumes
4. `save_stacked_data` - Save stacked outputs

**Parameters**:
- `slice_numbers`: List of all slice numbers
- `output_base_path`: Base path for outputs

**Dependencies**:
- Requires all slice flows to complete

**Outputs**:
- Stacked 2D: `{output_base_path}/stacked/all_slices_aip.nii`
- Stacked 3D: `{output_base_path}/stacked/all_slices_dBI.nii`

#### 2.2.11 Main Experiment Flow

**Purpose**: Orchestrate entire experiment processing.

**Flow Name**: `process_experiment_flow`

**Tasks**:
1. `discover_slices` - Discover available slices from data directory
2. `process_all_slices` - Process all slices (map task)
3. `stack_all_slices` - Stack all slices (subflow)

**Parameters**:
- `data_root_path`: Root path to data directory
- `output_base_path`: Base path for outputs
- `slice_numbers`: Optional list of slice numbers (if None, auto-discover)
- All parameters from subflows

**Dependencies**:
- Task 2 depends on Task 1
- Task 3 depends on Task 2

**Subflows**:
- `process_slice_flow` (mapped over slices)
- `stack_all_slices_flow`

### 2.3 Event-Driven Architecture

#### 2.3.1 Event Flow Design

**Key Principle**: Downstream processing flows are triggered by Prefect events, not called as subflows. This decouples processing stages and allows independent scaling.

**Event Types**:

1. **`tile_batch.complex2processed.ready`**:
   - Emitted by: `spectral_to_complex_batch_task` after converting batch to complex data
   - Triggers: `complex_to_processed_batch_event_flow`
   - Payload: `{project_name, project_base_path, mosaic_id, batch_id}`

2. **`tile_batch.upload_to_linc.ready`**:
   - Emitted by: `archive_tile_batch_task` after archiving batch
   - Triggers: `upload_to_linc_batch_event_flow`
   - Payload: `{project_name, project_base_path, mosaic_id, batch_id, archived_file_paths}`

3. **`mosaic.processed`**:
   - Emitted by: `complex_to_processed_batch_event_flow` when all batches in mosaic are processed
   - Triggers: `stitch_mosaic_flow` or mosaic-level orchestration
   - Payload: `{project_name, project_base_path, mosaic_id, total_batches}`

**Event Flow Benefits**:
- **Decoupling**: Flows don't need to wait for downstream processing
- **Scalability**: Event-driven flows can scale independently
- **Resilience**: Failed downstream flows don't block upstream flows
- **Flexibility**: Easy to add new event listeners without modifying existing flows

#### 2.3.2 State Management via Flag Files

**Flag File System**:
- Location: `{project_base_path}/mosaic-{mosaic_id}/state/`
- Naming: `batch-{batch_id}.{status}`
- Statuses: `started`, `archived`, `processed`, `uploaded`

**Flag File Usage**:
1. **Idempotency**: Check if batch already processed before starting
2. **Progress Tracking**: Count flag files to determine batch completion
3. **Mosaic Completion**: Check if all batches have `.processed` flag files
4. **Recovery**: Flag files persist across flow runs, enabling recovery

**Flag File Operations**:
- Create: `batch-{batch_id}.started` at flow start
- Update: `batch-{batch_id}.archived` after archiving
- Update: `batch-{batch_id}.processed` after processing
- Update: `batch-{batch_id}.uploaded` after upload

**Monitoring**:
- Mosaic-level flows scan flag files to determine progress
- Artifact table updated based on flag file counts
- Event emissions can be verified against flag file state

#### 2.3.3 Artifact Table Updates

**Purpose**: Track batch progress at mosaic level for monitoring and downstream triggering.

**Update Triggers**:
- After each batch completes processing (via `complex_to_processed_batch_event_flow`)
- Periodic checks by mosaic monitor flow
- Before triggering mosaic stitching

**Artifact Table Schema** (conceptual):
- `mosaic_id`: Mosaic identifier
- `total_batches`: Total number of batches (from grid_size_x)
- `processed_batches`: Count of batches with `.processed` flag files
- `archived_batches`: Count of batches with `.archived` flag files
- `uploaded_batches`: Count of batches with `.uploaded` flag files
- `progress_percentage`: `processed_batches / total_batches * 100`
- `last_updated`: Timestamp of last update

**Update Operations**:
- Increment counters when batch flag files are created
- Calculate progress percentage
- Check if all batches complete (trigger stitching if so)

#### 2.3.4 Compression and Storage Design

**Compression Task**:
- **Input**: Spectral raw tile file
- **Output**: Compressed file in separate directory/disk
- **Location**: `{compressed_base_path}/{mosaic_id}_tile_{tile_index}_spectral.nii.gz`
- **Implementation**: Uses `gzip` compression
- **Async Behavior**: Fire-and-forget, doesn't wait for completion
- **Error Handling**: Logs errors but doesn't fail the flow

**Storage Separation**:
- **Processed Data**: `{output_base_path}/processed/` (main storage)
- **Compressed Data**: `{compressed_base_path}/` (separate directory/disk)
- **Rationale**: Separate I/O paths to avoid disk contention

#### 2.3.5 Upload Queue Management

**Upload Queue Architecture**:
- **Queue Manager**: Centralized upload queue manager (singleton pattern)
- **Concurrency**: Maximum 5 concurrent uploads at a time
- **CLI Tool**: Uses external CLI tool for actual uploads (e.g., `aws s3 cp`, `gsutil cp`, `azcopy`)
- **Queue Implementation**: Thread-safe queue with semaphore for concurrency control

**Upload Queue Task**:
- **Purpose**: Add files to upload queue (non-blocking)
- **Behavior**: 
  - Adds file path to queue
  - Returns immediately (doesn't wait for upload)
  - Queue manager handles actual uploads in background
- **Queue Manager**:
  - Runs as background process/thread
  - Monitors queue for new files
  - Executes CLI tool with max 5 concurrent processes
  - Handles retries and error logging
  - Reports upload status

**Upload Queue Configuration**:
```python
class UploadQueueManager:
    def __init__(self, max_concurrent=5, cli_tool="aws", cli_args=None):
        self.queue = queue.Queue()
        self.semaphore = threading.Semaphore(max_concurrent)
        self.cli_tool = cli_tool
        self.cli_args = cli_args or []
        self.upload_workers = []
    
    def enqueue(self, file_path: str, destination: str):
        """Add file to upload queue (non-blocking)"""
        self.queue.put((file_path, destination))
    
    def start_workers(self, num_workers=1):
        """Start background upload workers"""
        for _ in range(num_workers):
            worker = threading.Thread(target=self._upload_worker)
            worker.daemon = True
            worker.start()
            self.upload_workers.append(worker)
    
    def _upload_worker(self):
        """Background worker that processes upload queue"""
        while True:
            try:
                file_path, destination = self.queue.get(timeout=1)
                with self.semaphore:  # Limit concurrent uploads
                    self._execute_upload(file_path, destination)
                self.queue.task_done()
            except queue.Empty:
                continue
    
    def _execute_upload(self, file_path: str, destination: str):
        """Execute CLI tool for upload"""
        import subprocess
        cmd = [self.cli_tool] + self.cli_args + [file_path, destination]
        subprocess.run(cmd, check=True)
```

#### 2.3.6 Slack Notification System

**Notification Milestones**:
1. **25% Tile Completion**: When 25% of tiles in a mosaic are processed
2. **50% Tile Completion**: When 50% of tiles in a mosaic are processed
3. **75% Tile Completion**: When 75% of tiles in a mosaic are processed
4. **100% Tile Completion**: When all tiles in a mosaic are processed
5. **Stitching Complete**: When mosaic stitching is complete (includes stitched image)

**Notification Tasks**:
- `notify_tile_complete`: Individual tile completion (optional, can be disabled for high-volume)
- `monitor_tile_progress`: Background task that monitors tile completion and sends milestone notifications
- `notify_stitched_complete`: Sends stitched AIP image to Slack channel

**Slack Integration**:
- Uses Slack Webhook API or Slack SDK
- Sends formatted messages with:
  - Mosaic ID
  - Progress percentage
  - Timestamp
  - Processing statistics
- For stitched images: Uploads image to Slack (file upload API)

**Slack Notification Implementation**:
```python
@task(name="notify_slack", allow_failure=True)
async def notify_slack_task(
    message: str,
    image_path: Optional[str] = None,
    slack_config: Dict = None
):
    """Send notification to Slack channel"""
    import slack_sdk
    from slack_sdk.webhook import WebhookClient
    
    webhook = WebhookClient(slack_config["webhook_url"])
    
    if image_path:
        # Upload image and send message with image
        with open(image_path, "rb") as f:
            response = webhook.send(
                text=message,
                files=[("image", f)]
            )
    else:
        # Send text message only
        response = webhook.send(text=message)
    
    return response.status_code == 200

@task(name="monitor_tile_progress")
async def monitor_tile_progress_task(
    mosaic_id: str,
    total_tiles: int,
    completed_tiles: List[str],
    slack_config: Dict = None
):
    """Monitor tile progress and send milestone notifications"""
    completed_count = len(completed_tiles)
    progress = completed_count / total_tiles
    
    milestones = [0.25, 0.50, 0.75, 1.0]
    for milestone in milestones:
        if progress >= milestone and not has_notified_milestone(mosaic_id, milestone):
            message = f"🎯 Mosaic {mosaic_id}: {milestone*100:.0f}% complete ({completed_count}/{total_tiles} tiles)"
            notify_slack_task(message, slack_config=slack_config)
            mark_milestone_notified(mosaic_id, milestone)
```

### 2.4 Flow Triggering Strategy

#### 2.4.1 Batch Flow Triggering

- **Tile Batch Processing**: Triggered when batch of tiles (grid_size_y tiles) is available
- **Batch Discovery**: Mosaic flow discovers batches based on grid_size_x and grid_size_y
- **Batch Execution**: Each batch flow runs independently, processes grid_size_y tiles

#### 2.4.2 Event-Driven Downstream Processing

- **Complex to Processed**: Triggered by `tile_batch.complex2processed.ready` event
- **Upload to LINC**: Triggered by `tile_batch.upload_to_linc.ready` event
- **Mosaic Stitching**: Triggered by `mosaic.processed` event (when all batches complete)

#### 2.4.3 Flag File-Based Completion Checking

- **Batch Completion**: Check flag files (`batch-*.processed`) to determine if all batches done
- **Mosaic Completion**: Mosaic flow polls flag files or listens for `mosaic.processed` event
- **State Persistence**: Flag files enable recovery and idempotent operations

#### 2.4.4 Artifact Table-Based Monitoring

- **Progress Tracking**: Artifact table updated as batches complete
- **Mosaic Triggering**: Mosaic stitching triggered when Artifact table shows 100% completion
- **Monitoring**: External systems can query Artifact table for progress

## 3. Deployment Design

### 3.1 Prefect Server/Cloud Setup

**Option A: Prefect Cloud (Recommended)**
- Use Prefect Cloud for centralized orchestration
- Benefits: Built-in UI, monitoring, alerting, team collaboration
- Configuration: API key-based authentication

**Option B: Self-Hosted Prefect Server**
- Deploy Prefect Server on processing host or separate server
- Benefits: Full control, no external dependencies
- Configuration: Docker container or direct installation

### 3.2 Work Pool Configuration

**Work Pools**:
1. **Tile Processing Pool**: High parallelism, CPU-intensive
   - Type: Process pool
   - Workers: 8-16 (depending on CPU cores)
   - Resources: High CPU, moderate memory

2. **Stitching Pool**: Moderate parallelism, memory-intensive
   - Type: Process pool
   - Workers: 4-8
   - Resources: High memory, moderate CPU

3. **Registration Pool**: Low parallelism, CPU-intensive
   - Type: Process pool
   - Workers: 2-4
   - Resources: High CPU, high memory

4. **Cloud Upload Pool**: High parallelism, I/O-bound
   - Type: Thread pool
   - Workers: 10-20
   - Resources: High network bandwidth

### 3.3 Agent Deployment

**Processing Host Agent**:
- Deploy Prefect agent on processing host
- Configure to pull from all work pools
- Use systemd service or Docker container for reliability
- Monitor agent health and auto-restart

**Configuration**:
```bash
prefect agent start --pool tile-processing-pool --pool stitching-pool --pool registration-pool --pool cloud-upload-pool
```

### 3.4 Storage Configuration

**Result Storage**:
- Use local filesystem for intermediate results
- Configure Prefect to use local storage backend
- Path: `/path/to/prefect/storage`

**Flow Storage**:
- Store flow definitions in Git repository
- Use Prefect's Git storage or local file storage
- Version control for flow changes

### 3.5 Concurrency and Resource Management

**Concurrency Limits**:
- Tile processing: 16 concurrent tasks
- Stitching: 4 concurrent tasks
- Registration: 2 concurrent tasks
- Cloud upload: 20 concurrent tasks

**Resource Tags**:
- Tag tasks with resource requirements
- Use Prefect's resource management to prevent over-subscription
- Example tags: `high-memory`, `gpu-required`, `network-intensive`

## 4. Run Management

### 4.1 Flow Scheduling

**Scheduled Runs**:
- Not applicable (event-driven based on data availability)

**Manual Runs**:
- Trigger flows manually via Prefect UI or CLI
- Useful for reprocessing or testing

**API-Based Runs**:
- Trigger flows programmatically via Prefect API
- Useful for integration with acquisition system

### 4.2 Run Monitoring

**Prefect UI**:
- Monitor flow runs in real-time
- View task execution status
- Inspect logs and errors
- View flow run history

**Custom Monitoring**:
- Set up alerts for failed runs
- Monitor processing times
- Track data throughput
- Generate processing reports

### 4.3 Error Handling and Retries

**Retry Configuration**:
- Tile processing: 3 retries with exponential backoff
- Stitching: 2 retries
- Registration: 2 retries
- Cloud upload: 5 retries (network issues)

**Failure Handling**:
- Failed tiles: Log and continue with other tiles
- Failed mosaics: Alert and pause slice processing
- Failed slices: Alert and continue with other slices
- Failed stacking: Alert and require manual intervention

**Checkpointing**:
- Save intermediate results after each major stage
- Enable resumption from last checkpoint
- Store checkpoint metadata in Prefect

### 4.4 Data Validation

**Input Validation**:
- Validate spectral raw file format
- Check file integrity
- Verify expected file structure

**Output Validation**:
- Validate output file formats
- Check output data ranges
- Verify output file sizes
- Compare with expected dimensions

### 4.5 Logging and Debugging

**Logging Strategy**:
- Use Prefect's built-in logging
- Log to files and Prefect UI
- Include timestamps, task IDs, and context

**Debugging Tools**:
- Use Prefect's task inspection
- Enable verbose logging for debugging
- Use Prefect's flow visualization
- Store intermediate results for inspection

## 5. Implementation Details

### 5.1 Task Implementations

**Key Task Functions**:

```python
@task(name="spectral_to_complex")
def spectral_to_complex_task(spectral_path: str) -> np.ndarray:
    """Convert spectral raw to complex data."""
    from oct_pipe.spectral_raw.spectral2complex import spectral2complex
    spectral_data = load_spectral_file(spectral_path)
    complex_data = spectral2complex(spectral_data)
    return complex_data  # In-memory, not saved

@task(name="complex_to_volumes")
def complex_to_volumes_task(complex_data: np.ndarray) -> Dict[str, np.ndarray]:
    """Convert complex to 3D volumes."""
    from oct_pipe.volume_3d.complex2vol import process_complex3d
    dBI, O3D, R3D = process_complex3d(complex_data)
    return {"dBI": dBI, "O3D": O3D, "R3D": R3D}

@task(name="volumes_to_enface")
def volumes_to_enface_task(
    volumes: Dict[str, np.ndarray],
    surface: Union[str, int, np.ndarray],
    depth: int = 80
) -> Dict[str, np.ndarray]:
    """Generate enface images from volumes."""
    from oct_pipe.enface.vol2enface import EnfaceVolume
    enface = EnfaceVolume(
        volumes["dBI"],
        volumes["R3D"],
        volumes["O3D"],
        surface=surface,
        depth=depth
    )
    return {
        "aip": enface.aip,
        "mip": enface.mip,
        "orientation": enface.orientation,
        "retardance": enface.retardance,
        "birefringence": enface.birefringence
    }

@task(name="stitch_mosaic")
def stitch_mosaic_task(
    tile_paths: List[str],
    coordinates: Dict[str, Tuple[int, int]],
    overlap: int = 50
) -> Dict[str, np.ndarray]:
    """Stitch tiles into mosaic."""
    from oct_pipe.stitch.stitch2d import stitch_2d
    from oct_pipe.stitch.process_tile_coord import load_tile_info
    
    tiles = []
    for path in tile_paths:
        tile_info = load_tile_info(path, coordinates[path])
        tiles.append(tile_info)
    
    stitched = {}
    for modality in ["aip", "mip", "orientation", "retardance", "birefringence"]:
        modality_tiles = [load_modality_tile(t, modality) for t in tiles]
        stitched[modality] = stitch_2d(modality_tiles, overlap)
    
    return stitched

# Async Task Implementations

@task(name="compress_spectral", allow_failure=True)
def compress_spectral_task(
    tile_path: str,
    compressed_base_path: str,
    mosaic_id: str,
    tile_index: int
) -> str:
    """Compress spectral raw file to separate directory (async, fire-and-forget)."""
    import gzip
    import shutil
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(compressed_base_path, exist_ok=True)
    
    # Output path
    output_filename = f"{mosaic_id}_tile_{tile_index}_spectral.nii.gz"
    output_path = os.path.join(compressed_base_path, output_filename)
    
    # Compress file
    with open(tile_path, 'rb') as f_in:
        with gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    return output_path

@task(name="queue_upload_spectral", allow_failure=True)
def queue_upload_spectral_task(
    compressed_file_path: str,
    destination: str,
    upload_queue: 'UploadQueueManager'
) -> None:
    """Queue compressed file for cloud upload (async, non-blocking)."""
    upload_queue.enqueue(compressed_file_path, destination)
    # Returns immediately, upload happens in background

@task(name="queue_upload_stitched_volumes", allow_failure=True)
def queue_upload_stitched_volumes_task(
    volume_paths: Dict[str, str],
    destination_base: str,
    upload_queue: 'UploadQueueManager'
) -> None:
    """Queue stitched volumes for cloud upload (async, non-blocking)."""
    for modality, path in volume_paths.items():
        destination = f"{destination_base}/{os.path.basename(path)}"
        upload_queue.enqueue(path, destination)

@task(name="notify_slack", allow_failure=True)
def notify_slack_task(
    message: str,
    image_path: Optional[str] = None,
    slack_config: Dict = None
) -> bool:
    """Send notification to Slack channel (async, non-blocking)."""
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import os
    
    client = WebClient(token=slack_config.get("bot_token"))
    channel = slack_config.get("channel", "#oct-processing")
    
    try:
        if image_path and os.path.exists(image_path):
            # Upload image with message
            response = client.files_upload_v2(
                channel=channel,
                file=image_path,
                initial_comment=message
            )
        else:
            # Send text message only
            response = client.chat_postMessage(
                channel=channel,
                text=message
            )
        return response["ok"]
    except SlackApiError as e:
        logger.error(f"Slack API error: {e}")
        return False

@task(name="monitor_tile_progress")
def monitor_tile_progress_task(
    mosaic_id: str,
    total_tiles: int,
    completed_tiles: List[str],
    slack_config: Dict = None
) -> None:
    """Monitor tile progress and send milestone notifications (async, background)."""
    import threading
    from collections import defaultdict
    
    # Track milestones per mosaic (in-memory, could use Redis for distributed)
    milestone_state = defaultdict(set)
    
    completed_count = len(completed_tiles)
    progress = completed_count / total_tiles
    
    milestones = [0.25, 0.50, 0.75, 1.0]
    for milestone in milestones:
        milestone_key = f"{mosaic_id}_{milestone}"
        if progress >= milestone and milestone_key not in milestone_state[mosaic_id]:
            message = (
                f"🎯 Mosaic {mosaic_id}: {milestone*100:.0f}% complete "
                f"({completed_count}/{total_tiles} tiles)"
            )
            # Send notification asynchronously
            notify_slack_task.submit(message, slack_config=slack_config)
            milestone_state[mosaic_id].add(milestone_key)
```

### 5.2 Upload Queue Manager Implementation

**UploadQueueManager Class**:

```python
import queue
import threading
import subprocess
import logging
from typing import Optional, Tuple, Dict
from pathlib import Path

logger = logging.getLogger(__name__)

class UploadQueueManager:
    """
    Manages upload queue with concurrency control.
    Uses CLI tool for actual uploads (e.g., aws s3 cp, gsutil cp).
    Maximum 5 concurrent uploads at a time.
    """
    
    def __init__(
        self,
        max_concurrent: int = 5,
        cli_tool: str = "aws",
        cli_base_args: Optional[list] = None,
        num_workers: int = 1
    ):
        """
        Initialize upload queue manager.
        
        Parameters
        ----------
        max_concurrent : int
            Maximum number of concurrent uploads (default: 5)
        cli_tool : str
            CLI tool command (default: "aws")
        cli_base_args : list, optional
            Base arguments for CLI tool (e.g., ["s3", "cp"] for aws s3 cp)
        num_workers : int
            Number of background worker threads (default: 1)
        """
        self.queue = queue.Queue()
        self.semaphore = threading.Semaphore(max_concurrent)
        self.cli_tool = cli_tool
        self.cli_base_args = cli_base_args or []
        self.upload_workers = []
        self.running = False
        self.num_workers = num_workers
        
    def start(self):
        """Start background upload workers."""
        if self.running:
            logger.warning("Upload queue manager already running")
            return
        
        self.running = True
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._upload_worker,
                name=f"UploadWorker-{i}",
                daemon=True
            )
            worker.start()
            self.upload_workers.append(worker)
        logger.info(f"Started {self.num_workers} upload worker(s)")
    
    def stop(self):
        """Stop upload workers (waits for queue to empty)."""
        self.running = False
        self.queue.join()  # Wait for all tasks to complete
        logger.info("Upload queue manager stopped")
    
    def enqueue(self, file_path: str, destination: str, metadata: Optional[Dict] = None):
        """
        Add file to upload queue (non-blocking).
        
        Parameters
        ----------
        file_path : str
            Path to file to upload
        destination : str
            Destination path/URL for upload
        metadata : dict, optional
            Additional metadata for logging
        """
        if not Path(file_path).exists():
            logger.error(f"File not found: {file_path}")
            return
        
        self.queue.put({
            "file_path": file_path,
            "destination": destination,
            "metadata": metadata or {}
        })
        logger.debug(f"Enqueued upload: {file_path} -> {destination}")
    
    def _upload_worker(self):
        """Background worker that processes upload queue."""
        while self.running:
            try:
                item = self.queue.get(timeout=1)
                with self.semaphore:  # Limit concurrent uploads
                    self._execute_upload(
                        item["file_path"],
                        item["destination"],
                        item["metadata"]
                    )
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Upload worker error: {e}")
                self.queue.task_done()
    
    def _execute_upload(self, file_path: str, destination: str, metadata: Dict):
        """
        Execute CLI tool for upload.
        
        Parameters
        ----------
        file_path : str
            Path to file to upload
        destination : str
            Destination path/URL
        metadata : dict
            Additional metadata
        """
        try:
            # Build command
            cmd = [self.cli_tool] + self.cli_base_args + [file_path, destination]
            
            logger.info(f"Uploading {file_path} to {destination}")
            
            # Execute CLI tool
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            logger.info(f"Successfully uploaded {file_path}")
            
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Upload failed for {file_path}: {e.stderr}"
            )
            # Could implement retry logic here
        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for {file_path}")
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")

# Global upload queue manager instance
_upload_queue_manager: Optional[UploadQueueManager] = None

def get_upload_queue_manager(
    max_concurrent: int = 5,
    cli_tool: str = "aws",
    cli_base_args: Optional[list] = None
) -> UploadQueueManager:
    """Get or create global upload queue manager (singleton pattern)."""
    global _upload_queue_manager
    
    if _upload_queue_manager is None:
        _upload_queue_manager = UploadQueueManager(
            max_concurrent=max_concurrent,
            cli_tool=cli_tool,
            cli_base_args=cli_base_args
        )
        _upload_queue_manager.start()
    
    return _upload_queue_manager
```

**Usage Examples**:

```python
# For AWS S3
upload_queue = get_upload_queue_manager(
    max_concurrent=5,
    cli_tool="aws",
    cli_base_args=["s3", "cp"]
)

# For Google Cloud Storage
upload_queue = get_upload_queue_manager(
    max_concurrent=5,
    cli_tool="gsutil",
    cli_base_args=["cp"]
)

# For Azure Blob Storage
upload_queue = get_upload_queue_manager(
    max_concurrent=5,
    cli_tool="azcopy",
    cli_base_args=["copy"]
)
```

### 5.3 Flow Definitions

**Example Batch Processing Flow with Event-Driven Architecture**:

```python
from prefect import flow, task
from prefect.events import emit_event
from typing import List, Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

@flow(name="process_tile_batch_flow")
def process_tile_batch_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str]
):
    """
    Process a batch of tiles (grid_size_y tiles).
    Only runs synchronous operations that involve direct I/O from source files.
    Emits events to trigger downstream processing (not subflows).
    """
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id}"
    mosaic_path.mkdir(parents=True, exist_ok=True)
    state_path = mosaic_path / "state"
    state_path.mkdir(parents=True, exist_ok=True)
    
    # Check if batch already started (idempotency)
    batch_started_path = state_path / f"batch-{batch_id}.started"
    if batch_started_path.exists():
        logger.info(f"Batch {batch_id} already started, skipping")
        return
    
    # Mark batch as started
    batch_started_path.touch()
    
    # Run archive and spectral_to_complex in parallel (both are synchronous I/O)
    archive_future = archive_tile_batch_task.submit(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        file_list=file_list
    )
    
    spectral_to_complex_future = spectral_to_complex_batch_task.submit(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        file_list=file_list
    )
    
    # Wait for both to complete
    archive_result = archive_future.wait()
    spectral_to_complex_result = spectral_to_complex_future.wait()
    
    # Mark batch as archived
    batch_archived_path = state_path / f"batch-{batch_id}.archived"
    batch_archived_path.touch()
    
    # Events are emitted by the tasks themselves
    # This flow completes here - downstream flows triggered by events
    
    logger.info(
        f"Batch {batch_id} in mosaic {mosaic_id} completed. "
        f"Events emitted for downstream processing."
    )
    
    return {
        "archive_result": archive_result,
        "spectral_to_complex_result": spectral_to_complex_result,
    }

@flow(name="complex_to_processed_batch_event_flow")
def complex_to_processed_batch_event_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
):
    """
    Event-driven flow triggered by 'tile_batch.complex2processed.ready' event.
    Not a subflow of tile_batch_flow - triggered independently by Prefect events.
    """
    from prefect.events import Event
    
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id}"
    state_path = mosaic_path / "state"
    state_path.mkdir(parents=True, exist_ok=True)
    
    # Check if already processed (idempotency)
    batch_processed_path = state_path / f"batch-{batch_id}.processed"
    if batch_processed_path.exists():
        logger.info(f"Batch {batch_id} already processed")
    else:
        # Process complex data to volumes and enface
        complex_to_processed_batch_task(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id
        )
        
        # Mark batch as processed
        batch_processed_path.touch()
        logger.info(f"Batch {batch_id} processed successfully")
    
    # Check if all batches in mosaic are processed
    batch_started_files = list(state_path.glob("batch-*.started"))
    total_batches = len(batch_started_files)
    
    if total_batches == 0:
        logger.warning(f"No batch files found for mosaic {mosaic_id}")
        return
    
    batch_processed_files = list(state_path.glob("batch-*.processed"))
    processed_count = len(batch_processed_files)
    
    logger.info(
        f"Mosaic {mosaic_id}: {processed_count}/{total_batches} batches processed"
    )
    
    # If all batches processed, emit mosaic_processed event
    if processed_count >= total_batches:
        logger.info(
            f"All batches processed for mosaic {mosaic_id}. "
            f"Emitting mosaic.processed event."
        )
        emit_event(
            event="mosaic.processed",
            resource={
                "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
                "project_name": project_name,
                "mosaic_id": str(mosaic_id),
            },
            payload={
                "project_name": project_name,
                "project_base_path": project_base_path,
                "mosaic_id": mosaic_id,
                "total_batches": total_batches,
            }
        )
    
    return {
        "mosaic_id": mosaic_id,
        "batch_id": batch_id,
        "processed_count": processed_count,
        "total_batches": total_batches,
        "all_processed": processed_count >= total_batches,
    }

@flow(name="process_mosaic_flow")
def process_mosaic_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    grid_size_x: int,
    grid_size_y: int,
    tile_file_pattern: str
):
    """
    Orchestrate batch processing for a mosaic.
    Triggers batch flows and monitors completion via flag files.
    """
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id}"
    state_path = mosaic_path / "state"
    state_path.mkdir(parents=True, exist_ok=True)
    
    # Discover all batches
    # For each column (grid_size_x), create a batch with grid_size_y tiles
    batches = []
    for batch_id in range(grid_size_x):
        # Collect tiles for this batch (grid_size_y tiles)
        batch_tiles = []
        for row in range(grid_size_y):
            tile_index = batch_id * grid_size_y + row
            # Find tile file matching pattern and tile_index
            # (implementation depends on file naming convention)
            tile_file = find_tile_file(tile_file_pattern, tile_index)
            if tile_file:
                batch_tiles.append(tile_file)
        
        if batch_tiles:
            batches.append((batch_id, batch_tiles))
    
    logger.info(f"Discovered {len(batches)} batches for mosaic {mosaic_id}")
    
    # Trigger batch flows (they run independently)
    batch_futures = []
    for batch_id, file_list in batches:
        future = process_tile_batch_flow.submit(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            file_list=file_list
        )
        batch_futures.append((batch_id, future))
    
    # Monitor batch completion via flag files
    # Wait for all batches to be processed
    while True:
        batch_processed_files = list(state_path.glob("batch-*.processed"))
        processed_count = len(batch_processed_files)
        
        if processed_count >= len(batches):
            logger.info(f"All {processed_count} batches processed for mosaic {mosaic_id}")
            break
        
        # Update Artifact table with progress
        update_artifact_table(
            project_name=project_name,
            mosaic_id=mosaic_id,
            processed_batches=processed_count,
            total_batches=len(batches)
        )
        
        # Wait a bit before checking again
        import time
        time.sleep(10)  # Poll every 10 seconds
    
    # All batches processed, now determine coordinates and stitch
    coordinates = determine_mosaic_coordinates_flow(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id
    )
    
    # Stitch mosaic
    stitched = stitch_mosaic_flow(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        coordinates=coordinates
    )
    
    return stitched

@flow(name="stitch_mosaic_flow")
def stitch_mosaic_flow(
    mosaic_id: str,
    tile_paths: List[str],
    coordinates: Dict,
    output_base_path: str,
    upload_queue=None,
    slack_config: Optional[Dict] = None
):
    """
    Stitch mosaic and send stitched image to Slack.
    """
    # Load coordinates and stitch
    stitched_enface = stitch_enface_images_task(tile_paths, coordinates)
    stitched_volumes = stitch_3d_volumes_task(tile_paths, coordinates)
    
    # Apply mask
    masked_enface = apply_mask_task(stitched_enface)
    masked_volumes = apply_mask_task(stitched_volumes)
    
    # Save stitched outputs
    aip_path = save_stitched_enface_task(
        masked_enface, output_base_path, mosaic_id
    )
    volume_paths = save_stitched_volumes_task(
        masked_volumes, output_base_path, mosaic_id
    )
    
    # Queue uploads (async, non-blocking)
    if upload_queue:
        queue_upload_stitched_volumes_task.submit(
            volume_paths,
            f"s3://bucket/stitched/{mosaic_id}/",
            upload_queue
        )
    
    # Send stitched image to Slack (async, non-blocking)
    if slack_config:
        notify_slack_task.submit(
            f"🎨 Stitched mosaic {mosaic_id} completed",
            image_path=aip_path,  # Send AIP image
            slack_config=slack_config
        )
    
    return {
        "enface": masked_enface,
        "volumes": masked_volumes
    }
```

### 5.3 Configuration Management

**Configuration File Structure**:

```yaml
# config.yaml
processing:
  surface_method: "find"
  depth: 80
  overlap: 50
  mask_threshold: 55
  
batch_processing:
  grid_size_x: 14  # Number of batches (columns) per mosaic
  grid_size_y: 31  # Number of tiles per batch (rows, shared between normal and tilted)
  # Total tiles per mosaic = grid_size_x * grid_size_y
  
paths:
  data_root: "/path/to/data"
  output_base: "/path/to/output"
  compressed_base: "/path/to/compressed"  # Separate directory/disk for compressed files
  cloud_upload_path: "s3://bucket/path"
  
resources:
  batch_processing_workers: 16  # Workers for batch flows
  stitching_workers: 4
  registration_workers: 2
  
cloud:
  provider: "s3"  # or "gcs", "azure"
  bucket: "oct-data"
  region: "us-east-1"
  upload:
    max_concurrent: 5  # Maximum concurrent uploads
    cli_tool: "aws"  # CLI tool for uploads (aws, gsutil, azcopy)
    cli_base_args: ["s3", "cp"]  # Base arguments for CLI tool
    
slack:
  enabled: true
  webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"  # For webhook method
  bot_token: "xoxb-your-bot-token"  # For bot API method
  channel: "#oct-processing"  # Slack channel for notifications
  milestones:
    - 0.25  # 25% completion
    - 0.50  # 50% completion
    - 0.75  # 75% completion
    - 1.0   # 100% completion
  send_stitched_images: true  # Send stitched images to Slack
```

## 6. Performance Considerations

### 6.1 Parallelization Strategy

- **Batch Processing**: All batches in a mosaic processed in parallel (grid_size_x batches)
- **Within Batch**: Archive and spectral_to_complex run in parallel (both synchronous I/O)
- **Event-Driven Processing**: Complex-to-processed and upload flows run independently via events
- **Mosaic Processing**: Sequential (wait for all batches → coordinates → stitching)
- **Slice Processing**: Parallel mosaics, then sequential registration
- **Multi-Slice**: Parallel slices, then sequential stacking

**Resource Optimization**:
- Batch processing reduces flow overhead (one flow per batch instead of per tile)
- Event-driven architecture allows independent scaling of processing stages
- Flag file-based state management enables efficient progress tracking without polling overhead

### 6.2 Resource Optimization

- **Memory Management**: Use dask arrays for large data, lazy evaluation
- **Disk I/O**: Minimize intermediate writes, use efficient formats (NIfTI)
- **Network**: Batch cloud uploads, use compression
- **CPU**: Balance parallelism with available cores

### 6.3 Caching Strategy

- Cache surface finding results (same for all modalities)
- Cache coordinate calculations
- Use Prefect's task result caching for idempotent operations
- Cache file metadata to avoid repeated reads

## 7. Testing Strategy

### 7.1 Unit Tests

- Test individual task functions
- Test data conversion functions
- Test coordinate calculation
- Test stitching algorithms

### 7.2 Integration Tests

- Test complete tile batch processing flow
- Test event-driven flows (complex_to_processed, upload_to_linc)
- Test flag file state management
- Test mosaic batch monitoring and completion detection
- Test mosaic stitching flow
- Test slice registration flow
- Test with sample data

### 7.3 End-to-End Tests

- Test complete experiment flow
- Test error handling and recovery
- Test with real data samples
- Validate output correctness

## 8. Future Enhancements

### 8.1 Scalability

- Support distributed processing across multiple hosts
- Use Prefect's distributed execution capabilities
- Implement load balancing

### 8.2 Monitoring and Observability

- Integrate with monitoring tools (Prometheus, Grafana)
- Set up custom dashboards
- Implement alerting for failures
- Track processing metrics

### 8.3 Optimization

- Profile and optimize slow tasks
- Implement incremental processing
- Add GPU acceleration where applicable
- Optimize data formats and compression

## 9. Conclusion

This design provides a comprehensive Prefect-based workflow orchestration system for OCT data processing. The system uses **batch processing** to optimize resource usage (processing grid_size_y tiles per batch instead of individual tiles) and an **event-driven architecture** to decouple processing stages. Flows communicate via Prefect events rather than direct subflow calls, enabling independent scaling and resilience.

**Key Architectural Decisions**:
- **Batch Processing**: Reduces resource overhead by processing tiles in batches rather than individually
- **Event-Driven**: Downstream flows triggered by events, not subflows, enabling decoupling and independent scaling
- **State Management**: Flag files track batch completion, enabling idempotent operations and recovery
- **Artifact Tracking**: Mosaic-level progress tracked in Artifact tables for monitoring and triggering

The system is designed to be:
- **Scalable**: Handle large datasets with many tiles and slices through batch processing
- **Resource-Efficient**: Batch processing reduces flow overhead and resource consumption
- **Resilient**: Event-driven architecture allows failed downstream flows to retry without blocking upstream flows
- **Observable**: Flag files and Artifact tables provide clear state tracking and progress monitoring
- **Maintainable**: Clear separation between batch processing, event-driven flows, and state management
- **Flexible**: Easy to add new event listeners and processing stages without modifying existing flows

