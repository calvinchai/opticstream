# Distributed OCT Data Acquisition & Real-Time Processing System

> Audience: advanced users and maintainers who need a detailed view of the
> distributed OCT pipeline. For a high-level introduction to OpticStream and
> its components, see the
> [project overview](concepts/project_overview.md) and
> [architecture](developer/architecture.md) pages.

## 1. Overview

This document describes the design of a distributed, event-driven data
acquisition and real-time processing system for Optical Coherence
Tomography (OCT) data. The system uses **Prefect** as the workflow
orchestration engine and supports scalable, fault-tolerant processing
across tiles, mosaics, slices, and whole datasets.

The primary goals of the system are:

* Efficient ingestion and integrity validation of raw OCT data
* Near–real-time processing with minimal redundant I/O
* Clear separation of processing stages with event-based coordination
* Scalable execution across heterogeneous compute hosts
* Transparent progress monitoring, QC reporting, and data publication

---

## 2. High-Level Architecture

### 2.1 Core Components

* **Prefect Server & Scheduler**

  * Runs on the primary processing host (Zircon)
  * Manages flow deployments, event triggers, state, and artifacts

* **Processing Workers**

  * Zircon: high I/O, large-memory tasks (tile processing, mosaics)
  * Auxiliary hosts: compute-heavy, low-I/O tasks (e.g., registration)

* **Event Bus (Prefect Events)**

  * Coordinates downstream workflows when upstream flows complete

* **Storage Backends**

  * Local high-speed SSDs (temporary processing)
  * External raw data archiving drives
  * DANDI / LINC s3 storage 

---

## 3. Data Processing Flow

Processing is organized hierarchically: **Tile → Mosaic → Slice → All-Slices**.

### 3.1 Tile-Level Processing

Each tile represents the smallest independently processed unit in the data hierarchy. Tiles are the atomic data unit - the fundamental building blocks that compose mosaics.

#### Inputs

* Spectral raw data **or** complex data
* File naming convention encodes acquisition metadata (parsed at ingest)

#### Tile Processing Steps

1. **Metadata Parsing**

   * Extract slice, mosaic, tile index, illumination

2. **Integrity & Compression**

   * Compute SHA-256 checksum
   * Compress raw tile data
   * Ensure single-read semantics where possible

3. **Spectral → Complex Conversion**

   * When input is spectral raw data: Performed in MATLAB to convert spectral data to complex format
   * When input is already complex data: Files are soft linked to `complex/` directory instead of converting
   * Output stored in `complex/`
   * See section 3.2 for details on batch processing optimization

4. **Complex → 3D Volumes Conversion**

   * Convert complex tiles to 3D volumes (dBI, O3D, R3D modalities)
   * Performed in MATLAB
   * See section 3.2 for details on batch processing optimization

5. **Surface Finding**

   * Automatic surface detection from intensity data
   * Surface finding method can be configured (e.g., "find" for automatic detection)

6. **Enface Image Generation**

   * Generate 2D enface images from 3D volumes using surface information
   * Multiple enface modalities: AIP (Average Intensity Projection), MIP (Maximum Intensity Projection), orientation, retardance, birefringence
   * Surface maps generated for visualization

7. **Archival Upload**

   * Compressed raw data uploaded to DANDI/LINC
   * Uploads are handled by a dedicated, event-triggered flow (see Section 11 for upload strategy details)

#### MATLAB Integration

* MATLAB is invoked via command-line interface from Python
* MATLAB functions handle spectral-to-complex and complex-to-processed conversions
* Data flow: Python → MATLAB → Python (processed tiles)
* Currently executed in MATLAB (future Python migration planned)
* Future migration to Python-native implementations will eliminate the need for batch processing optimization

#### QC

* Validate surface finding overlap with intensity images
* Verify processing quality at tile level
* QC images emitted as Prefect artifacts and Slack notifications

#### Outputs

* Complex tile data
* 3D volumes (dBI, O3D, R3D)
* Enface images (multiple modalities)
* Surface maps
* Checksum metadata
* Per-tile QC artifacts
* Upload event + flag file

---

### 3.2 Batch-Level Processing

Batch processing is an implementation optimization used specifically for MATLAB operations. It is **not** a data hierarchy level - tiles remain the atomic data unit. The data hierarchy is **Tile → Mosaic → Slice → All-Slices**. The details are discussed later in this documentation.


---

### 3.3 Mosaic-Level Processing

Triggered once **all tiles in a mosaic** complete tile-level processing. A mosaic contains all tiles for a given slice and illumination type (normal or tilted).

#### First Slice Processing

For each illumination type of the first slice(mosaic_001 for normal, mosaic_002 for tilted, maybe more type of illumination), additional processing steps are required that are not needed for subsequent slices:

1. **Tile Coordinate Determination**
   * Determine tile positions and alignment for stitching
   * Generates coordinate template that is reusable for all mosaics of the same illumination type
   * For subsequent slices of the same illumination type, the template from the first slice is reused
   * See Section 13.1 for detailed algorithms and methods

2. **Focus Finding**
   * Determine optimal focus plane for 3D volume stitching
   * Focus finding requires accurate surface information, so it uses an unfiltered version of the surface data
   * QC validation: verify focus finding overlap with intensity images
   * See Section 15 for detailed focus finding algorithms

#### 2D Enface Mosaic Stitching

1. **Template Application**
   * Apply coordinate template to current mosaic tiles
   * For first slice: use newly generated template
   * For subsequent slices: reuse template from first slice of same illumination type
   * Generate tile information files for each enface modality

2. **Stitch Enface Modalities**
   * Stitch all 2D enface modalities: AIP, MIP, orientation, retardance, birefringence, surface
   * Each modality is stitched independently using the same coordinate template
   * Generate overlap QC images to verify stitching quality
   * Detailed stitching tool and algorithm are discussed later in this document

3. **Mask Generation and Application**
   * Generate mask from stitched AIP using threshold-based approach
   * Apply mask to all stitched enface outputs
   * Mask removes background/noise regions

4. **Output Generation**
   * Save stitched enface images in multiple formats (NIfTI, JPEG)
   * Upload stitched 2D mosaics to cloud storage

#### 3D Volume Mosaic Stitching

1. **Focus Plane Application**
   * For first slice: use focus plane determined during first slice processing
   * For subsequent slices: reuse focus plane from first slice of same illumination type
   * Apply focus plane for optimal 3D volume alignment

2. **Stitch 3D Volumes**
   * Stitch 3D volume modalities: dBI, O3D, R3D
   * Use coordinate template from 2D stitching
   * Apply mask to stitched volumes
   * Detailed stitching tool and algorithm are discussed later in this document

3. **Upload Stitched Volumes**
   * Upload stitched 3D volumes to cloud storage
   * Volumes stored in appropriate format for downstream analysis

#### QC

* Focus finding overlap with intensity (first slice only)
* Stitched 2d modalities
* Mask

---

### 3.4 Slice-Level Processing

Triggered once **all mosaics in a slice** are complete. Each slice contains two (or more in future) mosaics: one with normal illumination and one with tilted illumination.

#### Registration Process

1. **Thruplane Registration**
   * Register normal and tilted illumination mosaics to combine orientations
   * Uses MATLAB-based registration algorithm
   * Accounts for tilt angle (gamma parameter) between illuminations
   * Compute-heavy but low I/O, suitable for offloading to auxiliary hosts

2. **3D Orientation Computation**
   * Combines information from both illumination angles
   * Generates 3D axis representations (normalization needed)
   * Generate RGB visualization of 3D axis orientation
   
3. **RGB 3D Axis Visualization**
   * Provides visual representation of fiber orientation in 3D space
   * Useful for quality control and visualization

#### Outputs

* Registered slice-level 2D/3D data 
* thru-plane and in-plane data in .mat
* 3d axis data in nifti
* orientation images in jpeg(thru-plane, in-plane, 3D axis)

---

### 3.5 All-Slices Processing

Currently manual:

* Stack 2D mosaics across slices
* Stack 3D volumes across slices

---

## 4. Folder Structure

### 4.1 Project Directory Structure

```
project/
│  ├─ mosaic-{mosaic_id:02d}/
│  │  ├─ complex/      # complex tiles (symlinked if needed)
│  │  ├─ processed/    # intermediate processed data (SSD)
│  │  ├─ stitched/     # outputs (symlinked to dandiset)
│  │  ├─ state/        # flag files for batch tracking
│  │  │  ├─ batch-001.started
│  │  │  ├─ batch-001.archived
│  │  │  ├─ batch-001.processed
│  │  │  ├─ batch-001.uploaded
│  │  │  └─ ...
│  │  └─ archived/     # raw data (symlinked to dandiset raw)
│  ├─ registered/
│  ├─ state/
│  ├─ focus-normal.nii        # focus finding results (first slice)
│  ├─ focus-tilted.nii        # focus finding results (first slice)
│  └─ tilemap-normal.j2     # tile coordinate map
│  └─ tilemap-tilted.j2     # tile coordinate map
```

### 4.2 DANDI/LINC Storage Structure

```
DANDISET/
├─ rawdata/...         # raw compressed tiles
│  └─ sub-{subject_id}/
│     └─ sample-slice-{slice_id:03d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii.gz
└─ derivative/
   └─ sub-{subject_id}/
      └─ mosaic_{mosaic_id:03d}_{modality}.nii
      └─ .../*.ome.zarr (for large volumes)
```

### 4.3 Symlink Strategy

Symlinks are used extensively to balance I/O performance and long-term storage:

* **Performance**: Processing occurs on high-speed SSDs (local paths)
* **Archival**: Final outputs symlinked to DANDI/LINC storage for long-term preservation
* **Efficiency**: Avoids data duplication while maintaining fast access during processing
* **Transparency**: Symlinks make data appear in both locations without copying

Symlinks are created:
* From processing directories to DANDI/LINC storage for final outputs
* From DANDI/LINC storage to processing directories for inputs (when needed)

---

## 5. Workflow Orchestration & Event Model

### 5.1 Flow Granularity

Flows operate at the fundamental data hierarchy levels: **Tile** and **Mosaic** (see Section 3.1 for data hierarchy details). 

* **Tile-Level Flows**: Handle ingestion, conversion, QC, upload triggers. When MATLAB processing is required, tiles are grouped into batches for efficiency, but tiles remain the atomic processing unit.
* **Mosaic-Level Flows**: Handle stitching, coordinate determination, focus finding, mosaic uploads. Operate on complete mosaics once all tiles are processed.
* **Slice-Level Flows**: Handle registration between normal and tilted illumination mosaics.
* **Upload Flows**: Always isolated and event-triggered, never block compute pipelines.

### 5.2 Event-Driven Architecture

The system uses an event-driven architecture where downstream flows are started via **Prefect events**, not synchronous waits. This decouples processing stages and enables independent scaling and resilience.

#### Benefits of Event-Driven Architecture

* **Decoupling**: Flows don't need to wait for downstream processing to complete
* **Scalability**: Event-driven flows can scale independently based on workload
* **Resilience**: Failed downstream flows don't block upstream flows - they can retry independently
* **Flexibility**: Easy to add new event listeners without modifying existing flows
* **Non-blocking**: Long-running operations (uploads, QC) don't block compute-intensive processing

Synchronous processing is only used when direct I/O dependencies exist (e.g., reading source files, writing intermediate results).

Downstream flows are automatically triggered when upstream flows emit completion events. See Section 6 for detailed event naming conventions, payload structure, and deployment triggers.



---

## 6. Event Design

### 6.1 Event Naming Convention

Events follow a consistent hierarchical naming pattern: `linc.oct.{hierarchy}.{state}`

* **Format**: `linc.oct.{hierarchy}.{state}`
* **Hierarchy levels**: `tile`, `batch`, `mosaic`, `slice`
* **Standardized states**: `started`, `archived`, `processed`, `uploaded`, `ready`, `stitched`, `registered`

This consistent naming convention enables clear identification of event hierarchy and state, making event routing and filtering straightforward.

### 6.2 Event Catalog

#### Batch-Level Events
* `linc.oct.batch.ready` - Batch of tiles detected (to start converting to complex data)
* `linc.oct.batch.processed` - Batch of tiles processed (complex-to-processed conversion complete)
  * Triggers: state management updates (emit mosaic.ready when all batches are done)
* `linc.oct.batch.archived` - Batch of tiles archived and compressed
  * Triggers: upload flow, state management updates
* `linc.oct.batch.uploaded` - Batch uploaded to LINC storage
  * Triggers: upload completion handlers

#### Mosaic-Level Events

* `linc.oct.mosaic.ready` - All tiles in mosaic processed (all batches complete)
  * Triggers: mosaic stitching flow 
* `linc.oct.mosaic.stitched` - All modalities (enface or volume) stitched
  * Triggers: mosaic state management (emit slice.ready when all mosaics are done for 2d), upload flow
#### Slice-Level Events

* `linc.oct.slice.ready` - Both mosaics in slice are stitched, ready for registration
  * Triggers: slice registration flow
* `linc.oct.slice.registered` - Slice registration complete
  * Triggers: slice state management flow, upload flow

### 6.3 Event Payload Structure

Event payloads carry contextual information needed by downstream flows:

* **Project Context**: `project_name`, `project_base_path`
* **Entity Identifiers**: `mosaic_id`, `slice_number`, `batch_id`
* **Processing Metadata**: `total_batches`, `grid_size_x`, `grid_size_y`
* **File Paths**: Paths to processed files, coordinate files, templates
* **Output Information**: Paths to generated outputs, artifact keys

Payloads are automatically extracted by event-driven deployments using Jinja2 templates, enabling parameter passing from events to flows.

### 6.4 Deployment Triggers

Flows are deployed with event triggers that automatically start flows when matching events are emitted:

* **Event Matching**: Deployments listen for specific event names
* **Parameter Extraction**: Event payloads are automatically mapped to flow parameters
* **Automatic Triggering**: No manual intervention needed - flows start automatically when events are emitted


---

## 7. State Management & Observability

The system uses multiple complementary mechanisms for tracking processing state and providing observability: flag files (authoritative state), Prefect Artifacts (human-readable progress), Slack notifications (real-time updates), and event-driven state management flows (automatic state updates).

### 7.1 Flag Files

Flag files serve as the authoritative source of truth for processing state. They enable idempotent operations, crash recovery, and efficient state checking without database queries.

#### Flag File Location and Naming

Flag files are stored in a `state/` directory at `{project_base_path}/mosaic-{mosaic_id}/state/`. The directory structure is shown in Section 4.1.

Flag files follow the naming pattern: `{hierarchy}-{id}.{state}` (e.g., `batch-0.started`). Note that flag files do not include the `linc.oct` prefix used in event names (see Section 6.1).

**Batch-Level Flag Files** (for MATLAB batch processing tracking):
* `batch-{batch_id}.started` - Batch processing initiated
* `batch-{batch_id}.archived` - Batch archived and compressed
* `batch-{batch_id}.processed` - Batch processed by MATLAB (complex-to-processed conversion complete)
* `batch-{batch_id}.uploaded` - Batch uploaded to cloud storage

**Mosaic-Level State**:
* Mosaic completion determined by checking if all batches have `.processed` flag files
* mosaic-{mosaic_id.started}
* mosaic-{mosaic_id.stitched}
* mosaic-{mosaic_id.volume_stitched}
* mosaic-{mosaic_id.volume_uploaded}

**Slice-Level State**:
* Slice read determined by checking both mosaic states
* slice-{slice_id}.started 
* slice-{slice_id}.registered
* slice-{slice_id}.uploaded

#### Flag File Lifecycle and Benefits

1. **Creation**: Flag files are created at key processing milestones
2. **Checking**: Flows check flag files before processing to avoid duplicate work (idempotency)
3. **Counting**: Mosaic and slice flows count flag files to determine completion status
4. **Persistence**: Flag files persist across flow runs, enabling recovery after crashes

### 7.2 Prefect Artifacts

Prefect Artifacts provide human-readable progress reports visible in the Prefect UI. They complement flag files by providing formatted, visual progress tracking.

**Mosaic Progress Artifacts** (`{project_name}_mosaic_{mosaic_id}_progress`):
* Progress table showing batch state counts (started, archived, processed, uploaded)
* Progress percentage calculation
* Milestone tracking (25%, 50%, 75%, 100% completion)
* Timestamp of last update

**Slice Progress Artifacts** (`{project_name}_progress`):
* Progress table for mosaics
* Overall slice status

Artifacts are updated automatically by state management flows (see Section 7.4) and are visible in Prefect UI under flow run artifacts.

### 7.3 Notifications (Slack)

Milestone-based notifications provide real-time updates on processing progress.

**Notification Milestones**:
* **25% Tile Completion**: When 25% of tiles in a mosaic are processed
* **50% Tile Completion**: When 50% of tiles in a mosaic are processed
* **75% Tile Completion**: When 75% of tiles in a mosaic are processed
* **100% Tile Completion**: When all tiles in a mosaic are processed
* **Mosaic Stitching Complete**: When mosaic stitching is complete (includes stitched image preview)
* **Slice Registration Complete**: When slice registration is complete

**Notification Content**:
* Processing milestone information (percentage, counts)
* QC image previews (stitched mosaics, registration results)
* Links to uploaded dandiset assets
* Error notifications for failures requiring attention

Notifications are sent asynchronously and don't block processing flows.

### 7.4 State Management Flows

State management flows are triggered by events (see Section 6 for event details), not called as subflows. This decoupled approach ensures state management doesn't block processing flows:

* **Batch State Flow**: 
  * Scans flag files to count batch states
  * Updates Prefect Artifacts with progress
  * Checks if all batches are processed
  * Emits mosaic completion event when all batches complete

* **Slice State Flow**: 
  * Checks state of both mosaics in slice
  * Updates Prefect Artifacts with slice progress
  * Checks if both mosaics are stitched
  * Emits slice ready event when both mosaics complete

---

## 8. Configuration & Secrets Management

### 8.1 Global Secrets

Secrets are managed via **Prefect Blocks** to ensure secure, centralized credential management:

* **DANDI Credentials**: API keys and authentication tokens for DANDI archive access
* **Slack Webhooks**: Webhook URLs for Slack notifications


Secrets are stored securely in Prefect and accessed by flows at runtime. They are never hardcoded in flow definitions or committed to version control.

### 8.2 Project-Level Parameters

Project-level parameters define the structure and configuration of a processing run. they should be accesed with prefect variable. see https://docs.prefect.io/v3/concepts/variables. they should be retrieved with project_name as key, and be used as default parameter to the flow.

* **`project_name`**: Project identifier used throughout processing
* **`project_base_path`**: Base filesystem path for project data
* **`grid_size_x_normal/grid_size_x_tilted`**: Number of batches (columns) per mosaic - determines how tiles are organized for MATLAB batch processing
* **`grid_size_y`**: Number of tiles per batch (rows) - determines batch size for MATLAB processing
* **`tile_overlap`**: Overlap between tiles in pixels (default: 20.0)
* **`mask_threshold_normal`**: Threshold for mask generation and coordinate processing for normal illumination (default: 60.0)
* **`mask_threshold_tilted`**: Threshold for mask generation and coordinate processing for tilted illumination (default: 55.0)
* **`scan_resolution_3d`**: Scan resolution for 3D volumes [x, y, z] in millimeters (default: [0.01, 0.01, 0.0025])

Parameters are resolved dynamically based on project context. They can be:
* Specified in event payloads
* Retrieved from configuration files
* Stored in Prefect Artifacts
* Passed as flow parameters

### 8.3 Processing Parameters

Processing parameters control algorithmic behavior:

* **Surface Finding Method**: Method for surface detection ("find" for automatic, constant value, or file path)
* **Depth**: Depth below surface for enface window in pixels (default: 80)
* **Gamma**: Tilt angle parameter for registration (default: -15.0)
* **Mask File Type**: Type of mask to use for registration ("aip", "mip", or "" for no mask)
* **Force Refresh Coordinates**: Force coordinate determination even if not first slice (default: False)

These parameters can be customized per project or per processing run based on data characteristics.

---

## 9. Processing Hosts & Deployment Strategy

### 9.1 Host Architecture

The system uses a hybrid host strategy to optimize for different workload characteristics:

* **Zircon (Primary Host)**
  * Primary data processing host
  * High I/O workflows (tile processing, mosaics, file operations)
  * Large-memory tasks (stitching, volume processing)
  * Prefect server and scheduler
  * Local high-speed SSDs for processing

* **Auxiliary Hosts**
  * Compute-heavy, low-I/O tasks (e.g., registration)
  * Deployments connected to Zircon Prefect server
  * Can offload compute-intensive work from Zircon
  * Useful for tasks that don't require high I/O bandwidth

This hybrid strategy maximizes throughput while minimizing contention on shared storage and network resources.

### 9.2 Work Pool Strategy

Different work pools are configured for different task types to optimize resource allocation:

* **Tile Processing Pool**: High parallelism, CPU-intensive, moderate memory
  * Handles MATLAB batch processing, spectral-to-complex conversion
  * Many concurrent workers for parallel tile processing

* **Stitching Pool**: Moderate parallelism, memory-intensive
  * Handles mosaic stitching operations
  * Fewer workers due to memory requirements

* **Registration Pool**: Low parallelism, CPU-intensive, high memory
  * Handles slice registration (compute-heavy)
  * Can run on auxiliary hosts

* **Upload Pool**: High parallelism, I/O-bound
  * Handles cloud storage uploads
  * Many concurrent workers for parallel uploads

Work pools enable fine-grained control over resource allocation and can be tuned based on workload characteristics.

### 9.3 Agent Deployment

Prefect agents run on processing hosts and pull work from work pools:

* **Zircon Agent**: Pulls from tile processing, stitching, and upload pools
* **Auxiliary Host Agents**: Pull from registration pool and other compute-heavy pools
* **Connection**: All agents connect to Zircon Prefect server
* **Reliability**: Agents run as systemd services or Docker containers for auto-restart

Agents automatically pull work from their assigned pools and execute flows on the local host, enabling distributed execution while maintaining centralized orchestration.

---

## 10. MATLAB Batch Processing (Implementation Detail)

**Note**: See section 3.2 for a high-level overview of batch-level processing. This section provides additional implementation details.

### 10.1 Data Hierarchy vs Batch Processing

Batches are a temporary grouping used only during MATLAB processing for efficiency.

### 10.2 Grid Configuration

The grid configuration defines how tiles are organized for MATLAB batch processing:

* **`grid_size_x`**: Number of batches (columns) per mosaic
  * Determines how many batches are needed to process all tiles in a mosaic
  * Each batch contains `grid_size_y` tiles

* **`grid_size_y`**: Number of tiles per batch (rows)
  * Determines batch size for MATLAB processing
  * Normal and tilted illuminations share the same `grid_size_y`

* **Total tiles per mosaic**: `grid_size_x × grid_size_y`

### 10.3 Why Batches Are Used

Batch processing is used specifically because MATLAB processing is more
efficient when processing multiple tiles together:

* Reduces MATLAB startup overhead by processing many tiles per session
* Enables better use of MATLAB's parallel operations
* Reduces the number of MATLAB processes and improves resource utilization

### 10.4 Batch Organization and State Tracking

Tiles are organized into batches based on their position in the acquisition
grid:
* Each column (`grid_size_x`) becomes a batch
* Each batch contains `grid_size_y` tiles (rows)
* Batches are processed in parallel for efficiency

Batch completion is tracked using flag files stored in the `state/`
directory. See Section 7.1 for comprehensive details on flag file
structure, lifecycle, and benefits. The batch-level flag files
(`batch-{batch_id}.started`, `.archived`, `.processed`, `.uploaded`) enable
progress tracking, idempotency, and crash recovery for MATLAB batch
processing.

### 10.5 Post-MATLAB Processing

Once MATLAB batch processing completes, the system operates on individual
tiles again for downstream processing (stitching, QC, coordinate
determination). The batch grouping is only relevant during MATLAB
processing and exists purely as an implementation optimization; tiles
remain the atomic data unit in the hierarchy.
---

## 11. Upload Strategy

### 12.1 Separate Upload Flows (Event-Driven, Non-Blocking)

Uploads are handled by dedicated, event-triggered flows that run independently of compute pipelines:
* **Event-Triggered**: Upload flows are triggered by events (e.g., `linc.oct.batch.uploaded`)
* **Non-Blocking**: Upload flows don't block compute-intensive processing
* **Isolated**: Upload failures don't affect compute pipeline
* **Independent Retry**: Upload flows can retry independently without blocking upstream processing

### 12.2 Upload Queue Management

Upload queue management enables controlled, concurrent uploads:
* **Concurrency Control**: Maximum number of concurrent uploads (e.g., 5)
* **Queue-Based**: Files are queued for upload, processed by background workers
* **Non-Blocking Enqueue**: Adding files to upload queue returns immediately
* **Background Processing**: Actual uploads happen in background threads/processes

This prevents uploads from overwhelming network bandwidth or cloud storage APIs.

### 12.3 Cloud Storage Integration (DANDI/LINC)

* **DANDI Archive**: Processed data uploaded to DANDI archive for long-term preservation
* **LINC Storage**: Raw compressed tiles uploaded to LINC storage
* **Symlinks**: Final outputs symlinked to DANDI/LINC storage locations
* **Metadata**: Upload events include metadata for tracking and verification

### 12.4 Upload Retry and Error Handling

* **Retry Logic**: Upload failures are retried automatically with exponential backoff
* **Error Logging**: Upload errors are logged for debugging and monitoring
* **Failure Notification**: Critical upload failures trigger notifications
* **Resumability**: Partial uploads can be resumed using flag files

---

## 12. MATLAB Integration

### 13.1 MATLAB Invocation

MATLAB is invoked from Python via command-line interface:
* **Batch Mode**: MATLAB runs in batch mode (non-interactive) for automation
* **Function Calls**: Python constructs MATLAB function calls with batch of tiles
* **Path Management**: MATLAB paths are configured to include required toolboxes and functions
* **Output Capture**: MATLAB output is captured and logged for debugging

### 13.2 Batch Processing Requirement

MATLAB processes tiles in batches (not individually) for efficiency:
* **Spectral-to-Complex**: Multiple tiles passed to MATLAB function for batch conversion
* **Complex-to-Processed**: Multiple tiles passed to MATLAB function for batch processing
* **Reduced Overhead**: Batching reduces MATLAB startup overhead per tile

This is why batch-level processing exists - it's an optimization for MATLAB efficiency, not a data hierarchy level.

### 13.3 MATLAB Functions Used

High-level MATLAB functions (not specific implementation details):
* **Spectral-to-Complex**: Converts spectral raw data to complex format
* **Complex-to-Processed**: Converts complex data to 3D volumes and enface images
* **Surface Finding**: Automatic surface detection from intensity data
* **Registration**: Thruplane registration for combining normal and tilted illuminations

### 13.4 Data Flow Between Python and MATLAB

* **Python → MATLAB**: Batches of tile file paths passed to MATLAB functions
* **MATLAB Processing**: MATLAB reads files, processes tiles, writes outputs
* **MATLAB → Python**: Processed tile files written to filesystem, Python reads results
* **File-Based Interface**: Communication via filesystem (no in-memory data transfer)

### 13.5 Future Migration Strategy

When MATLAB steps are migrated to Python-native implementations:
* Batch processing may no longer be necessary (Python can process tiles individually more efficiently)
* Data hierarchy remains Tile → Mosaic (no change to fundamental structure)
* Processing efficiency may improve (no MATLAB startup overhead)
* System becomes more maintainable (single language codebase)

---

## 13. Coordinate Determination & Stitching

### 13.1 Fiji-Based Coordinate Determination

Fiji (ImageJ) is used for initial tile alignment and coordinate determination:
* **Tile Configuration**: Fiji generates `TileConfiguration.txt` with initial tile positions
* **Overlap-Based**: Uses tile overlap information to align tiles
* **First Slice Only**: Coordinate determination runs only for first slice of each illumination type
* **Template Generation**: Coordinates are processed and converted to reusable templates

### 13.2 Template Generation and Reuse Strategy

Templates are generated once per illumination type and reused for all slices:
* **Template Generation**: Jinja2 templates generated from first slice coordinates
* **Template Reuse**: Subsequent slices of same illumination type reuse template
* **Efficiency**: Avoids redundant coordinate determination for each slice
* **Consistency**: Ensures consistent tile positioning across slices

Templates contain:
* Tile positioning information
* Scan resolution parameters
* Base directory paths (parameterized for reuse)

### 13.3 Stitching Process

#### 2D Enface Stitching

* **Template Application**: Apply coordinate template to current mosaic tiles
* **Modality Stitching**: Stitch each enface modality independently (AIP, MIP, orientation, retardance, birefringence, surface)
* **Mask Generation**: Generate mask from stitched AIP using threshold
* **Mask Application**: Apply mask to all stitched enface outputs
* **Output Formats**: Save in multiple formats (NIfTI, JPEG, TIFF)

#### 3D Volume Stitching

* **Focus Finding**: Determine optimal focus plane (first slice only)
* **Volume Stitching**: Stitch 3D volume modalities (dBI, O3D, R3D)
* **Template Reuse**: Use same coordinate template as 2D stitching
* **Mask Application**: Apply mask to stitched volumes

### 13.4 Mask Generation and Application

* **Threshold-Based**: Mask generated from stitched AIP using intensity threshold
* **Background Removal**: Mask removes background/noise regions
* **Consistent Application**: Same mask applied to all stitched modalities
* **Quality Control**: Mask quality validated as part of QC process

---

## 14. Tile Coordinate Determination Algorithms

*[This section will discuss the detailed algorithms and methods used for tile coordinate determination. Content to be added.]*

---

## 15. Focus Finding Algorithms

*[This section will discuss the detailed focus finding algorithms used to determine optimal focus planes for 3D volume stitching. Content to be added.]*

---

## 16. Stitching Tool and Algorithm

*[This section will discuss the detailed stitching tool and algorithm used for both 2D enface and 3D volume stitching. Content to be added.]*


## 17. Error Handling & Recovery

### 17.1 Retry Strategies

The system uses retry strategies at different levels:
* **Task-Level Retries**: Individual tasks can retry on failure with exponential backoff
* **Flow-Level Retries**: Flows can be configured to retry on failure
* **Event-Driven Retries**: Failed event-driven flows can be retried by re-emitting events
* **Upload Retries**: Upload operations retry with longer timeouts for network issues

### 17.2 Flag File-Based Recovery

Flag files enable recovery after crashes or failures. See Section 7.1 for comprehensive details on flag file structure and lifecycle. Key recovery mechanisms:

* **State Persistence**: Flag files persist across flow runs
* **State Checking**: Flows check flag files to determine what work has been completed
* **Resume from Last State**: System can resume from last completed state
* **Idempotent Operations**: Flows can safely rerun - flag files prevent duplicate work

### 17.3 Event Idempotency

Events and event-driven flows are designed to be idempotent:
* **Duplicate Events**: Multiple events with same payload are safe (idempotent)
* **State Checking**: Event-driven flows check state before processing
* **Flag File Protection**: Flag files prevent duplicate processing even if events are duplicated
* **Redundancy**: Multiple flows can emit same completion events for redundancy

### 17.4 Failure Handling at Different Levels

* **Tile-Level Failures**: Failed tiles don't block other tiles - processing continues
* **Batch-Level Failures**: Failed batches can be retried independently
* **Mosaic-Level Failures**: Failed mosaics trigger alerts but don't block slice processing
* **Slice-Level Failures**: Failed slices trigger alerts but don't block other slices
* **Upload Failures**: Upload failures don't affect compute pipeline - can retry independently

---

## 18. Quality Control (QC)

### 18.1 QC at Tile Level

Tile-level QC validates individual tile processing:
* **Surface Finding Validation**: Verify surface finding overlap with intensity images
* **Processing Quality**: Validate that processing completed successfully
* **QC Images**: Generate QC images for visual inspection
* **Artifact Generation**: Emit QC images as Prefect artifacts

### 18.2 QC at Mosaic Level

Mosaic-level QC validates stitching and alignment:
* **Stitching Consistency**: Verify stitching consistency across modalities
* **Focus Finding Validation**: Validate focus finding overlap with intensity (first slice)
* **Overlap QC Images**: Generate overlap QC images to verify tile alignment
* **Mask Quality**: Validate mask quality and coverage

### 18.3 QC Artifacts and Notifications

* **Prefect Artifacts**: QC images stored as Prefect artifacts for easy access
* **Slack Notifications**: QC images sent to Slack for real-time review
* **Progress Tracking**: QC status tracked in progress artifacts
* **Links**: Links to QC images included in notifications and artifacts

### 18.4 QC Image Generation

QC images are generated at key processing stages:
* **Tile Processing**: Surface finding validation images
* **Mosaic Stitching**: Overlap images, stitched mosaic previews
* **Registration**: Registration result visualizations, 3D axis images
* **Formats**: QC images in multiple formats (JPEG, PNG) for easy viewing

---

## 19. Future Extensions

### 19.1 MATLAB to Python Migration

* **Replace MATLAB Steps**: Migrate spectral-to-complex and complex-to-processed conversions to Python-native implementations
* **Eliminate Batch Processing**: Python can process tiles individually more efficiently, eliminating need for batch processing optimization
* **Maintainability**: Single language codebase improves maintainability
* **Performance**: Potential performance improvements by eliminating MATLAB startup overhead

### 19.2 Enhanced Retry Policies

* **Flag File-Based Retries**: Add retry policies that check flag-file state before retrying
* **Intelligent Retries**: Retry only failed work, not completed work
* **Exponential Backoff**: Implement exponential backoff for retries
* **Failure Analysis**: Analyze failure patterns to improve retry strategies

### 19.3 Real-Time Dashboarding

* **Beyond Prefect UI**: Integrate real-time dashboarding tools (e.g., Grafana, custom dashboards)
* **Processing Metrics**: Track processing metrics (throughput, latency, error rates)
* **Resource Monitoring**: Monitor resource utilization (CPU, memory, disk, network)
* **Custom Visualizations**: Custom visualizations for processing progress and QC results

---

## 20. Summary

This design provides a robust, scalable framework for distributed OCT data processing. The system is built on several key architectural principles:

### 20.1 Core Architecture Principles

* **Event-Driven Orchestration**: Downstream flows are triggered by events, not synchronous waits, enabling decoupling, scalability, and resilience
* **Hierarchical Data Organization**: Clear hierarchy of Tile → Mosaic → Slice → All-Slices, with tiles as the atomic data unit
* **MATLAB Batch Processing Optimization**: Batch processing exists as an implementation detail for MATLAB efficiency, not as a data hierarchy level
* **Flag File-Based State Management**: Flag files serve as the source of truth for processing state, enabling idempotency and crash recovery
* **Hybrid Host Strategy**: Optimize for different workload characteristics (high I/O vs compute-intensive)

### 20.2 Key Features

* **Scalability**: Handles large datasets with many tiles and slices through batch processing and parallel execution
* **Resilience**: Event-driven architecture allows failed downstream flows to retry without blocking upstream flows
* **Observability**: Flag files, Prefect Artifacts, and Slack notifications provide comprehensive progress monitoring
* **Flexibility**: Easy to add new event listeners and processing stages without modifying existing flows
* **Efficiency**: MATLAB batch processing optimization reduces overhead while maintaining clear data hierarchy
