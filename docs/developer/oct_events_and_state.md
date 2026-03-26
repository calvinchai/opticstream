---
title: OCT events and state management
---

# OCT events and state management

## Workflow orchestration and event model

### Flow granularity

Flows operate at the fundamental data hierarchy levels: **Tile** and **Mosaic** (see the OCT data flow doc for data hierarchy details).

* **Tile-level flows**: Handle ingestion, conversion, QC, upload triggers. When MATLAB processing is required, tiles are grouped into batches for efficiency, but tiles remain the atomic processing unit.
* **Mosaic-level flows**: Handle stitching, coordinate determination, focus finding, mosaic uploads. Operate on complete mosaics once all tiles are processed.
* **Slice-level flows**: Handle registration between normal and tilted illumination mosaics.
* **Upload flows**: Always isolated and event-triggered, never block compute pipelines.

### Event-driven architecture

The system uses an event-driven architecture where downstream flows are started via **Prefect events**, not synchronous waits. This decouples processing stages and enables independent scaling and resilience.

#### Benefits of event-driven architecture

* **Decoupling**: Flows don't need to wait for downstream processing to complete
* **Scalability**: Event-driven flows can scale independently based on workload
* **Resilience**: Failed downstream flows don't block upstream flows - they can retry independently
* **Flexibility**: Easy to add new event listeners without modifying existing flows
* **Non-blocking**: Long-running operations (uploads, QC) don't block compute-intensive processing

Synchronous processing is only used when direct I/O dependencies exist (e.g., reading source files, writing intermediate results).

Downstream flows are automatically triggered when upstream flows emit completion events.

## Event design

### Event naming convention

Events follow a consistent hierarchical naming pattern: `linc.opticstream.psoct.{hierarchy}.{state}`.

* **Format**: `linc.opticstream.psoct.{hierarchy}.{state}`
* **Hierarchy levels**: `batch`, `mosaic`, `slice`
* **Standardized states**: `started`, `archived`, `processed`, `uploaded`, `ready`, `stitched`, `registered`

This consistent naming convention enables clear identification of event hierarchy and state, making event routing and filtering straightforward.

### Event catalog (examples)

**Batch-level events**

* `linc.opticstream.psoct.batch.ready` - Batch of tiles detected (to start converting to complex data)
* `linc.opticstream.psoct.batch.complexed` - Batch complex data prepared
* `linc.opticstream.psoct.batch.processed` - Batch of tiles processed (complex-to-processed conversion complete)
  * Triggers: state management updates (emit `mosaic.ready` when all batches are done)
* `linc.opticstream.psoct.batch.archived` - Batch of tiles archived and compressed
  * Triggers: upload flow, state management updates
* `linc.opticstream.psoct.batch.uploaded` - Batch uploaded to LINC storage
  * Triggers: upload completion handlers

**Mosaic-level events**

* `linc.opticstream.psoct.mosaic.ready` - All tiles in mosaic processed (all batches complete)
  * Triggers: mosaic stitching flow (`opticstream.flows.psoct.mosaic_process_flow.process_mosaic_event_flow`)
* `linc.opticstream.psoct.mosaic.enface_uploaded` - Enface outputs uploaded
  * Triggers: mosaic/slice state updates and notification or downstream upload handlers
* `linc.opticstream.psoct.mosaic.volume_stitched` - Volume modalities stitched
  * Triggers: volume upload flow
* `linc.opticstream.psoct.mosaic.volume_uploaded` - Volume modalities uploaded
  * Triggers: upload completion handlers

**Slice-level events**

* `linc.opticstream.psoct.slice.ready` - Both mosaics in slice are stitched, ready for registration
  * Triggers: slice registration flow
* `linc.opticstream.psoct.slice.registered` - Slice registration complete
  * Triggers: slice state management flow, upload flow

### `MOSAIC_READY` and mosaic stitching

`process_mosaic_event_flow` requires `mosaic_ident` in the event payload. Stitching
parameters (grid sizes, masks, modalities, etc.) are taken from the project
config block (`PSOCTScanConfig`); the payload may override individual keys
(see `PROCESS_MOSAIC_FLOW_KWARGS_KEYS` in `opticstream.flows.psoct.utils`), same
idea as LSM strip flows.

Emitters include `mosaic_ident` (e.g. `state_management_flow`, `tile_batch_update_flow` via `emit_mosaic_psoct_event`).

### Event payload structure

Event payloads carry contextual information needed by downstream flows:

* **Project context**: `project_name`, `project_base_path`
* **Entity identifiers**: `mosaic_id`, `slice_id`, `batch_id`
* **Processing metadata**: `total_batches`, `grid_size_x`, `grid_size_y`
* **File paths**: Paths to processed files, coordinate files, templates
* **Output information**: Paths to generated outputs, artifact keys

Payloads are automatically extracted by event-driven deployments using Jinja2 templates, enabling parameter passing from events to flows.

### Deployment triggers

Flows are deployed with event triggers that automatically start flows when matching events are emitted:

* **Event matching**: Deployments listen for specific event names
* **Parameter extraction**: Event payloads are automatically mapped to flow parameters
* **Automatic triggering**: No manual intervention needed - flows start automatically when events are emitted

## State management and observability

The system uses multiple complementary mechanisms for tracking processing state and providing observability: flag files (authoritative state), Prefect Artifacts (human-readable progress), Slack notifications (real-time updates), and event-driven state management flows (automatic state updates).

For a detailed description of the PostgreSQL-backed project state store (locks, views vs mutable models, `open_*` / `read_*` / `peek_*` APIs), see the [state design](prefect_state_design.md) developer document.

### Flag files

Flag files serve as the authoritative source of truth for processing state. They enable idempotent operations, crash recovery, and efficient state checking without database queries.

#### Flag file location and naming

Flag files are stored in a `state/` directory at `{project_base_path}/mosaic-{mosaic_id}/state/`. The directory structure is shown in the OCT data flow doc.

Flag files follow the naming pattern: `{hierarchy}-{id}.{state}` (e.g., `batch-0.started`). Note that flag files do not include the `linc.opticstream.psoct` prefix used in event names.

**Batch-level flag files** (for MATLAB batch processing tracking):

* `batch-{batch_id}.started` - Batch processing initiated
* `batch-{batch_id}.archived` - Batch archived and compressed
* `batch-{batch_id}.processed` - Batch processed by MATLAB (complex-to-processed conversion complete)
* `batch-{batch_id}.uploaded` - Batch uploaded to cloud storage

**Mosaic-level state**

* Mosaic completion determined by checking if all batches have `.processed` flag files
* `mosaic-{mosaic_id}.started`
* `mosaic-{mosaic_id}.stitched`
* `mosaic-{mosaic_id}.volume_stitched`
* `mosaic-{mosaic_id}.volume_uploaded`

**Slice-level state**

* Slice readiness determined by checking both mosaic states
* `slice-{slice_id}.started`
* `slice-{slice_id}.registered`
* `slice-{slice_id}.uploaded`

#### Flag file lifecycle and benefits

1. **Creation**: Flag files are created at key processing milestones
2. **Checking**: Flows check flag files before processing to avoid duplicate work (idempotency)
3. **Counting**: Mosaic and slice flows count flag files to determine completion status
4. **Persistence**: Flag files persist across flow runs, enabling recovery after crashes

### Prefect artifacts

Prefect Artifacts provide human-readable progress reports visible in the Prefect UI. They complement flag files by providing formatted, visual progress tracking.

**Mosaic progress artifacts** (`{project_name}_mosaic_{mosaic_id}_progress`):

* Progress table showing batch state counts (started, archived, processed, uploaded)
* Progress percentage calculation
* Milestone tracking (25%, 50%, 75%, 100% completion)
* Timestamp of last update

**Slice progress artifacts** (`{project_name}_progress`):

* Progress table for mosaics
* Overall slice status

Artifacts are updated automatically by state management flows and are visible in Prefect UI under flow run artifacts.

### Notifications (Slack)

Milestone-based notifications provide real-time updates on processing progress.

**Notification milestones**:

* **25% tile completion**: When 25% of tiles in a mosaic are processed
* **50% tile completion**: When 50% of tiles in a mosaic are processed
* **75% tile completion**: When 75% of tiles in a mosaic are processed
* **100% tile completion**: When all tiles in a mosaic are processed
* **Mosaic stitching complete**: When mosaic stitching is complete (includes stitched image preview)
* **Slice registration complete**: When slice registration is complete

**Notification content**:

* Processing milestone information (percentage, counts)
* QC image previews (stitched mosaics, registration results)
* Links to uploaded dandiset assets
* Error notifications for failures requiring attention

Notifications are sent asynchronously and don't block processing flows.

### State management flows

State management flows are triggered by events, not called as subflows. This decoupled approach ensures state management doesn't block processing flows:

* **Batch state flow**
  * Scans flag files to count batch states
  * Updates Prefect Artifacts with progress
  * Checks if all batches are processed
  * Emits mosaic completion event when all batches complete
* **Slice state flow**
  * Checks state of both mosaics in slice
  * Updates Prefect Artifacts with slice progress
  * Checks if both mosaics are stitched
  * Emits slice ready event when both mosaics complete

