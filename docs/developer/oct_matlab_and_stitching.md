---
title: OCT MATLAB batch processing, upload, and stitching
---

# OCT MATLAB batch processing, upload, and stitching

## MATLAB batch processing (implementation detail)

> See also the OCT data flow doc for a high-level overview of batch-level processing. This section provides additional implementation details.

### Data hierarchy vs batch processing

Batches are a temporary grouping used only during MATLAB processing for efficiency.

### Grid configuration

The grid configuration defines how tiles are organized for MATLAB batch processing:

* **`grid_size_x`**: Number of batches (columns) per mosaic
  * Determines how many batches are needed to process all tiles in a mosaic
  * Each batch contains `grid_size_y` tiles
* **`grid_size_y`**: Number of tiles per batch (rows)
  * Determines batch size for MATLAB processing
  * Normal and tilted illuminations share the same `grid_size_y`
* **Total tiles per mosaic**: `grid_size_x × grid_size_y`

### Why batches are used

Batch processing is used specifically because MATLAB processing is more efficient when processing multiple tiles together:

* Reduces MATLAB startup overhead by processing many tiles per session
* Enables better use of MATLAB's parallel operations
* Reduces the number of MATLAB processes and improves resource utilization

### Batch organization and state tracking

Tiles are organized into batches based on their position in the acquisition grid:

* Each column (`grid_size_x`) becomes a batch
* Each batch contains `grid_size_y` tiles (rows)
* Batches are processed in parallel for efficiency

Batch completion is tracked using flag files stored in the `state/` directory. See the events and state management doc for comprehensive details on flag file structure, lifecycle, and benefits. The batch-level flag files (`batch-{batch_id}.started`, `.archived`, `.processed`, `.uploaded`) enable progress tracking, idempotency, and crash recovery for MATLAB batch processing.

### Post-MATLAB processing

Once MATLAB batch processing completes, the system operates on individual tiles again for downstream processing (stitching, QC, coordinate determination). The batch grouping is only relevant during MATLAB processing and exists purely as an implementation optimization; tiles remain the atomic data unit in the hierarchy.

## Upload strategy

### Separate upload flows (event-driven, non-blocking)

Uploads are handled by dedicated, event-triggered flows that run independently of compute pipelines:

* **Event-triggered**: Upload flows are triggered by events (e.g., `linc.oct.batch.uploaded`)
* **Non-blocking**: Upload flows don't block compute-intensive processing
* **Isolated**: Upload failures don't affect compute pipeline
* **Independent retry**: Upload flows can retry independently without blocking upstream processing

### Upload queue management

Upload queue management enables controlled, concurrent uploads:

* **Concurrency control**: Maximum number of concurrent uploads (e.g., 5)
* **Queue-based**: Files are queued for upload, processed by background workers
* **Non-blocking enqueue**: Adding files to upload queue returns immediately
* **Background processing**: Actual uploads happen in background threads/processes

This prevents uploads from overwhelming network bandwidth or cloud storage APIs.

### Cloud storage integration (DANDI/LINC)

* **DANDI archive**: Processed data uploaded to DANDI archive for long-term preservation
* **LINC storage**: Raw compressed tiles uploaded to LINC storage
* **Symlinks**: Final outputs symlinked to DANDI/LINC storage locations
* **Metadata**: Upload events include metadata for tracking and verification

### Upload retry and error handling

* **Retry logic**: Upload failures are retried automatically with exponential backoff
* **Error logging**: Upload errors are logged for debugging and monitoring
* **Failure notification**: Critical upload failures trigger notifications
* **Resumability**: Partial uploads can be resumed using flag files

## MATLAB integration

### MATLAB invocation

MATLAB is invoked from Python via command-line interface:

* **Batch mode**: MATLAB runs in batch mode (non-interactive) for automation
* **Function calls**: Python constructs MATLAB function calls with batch of tiles
* **Path management**: MATLAB paths are configured to include required toolboxes and functions
* **Output capture**: MATLAB output is captured and logged for debugging

### Batch processing requirement

MATLAB processes tiles in batches (not individually) for efficiency:

* **Spectral-to-complex**: Multiple tiles passed to MATLAB function for batch conversion
* **Complex-to-processed**: Multiple tiles passed to MATLAB function for batch processing
* **Reduced overhead**: Batching reduces MATLAB startup overhead per tile

This is why batch-level processing exists - it's an optimization for MATLAB efficiency, not a data hierarchy level.

### MATLAB functions used

High-level MATLAB functions (not specific implementation details):

* **Spectral-to-complex**: Converts spectral raw data to complex format
* **Complex-to-processed**: Converts complex data to 3D volumes and enface images
* **Surface finding**: Automatic surface detection from intensity data
* **Registration**: Thruplane registration for combining normal and tilted illuminations

### Data flow between Python and MATLAB

* **Python → MATLAB**: Batches of tile file paths passed to MATLAB functions
* **MATLAB processing**: MATLAB reads files, processes tiles, writes outputs
* **MATLAB → Python**: Processed tile files written to filesystem, Python reads results
* **File-based interface**: Communication via filesystem (no in-memory data transfer)

### Future migration strategy

When MATLAB steps are migrated to Python-native implementations:

* Batch processing may no longer be necessary (Python can process tiles individually more efficiently)
* Data hierarchy remains Tile → Mosaic (no change to fundamental structure)
* Processing efficiency may improve (no MATLAB startup overhead)
* System becomes more maintainable (single language codebase)

## Coordinate determination and stitching

### Fiji-based coordinate determination

Fiji (ImageJ) is used for initial tile alignment and coordinate determination:

* **Tile configuration**: Fiji generates `TileConfiguration.txt` with initial tile positions
* **Overlap-based**: Uses tile overlap information to align tiles
* **First slice only**: Coordinate determination runs only for first slice of each illumination type
* **Template generation**: Coordinates are processed and converted to reusable templates

### Template generation and reuse strategy

Templates are generated once per illumination type and reused for all slices:

* **Template generation**: Jinja2 templates generated from first slice coordinates
* **Template reuse**: Subsequent slices of same illumination type reuse template
* **Efficiency**: Avoids redundant coordinate determination for each slice
* **Consistency**: Ensures consistent tile positioning across slices

Templates contain:

* Tile positioning information
* Scan resolution parameters
* Base directory paths (parameterized for reuse)

### Stitching process

#### 2D enface stitching

* **Template application**: Apply coordinate template to current mosaic tiles
* **Modality stitching**: Stitch each enface modality independently (AIP, MIP, orientation, retardance, birefringence, surface)
* **Mask generation**: Generate mask from stitched AIP using threshold
* **Mask application**: Apply mask to all stitched enface outputs
* **Output formats**: Save in multiple formats (NIfTI, JPEG, TIFF)

#### 3D volume stitching

* **Focus finding**: Determine optimal focus plane (first slice only)
* **Volume stitching**: Stitch 3D volume modalities (dBI, O3D, R3D)
* **Template reuse**: Use same coordinate template as 2D stitching
* **Mask application**: Apply mask to stitched volumes

### Mask generation and application

* **Threshold-based**: Mask generated from stitched AIP using intensity threshold
* **Background removal**: Mask removes background/noise regions
* **Consistent application**: Same mask applied to all stitched modalities
* **Quality control**: Mask quality validated as part of QC process

