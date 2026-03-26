---
title: OCT data flow and folder layout
---

# OCT data flow and folder layout

## High-level architecture

### Core components

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

## Data processing flow

Processing is organized hierarchically: **Tile → Mosaic → Slice → All-Slices**.

### Tile-level processing

Each tile represents the smallest independently processed unit in the data hierarchy. Tiles are the atomic data unit - the fundamental building blocks that compose mosaics.

#### Inputs

* Spectral raw data **or** complex data
* File naming convention encodes acquisition metadata (parsed at ingest)

#### Tile processing steps

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
   * See the MATLAB batch processing design doc for details on batch processing optimization
4. **Complex → 3D Volumes Conversion**
   * Convert complex tiles to 3D volumes (dBI, O3D, R3D modalities)
   * Performed in MATLAB
   * See the MATLAB batch processing design doc for details on batch processing optimization
5. **Surface Finding**
   * Automatic surface detection from intensity data
   * Surface finding method can be configured (e.g., \"find\" for automatic detection)
6. **Enface Image Generation**
   * Generate 2D enface images from 3D volumes using surface information
   * Multiple enface modalities: AIP (Average Intensity Projection), MIP (Maximum Intensity Projection), orientation, retardance, birefringence
   * Surface maps generated for visualization
7. **Archival Upload**
   * Compressed raw data uploaded to DANDI/LINC
   * Uploads are handled by a dedicated, event-triggered flow (see the upload strategy design doc for details)

#### MATLAB integration

* MATLAB is invoked via command-line interface from Python
* MATLAB functions handle spectral-to-complex and complex-to-processed conversions
* Data flow: Python → MATLAB → Python (processed tiles)
* Currently executed in MATLAB (future Python migration planned)
* Future migration to Python-native implementations will eliminate the need for batch processing optimization

#### Tile-level QC

* Validate surface finding overlap with intensity images
* Verify processing quality at tile level
* QC images emitted as Prefect artifacts and Slack notifications

### Mosaic-level processing

Triggered once **all tiles in a mosaic** complete tile-level processing. A mosaic contains all tiles for a given slice and illumination type (normal or tilted).

#### First slice processing

For each illumination type of the first slice (mosaic_001 for normal, mosaic_002 for tilted, maybe more types of illumination), additional processing steps are required that are not needed for subsequent slices:

1. **Tile Coordinate Determination**
   * Determine tile positions and alignment for stitching
   * Generates coordinate template that is reusable for all mosaics of the same illumination type
   * For subsequent slices of the same illumination type, the template from the first slice is reused
   * See the stitching and coordinate determination design doc for detailed algorithms and methods
2. **Focus Finding**
   * Determine optimal focus plane for 3D volume stitching
   * Focus finding requires accurate surface information, so it uses an unfiltered version of the surface data
   * QC validation: verify focus finding overlap with intensity images
   * See the focus finding algorithms design doc for details

#### 2D enface mosaic stitching

1. **Template Application**
   * Apply coordinate template to current mosaic tiles
   * For first slice: use newly generated template
   * For subsequent slices: reuse template from first slice of same illumination type
   * Generate tile information files for each enface modality
2. **Stitch enface modalities**
   * Stitch all 2D enface modalities: AIP, MIP, orientation, retardance, birefringence, surface
   * Each modality is stitched independently using the same coordinate template
   * Generate overlap QC images to verify stitching quality
3. **Mask generation and application**
   * Generate mask from stitched AIP using threshold-based approach
   * Apply mask to all stitched enface outputs
   * Mask removes background/noise regions
4. **Output generation**
   * Save stitched enface images in multiple formats (NIfTI, JPEG)
   * Upload stitched 2D mosaics to cloud storage

#### 3D volume mosaic stitching

1. **Focus plane application**
   * For first slice: use focus plane determined during first slice processing
   * For subsequent slices: reuse focus plane from first slice of same illumination type
   * Apply focus plane for optimal 3D volume alignment
2. **Stitch 3D volumes**
   * Stitch 3D volume modalities: dBI, O3D, R3D
   * Use coordinate template from 2D stitching
   * Apply mask to stitched volumes
3. **Upload stitched volumes**
   * Upload stitched 3D volumes to cloud storage
   * Volumes stored in appropriate format for downstream analysis

### Slice-level processing

Triggered once **all mosaics in a slice** are complete. Each slice contains two (or more in future) mosaics: one with normal illumination and one with tilted illumination.

#### Registration process

1. **Thruplane registration**
   * Register normal and tilted illumination mosaics to combine orientations
   * Uses MATLAB-based registration algorithm
   * Accounts for tilt angle (gamma parameter) between illuminations
   * Compute-heavy but low I/O, suitable for offloading to auxiliary hosts
2. **3D orientation computation**
   * Combines information from both illumination angles
   * Generates 3D axis representations (normalization needed)
   * Generates RGB visualization of 3D axis orientation
3. **RGB 3D axis visualization**
   * Provides visual representation of fiber orientation in 3D space
   * Useful for quality control and visualization

#### Slice-level outputs

* Registered slice-level 2D/3D data
* Thru-plane and in-plane data in `.mat`
* 3D axis data in NIfTI
* Orientation images in JPEG (thru-plane, in-plane, 3D axis)

### All-slices processing

Currently manual:

* Stack 2D mosaics across slices
* Stack 3D volumes across slices

## Folder structure

### Project directory structure

```text
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
│  └─ tilemap-normal.j2       # tile coordinate map
│  └─ tilemap-tilted.j2       # tile coordinate map
```

### DANDI/LINC storage structure

```text
DANDISET/
├─ rawdata/...         # raw compressed tiles
│  └─ sub-{subject_id}/
│     └─ sample-slice-{slice_id:03d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii.gz
└─ derivative/
   └─ sub-{subject_id}/
      └─ mosaic_{mosaic_id:03d}_{modality}.nii
      └─ .../*.ome.zarr (for large volumes)
```

### Symlink strategy

Symlinks are used extensively to balance I/O performance and long-term storage:

* **Performance**: Processing occurs on high-speed SSDs (local paths)
* **Archival**: Final outputs symlinked to DANDI/LINC storage for long-term preservation
* **Efficiency**: Avoids data duplication while maintaining fast access during processing
* **Transparency**: Symlinks make data appear in both locations without copying

Symlinks are created:

* From processing directories to DANDI/LINC storage for final outputs
* From DANDI/LINC storage to processing directories for inputs (when needed)

