---
title: OCT algorithms, error handling, and QC
---

# OCT algorithms, error handling, and QC

## Tile coordinate determination algorithms

This section will discuss the detailed algorithms and methods used for tile coordinate determination. Content is to be added as algorithms are finalized and documented.

## Focus finding algorithms

This section will discuss the detailed focus finding algorithms used to determine optimal focus planes for 3D volume stitching. Content is to be added.

## Stitching tool and algorithm

This section will discuss the detailed stitching tool and algorithm used for both 2D enface and 3D volume stitching. Content is to be added.

## Error handling and recovery

### Retry strategies

The system uses retry strategies at different levels:

* **Task-level retries**: Individual tasks can retry on failure with exponential backoff
* **Flow-level retries**: Flows can be configured to retry on failure
* **Event-driven retries**: Failed event-driven flows can be retried by re-emitting events
* **Upload retries**: Upload operations retry with longer timeouts for network issues

### Flag file-based recovery

Flag files enable recovery after crashes or failures. See the events and state management doc for comprehensive details on flag file structure and lifecycle. Key recovery mechanisms:

* **State persistence**: Flag files persist across flow runs
* **State checking**: Flows check flag files to determine what work has been completed
* **Resume from last state**: System can resume from last completed state
* **Idempotent operations**: Flows can safely rerun - flag files prevent duplicate work

### Event idempotency

Events and event-driven flows are designed to be idempotent:

* **Duplicate events**: Multiple events with same payload are safe (idempotent)
* **State checking**: Event-driven flows check state before processing
* **Flag file protection**: Flag files prevent duplicate processing even if events are duplicated
* **Redundancy**: Multiple flows can emit same completion events for redundancy

### Failure handling at different levels

* **Tile-level failures**: Failed tiles don't block other tiles - processing continues
* **Batch-level failures**: Failed batches can be retried independently
* **Mosaic-level failures**: Failed mosaics trigger alerts but don't block slice processing
* **Slice-level failures**: Failed slices trigger alerts but don't block other slices
* **Upload failures**: Upload failures don't affect compute pipeline - can retry independently

## Quality control (QC)

### QC at tile level

Tile-level QC validates individual tile processing:

* **Surface finding validation**: Verify surface finding overlap with intensity images
* **Processing quality**: Validate that processing completed successfully
* **QC images**: Generate QC images for visual inspection
* **Artifact generation**: Emit QC images as Prefect artifacts

### QC at mosaic level

Mosaic-level QC validates stitching and alignment:

* **Stitching consistency**: Verify stitching consistency across modalities
* **Focus finding validation**: Validate focus finding overlap with intensity (first slice)
* **Overlap QC images**: Generate overlap QC images to verify tile alignment
* **Mask quality**: Validate mask quality and coverage

### QC artifacts and notifications

* **Prefect artifacts**: QC images stored as Prefect artifacts for easy access
* **Slack notifications**: QC images sent to Slack for real-time review
* **Progress tracking**: QC status tracked in progress artifacts
* **Links**: Links to QC images included in notifications and artifacts

### QC image generation

QC images are generated at key processing stages:

* **Tile processing**: Surface finding validation images
* **Mosaic stitching**: Overlap images, stitched mosaic previews
* **Registration**: Registration result visualizations, 3D axis images
* **Formats**: QC images in multiple formats (JPEG, PNG) for easy viewing

