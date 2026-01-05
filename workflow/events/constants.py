"""
Event name constants following the design document naming convention.

All events follow the pattern: linc.oct.{hierarchy}.{state}
"""

# Batch-Level Events
BATCH_READY = "linc.oct.batch.ready"  # Batch of tiles detected (to start converting to complex data)
BATCH_COMPLEXED = "linc.oct.batch.complexed"  # Batch complexed (triggers complex to processed flow)
BATCH_PROCESSED = "linc.oct.batch.processed"  # Batch processed (triggers state management, emits mosaic.ready when all batches done)
BATCH_ARCHIVED = "linc.oct.batch.archived"  # Batch archived and compressed (triggers upload flow, state management)
BATCH_UPLOADED = "linc.oct.batch.uploaded"  # Batch uploaded to LINC storage (triggers upload completion handlers)

# Mosaic-Level Events
MOSAIC_READY = "linc.oct.mosaic.ready"  # All tiles in mosaic processed (all batches complete), triggers mosaic stitching flow # Mosaic started (triggers state management, emits mosaic.ready when all mosaics done)
MOSAIC_STITCHED = "linc.oct.mosaic.stitched"  # All modalities stitched (triggers mosaic state management, upload flow)
MOSAIC_VOLUME_STITCHED = "linc.oct.mosaic.volume_stitched"  # All volume modalities stitched (triggers mosaic state management, upload flow)
MOSAIC_VOLUME_UPLOADED = "linc.oct.mosaic.volume_uploaded"  # All volume modalities uploaded to LINC storage (triggers upload completion handlers)

# Slice-Level Events
SLICE_READY = "linc.oct.slice.ready"  # Both mosaics in slice are stitched, ready for registration (triggers slice registration flow)
SLICE_REGISTERED = "linc.oct.slice.registered"  # Slice registration complete (triggers slice state management, upload flow)
