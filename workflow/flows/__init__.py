"""
Flows module for OCT pipeline workflow.

This module contains all flow definitions organized by scope:
- tile_flow: Process a single tile
- tile_batch_flow: Process a batch of tiles
- mosaic_processing_flow: Event-driven mosaic processing (stitching)
- slice_flow: Process a slice (two mosaics + registration)
- experiment_flow: Main experiment flow and stacking
- upload_flow: Handle uploads to cloud storage
"""

# Tile flows
from .tile_flow import (
    process_tile_flow,
)

from .tile_batch_flow import (
    process_tile_batch_flow,
    complex_to_processed_batch_event_flow,
    upload_to_linc_batch_event_flow,
)

# Mosaic processing flow
from .mosaic_processing_flow import (
    process_mosaic_event_flow,
)

__all__ = [
    # Tile flows
    "process_tile_flow",
    "process_tile_batch_flow",
    "complex_to_processed_batch_event_flow",
    "upload_to_linc_batch_event_flow",
    # Mosaic processing
    "process_mosaic_event_flow",
]

