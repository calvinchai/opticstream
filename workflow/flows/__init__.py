"""
Flows module for OCT pipeline workflow.

This module contains all flow definitions organized by scope:
- tile_flow: Process a single tile
- mosaic_flow: Process a mosaic (all tiles + stitching)
- slice_flow: Process a slice (two mosaics + registration)
- experiment_flow: Main experiment flow and stacking
- upload_flow: Handle uploads to cloud storage
"""

# Tile flow
from .tile_flow import (
    process_tile_flow,
)

__all__ = [
    # Tile flow
    "process_tile_flow",
]

