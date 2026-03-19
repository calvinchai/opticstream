"""
Legacy import path for tile-batch processing.

Implementation lives in :mod:`opticstream.flows.psoct.tile_batch_process_flow`.
"""

from opticstream.flows.psoct.tile_batch_process_flow import (
    archive_tile_batch,
    link_complex_inputs_to_mosaic_complex_dir,
    process_spectral_tile_batch,
    process_tile_batch,
    process_tile_batch_event_flow,
    process_tile_batch_flow,
)

__all__ = [
    "archive_tile_batch",
    "link_complex_inputs_to_mosaic_complex_dir",
    "process_spectral_tile_batch",
    "process_tile_batch",
    "process_tile_batch_event_flow",
    "process_tile_batch_flow",
]
