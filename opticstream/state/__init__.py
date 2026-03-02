"""
State management classes for tracking processing state.

This module contains classes for managing state at different levels:
- BatchState: Individual batch state
- MosaicState: Mosaic-level state (contains multiple batches)
- SliceState: Slice-level state (contains two mosaics)
- ProjectState: Project-level state (contains all slices)
"""
from .state_classes import BatchState, MosaicState, SliceState, ProjectState
from .batch_state_utils import (
    is_batch_archived,
    is_batch_processed,
    is_batch_started,
    mark_batch_archived,
    mark_batch_processed,
    mark_batch_started,
)

from .flags import (
    ARCHIVED,
    PROCESSED,
    STARTED,
    STITCHED,
    UPLOADED,
    REGISTERED,
    VOLUME_STITCHED,
    VOLUME_UPLOADED,
    get_batch_flag_path,
    get_mosaic_flag_path,
    get_slice_flag_path,
)

