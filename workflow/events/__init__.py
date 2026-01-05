"""
Event system for OCT pipeline workflow.

This module provides event constants and utilities following the design document
naming convention: linc.oct.{hierarchy}.{state}
"""

from workflow.events.constants import (
    BATCH_ARCHIVED,
    BATCH_PROCESSED,
    BATCH_READY,
    BATCH_UPLOADED,
    MOSAIC_READY,
    MOSAIC_STITCHED,
    SLICE_READY,
    SLICE_REGISTERED,
)
from workflow.events.utils import get_event_trigger

__all__ = [
    # Event constants
    "BATCH_READY",
    "BATCH_PROCESSED",
    "BATCH_ARCHIVED",
    "BATCH_UPLOADED",
    "MOSAIC_READY",
    "MOSAIC_STITCHED",
    "MOSAIC_VOLUME_STITCHED",
    "MOSAIC_VOLUME_UPLOADED",
    "SLICE_READY",
    "SLICE_REGISTERED",
    # Utilities
    "get_event_trigger",
]

