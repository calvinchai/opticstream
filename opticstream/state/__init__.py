"""
State management classes for tracking processing state.

This module contains classes for managing state at different levels:
- BatchState: Individual batch state
- MosaicState: Mosaic-level state (contains multiple batches)
- SliceState: Slice-level state (contains two mosaics)
- ProjectState: Project-level state (contains all slices)
"""

__all__ = ["BatchState", "MosaicState", "SliceState", "ProjectState"]
