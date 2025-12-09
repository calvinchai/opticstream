"""
Tasks module for OCT pipeline workflow.

This module contains all task definitions organized by functionality.
"""

# Tile processing tasks
from .tile_processing import (
    spectral_to_complex_task,
    complex_to_volumes_task,
    find_surface_task,
    volumes_to_enface_task,
    # save_volumes_task,
    # save_enface_task,
)

# # Upload tasks
# from .upload import (
#     compress_spectral_task,
#     queue_upload_spectral_task,
#     queue_upload_stitched_volumes_task,
# )

#
# # Utility functions and tasks
# from .utils import (
#     extract_path_from_asset,
#     extract_paths_from_assets,
#     discover_slices_task,
# )

__all__ = [
    # Tile processing
    "spectral_to_complex_task",
    "complex_to_volumes_task",
    "find_surface_task",
    "volumes_to_enface_task",
    # "save_volumes_task",
    # "save_enface_task",
    # Utils
    # "extract_path_from_asset",
    # "extract_paths_from_assets",
    # "discover_slices_task",
]

