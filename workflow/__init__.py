# """
# OCT Pipeline Workflow - Refactored Prefect-based workflow orchestration system.
#
# This package provides Prefect flows and tasks for processing tiled OCT scan data
# through multiple stages: conversion to complex data, generation of 3D volumes
# and enface images, tile stitching, registration of dual-illumination scans,
# and final stacking across slices.
#
# The workflow is organized by scope:
# - Tile: Process a single tile
# - Mosaic: Process all tiles in a mosaic and stitch them
# - Slice: Process two mosaics (normal and tilted) and register them
# - Experiment: Process all slices and stack them
# - Upload: Handle uploads to cloud storage
# """
#
# # Flows
# from .flows import (
#     process_experiment_flow,
#     process_slice_flow,
#     process_mosaic_flow,
#     process_tile_flow,
#     determine_mosaic_coordinates_flow,
#     stitch_mosaic_flow,
#     register_slice_flow,
#     stack_all_slices_flow,
#     upload_files_flow,
# )
#
# # Tasks (exported for convenience, but typically accessed through flows)
# from .tasks import (
#     # Tile processing
#     load_spectral_raw_task,
#     spectral_to_complex_task,
#     complex_to_volumes_task,
#     find_surface_task,
#     volumes_to_enface_task,
#     save_volumes_task,
#     save_enface_task,
#     # Mosaic processing
#     collect_tile_aip_images_task,
#     determine_tile_coordinates_task,
#     save_coordinates_task,
#     load_coordinates_task,
#     create_mask_from_aip_task,
#     stitch_enface_images_task,
#     stitch_3d_volumes_task,
#     apply_mask_task,
#     save_stitched_enface_task,
#     save_stitched_volumes_task,
#     # Slice processing
#     load_normal_mosaic_task,
#     load_tilted_mosaic_task,
#     register_orientations_task,
#     compute_3d_orientation_task,
#     save_registered_data_task,
#     # Stacking
#     collect_slice_data_task,
#     stack_2d_images_task,
#     stack_3d_volumes_task,
#     save_stacked_data_task,
#     # Upload
#     compress_spectral_task,
#     queue_upload_spectral_task,
#     queue_upload_stitched_volumes_task,
#     # Notifications
#     notify_slack_task,
#     notify_tile_complete_task,
#     notify_stitched_complete_task,
#     monitor_tile_progress_task,
#     # Utils
#     extract_path_from_asset,
#     extract_paths_from_assets,
#     discover_slices_task,
# )
#
# __all__ = [
#     # Flows
#     "process_experiment_flow",
#     "process_slice_flow",
#     "process_mosaic_flow",
#     "process_tile_flow",
#     "determine_mosaic_coordinates_flow",
#     "stitch_mosaic_flow",
#     "register_slice_flow",
#     "stack_all_slices_flow",
#     "upload_files_flow",
#     # Tasks - Tile processing
#     "load_spectral_raw_task",
#     "spectral_to_complex_task",
#     "complex_to_volumes_task",
#     "find_surface_task",
#     "volumes_to_enface_task",
#     "save_volumes_task",
#     "save_enface_task",
#     # Tasks - Mosaic processing
#     "collect_tile_aip_images_task",
#     "determine_tile_coordinates_task",
#     "save_coordinates_task",
#     "load_coordinates_task",
#     "create_mask_from_aip_task",
#     "stitch_enface_images_task",
#     "stitch_3d_volumes_task",
#     "apply_mask_task",
#     "save_stitched_enface_task",
#     "save_stitched_volumes_task",
#     # Tasks - Slice processing
#     "load_normal_mosaic_task",
#     "load_tilted_mosaic_task",
#     "register_orientations_task",
#     "compute_3d_orientation_task",
#     "save_registered_data_task",
#     # Tasks - Stacking
#     "collect_slice_data_task",
#     "stack_2d_images_task",
#     "stack_3d_volumes_task",
#     "save_stacked_data_task",
#     # Tasks - Upload
#     "compress_spectral_task",
#     "queue_upload_spectral_task",
#     "queue_upload_stitched_volumes_task",
#     # Tasks - Notifications
#     "notify_slack_task",
#     "notify_tile_complete_task",
#     "notify_stitched_complete_task",
#     "monitor_tile_progress_task",
#     # Tasks - Utils
#     "extract_path_from_asset",
#     "extract_paths_from_assets",
#     "discover_slices_task",
# ]
#
