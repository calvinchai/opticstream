"""
OCT Pipeline Workflow - Prefect-based workflow orchestration system.

This package provides Prefect flows and tasks for processing tiled OCT scan data
through multiple stages: conversion to complex data, generation of 3D volumes
and enface images, tile stitching, registration of dual-illumination scans,
and final stacking across slices.
"""

from .flows import (
    process_experiment_flow,
    process_slice_flow,
    process_mosaic_flow,
    process_tile_flow,
    determine_mosaic_coordinates_flow,
    stitch_mosaic_flow,
    register_slice_flow,
    stack_all_slices_flow,
)

from .config import (
    WorkflowConfig,
    ProcessingConfig,
    PathsConfig,
    ResourcesConfig,
    CloudConfig,
    SlackConfig,
    load_config,
    get_slack_config_dict,
)

from .upload_queue import (
    UploadQueueManager,
    get_upload_queue_manager,
)

__all__ = [
    # Flows
    "process_experiment_flow",
    "process_slice_flow",
    "process_mosaic_flow",
    "process_tile_flow",
    "determine_mosaic_coordinates_flow",
    "stitch_mosaic_flow",
    "register_slice_flow",
    "stack_all_slices_flow",
    # Config
    "WorkflowConfig",
    "ProcessingConfig",
    "PathsConfig",
    "ResourcesConfig",
    "CloudConfig",
    "SlackConfig",
    "load_config",
    "get_slack_config_dict",
    # Upload Queue
    "UploadQueueManager",
    "get_upload_queue_manager",
]

