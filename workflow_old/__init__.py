"""
OCT Pipeline Workflow - Prefect-based workflow orchestration system.

This package provides Prefect flows and tasks for processing tiled OCT scan data
through multiple stages: conversion to complex data, generation of 3D volumes
and enface images, tile stitching, registration of dual-illumination scans,
and final stacking across slices.
"""

from .config import (CloudConfig, PathsConfig, ProcessingConfig, ResourcesConfig,
                     SlackConfig, WorkflowConfig, get_slack_config_dict, load_config)
from .flows import (determine_mosaic_coordinates_flow, process_experiment_flow,
                    process_mosaic_flow, process_slice_flow, process_tile_flow,
                    register_slice_flow, stack_all_slices_flow, stitch_mosaic_flow)
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
