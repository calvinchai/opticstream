"""
Unified deployment script for all Prefect flows in the OCT pipeline workflow.

This script consolidates all flow deployments from across the codebase into a single
entry point, making it easier to deploy and manage all flows together.

Usage:
    python deploy_all_flows.py
"""

from prefect import serve
from prefect.events import DeploymentEventTrigger

# Import flows
from workflow.events.constants import BATCH_COMPLEXED
from workflow.flows.mosaic_processing_flow import process_mosaic_event_flow
from workflow.flows.slice_registration_flow import register_slice_event_flow
from workflow.flows.slack_notification_flow import slack_enface_notification_flow
from workflow.flows.state_management_flow import unified_state_management_event_flow
from workflow.flows.tile_batch_flow import (
    complex_to_processed_batch_event_flow,
    complex_to_processed_batch_flow,
    process_tile_batch_event_flow,
    process_tile_batch_flow,
)
from workflow.flows.upload_flow import (
    upload_mosaic_enface_to_dandi_event_flow,
    upload_mosaic_volume_to_dandi_event_flow,
    upload_to_linc_batch_event_flow,
    upload_to_linc_batch_flow,
)

# Import event constants and utilities
from workflow.events import (
    BATCH_ARCHIVED,
    BATCH_PROCESSED,
    BATCH_READY,
    MOSAIC_READY,
    MOSAIC_STITCHED,
    MOSAIC_VOLUME_STITCHED,
    SLICE_READY,
    get_event_trigger,
)

# ============================================================================
# Tile Batch Flow Deployments
# ============================================================================

process_tile_batch_deployment = process_tile_batch_flow.to_deployment(
    name="process_tile_batch_flow",
    tags=["tile-batch", "process-tile-batch"],
    concurrency_limit=1,
)


process_tile_batch_event_deployment = (
    process_tile_batch_event_flow.to_deployment(
        name="process_tile_batch_event_flow",
        tags=["event-driven", "tile-batch", "process-tile-batch"],
        triggers=[
            get_event_trigger(BATCH_READY),
        ],
        concurrency_limit=1,
    )
)


complex_to_processed_batch_deployment = complex_to_processed_batch_flow.to_deployment(
    name="complex_to_processed_batch_flow",
    tags=["tile-batch", "complex-to-processed"],
    concurrency_limit=1,
)

complex_to_processed_batch_event_deployment = (
    complex_to_processed_batch_event_flow.to_deployment(
        name="complex_to_processed_batch_event_flow",
        tags=["event-driven", "tile-batch", "complex-to-processed"],
        triggers=[
            get_event_trigger(BATCH_COMPLEXED),
        ],
        concurrency_limit=1,
    )
)       
# ============================================================================
# Upload Flow Deployments
# ============================================================================

upload_to_linc_batch_deployment = upload_to_linc_batch_flow.to_deployment(
    name="upload_to_linc_batch_flow",
    tags=["tile-batch", "upload-to-linc"],
)

upload_to_linc_batch_event_deployment = upload_to_linc_batch_event_flow.to_deployment(
    name="upload_to_linc_batch_event_flow",
    tags=["event-driven", "tile-batch", "upload-to-linc"],
    triggers=[
        get_event_trigger(BATCH_ARCHIVED),
    ],
)

upload_mosaic_enface_to_dandi_event_deployment = (
    upload_mosaic_enface_to_dandi_event_flow.to_deployment(
        name="upload_mosaic_enface_to_dandi_event_flow",
        tags=["event-driven", "upload", "dandi", "enface"],
        triggers=[
            get_event_trigger(MOSAIC_STITCHED),
        ],
    )
)

upload_mosaic_volume_to_dandi_event_deployment = (
    upload_mosaic_volume_to_dandi_event_flow.to_deployment(
        name="upload_mosaic_volume_to_dandi_event_flow",
        tags=["event-driven", "upload", "dandi", "volume"],
        triggers=[
            get_event_trigger(MOSAIC_VOLUME_STITCHED),
        ],
    )
)

# ============================================================================
# Mosaic Processing Flow Deployments
# ============================================================================

process_mosaic_event_deployment = process_mosaic_event_flow.to_deployment(
    name="process_mosaic_event_flow",
    tags=["event-driven", "mosaic-processing", "stitching"],
    triggers=[
        get_event_trigger(MOSAIC_READY),
    ],
    concurrency_limit=1,
)

# ============================================================================
# Slice Registration Flow Deployments
# ============================================================================

register_slice_event_deployment = register_slice_event_flow.to_deployment(
    name="register_slice_event_flow",
    tags=["event-driven", "slice-registration"],
    triggers=[
        get_event_trigger(SLICE_READY),
    ],
)

# ============================================================================
# State Management Flow Deployments
# ============================================================================

# Unified deployment for all state management events with concurrency_limit=1
# This ensures all state management operations are serialized to avoid race conditions
unified_state_management_event_deployment = (
    unified_state_management_event_flow.to_deployment(
        name="unified_state_management_event_flow",
        tags=["event-driven", "state-management", "unified"],
        triggers=[
            get_event_trigger(BATCH_PROCESSED, {"event": BATCH_PROCESSED}),
            # Trigger when a batch is archived (for early progress tracking)
            get_event_trigger(BATCH_ARCHIVED, {"event": BATCH_ARCHIVED}),
            # Trigger when a mosaic is stitched
            get_event_trigger(MOSAIC_STITCHED, {"event": MOSAIC_STITCHED}),
        ],
        concurrency_limit=1,
    )
)

# ============================================================================
# Slack Notification Flow Deployments
# ============================================================================

slack_enface_notification_deployment = slack_enface_notification_flow.to_deployment(
    name="slack_enface_notification_flow",
    tags=["event-driven", "slack-notifications", "enface-stitched"],
    triggers=[
        get_event_trigger(MOSAIC_STITCHED),
    ],

)

# ============================================================================
# Serve All Deployments
# ============================================================================

if __name__ == "__main__":
    serve(
        # Tile batch flows
        process_tile_batch_deployment,
        process_tile_batch_event_deployment,
        complex_to_processed_batch_deployment,
        complex_to_processed_batch_event_deployment,
        # Upload flows
        upload_to_linc_batch_deployment,
        # upload_to_linc_batch_event_deployment,
        upload_mosaic_enface_to_dandi_event_deployment,
        upload_mosaic_volume_to_dandi_event_deployment,
        # Mosaic processing
        process_mosaic_event_deployment,
        # Slice registration
        register_slice_event_deployment,
        # State management
        unified_state_management_event_deployment,
        # Slack notifications
        # slack_enface_notification_deployment,
    )

