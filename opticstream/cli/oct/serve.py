from typing import List, Optional

from cyclopts import App
from prefect import serve

from opticstream.cli.oct import oct_cli
from opticstream.events import (
    BATCH_ARCHIVED,
    BATCH_COMPLEXED,
    BATCH_READY,
    MOSAIC_READY,
    MOSAIC_STITCHED,
    MOSAIC_VOLUME_STITCHED,
    SLICE_READY,
    get_event_trigger,
)
from opticstream.flows.mosaic_processing_flow import process_mosaic_event_flow
from opticstream.flows.process_tile_batch_complex2processed_flow import (
    complex_to_processed_batch_event_flow,
    complex_to_processed_batch_flow,
)
from opticstream.flows.process_tile_batch_flow import (
    process_tile_batch_event_flow,
    process_tile_batch_flow,
)
from opticstream.flows.slice_registration_flow import register_slice_event_flow
from opticstream.flows.slack_notification_flow import slack_enface_notification_flow
from opticstream.flows.state_management_flow import unified_state_management_event_flow
from opticstream.flows.upload_flow import (
    upload_mosaic_enface_to_dandi_event_flow,
    upload_mosaic_volume_to_dandi_event_flow,
    upload_to_linc_batch_event_flow,
)
from opticstream.flows.volume_stitching_flow import stitch_volume_event_flow

serve_cli = oct_cli.command(App(name="serve"))


def build_deployments(project_name: Optional[str] = None) -> List:
    """Build all flow deployments. When project_name is set, event triggers only fire for that project."""
    # ============================================================================
    # Tile Batch Flow Deployments
    # ============================================================================

    process_tile_batch_deployment = process_tile_batch_flow.to_deployment(
        name="process_tile_batch_flow",
        tags=["tile-batch", "process-tile-batch"],
        concurrency_limit=1,
    )

    process_tile_batch_event_deployment = process_tile_batch_event_flow.to_deployment(
        name="process_tile_batch_event_flow",
        tags=["event-driven", "tile-batch", "process-tile-batch"],
        triggers=[
            get_event_trigger(BATCH_READY, project_name=project_name),
        ],
        concurrency_limit=1,
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
                get_event_trigger(BATCH_COMPLEXED, project_name=project_name),
            ],
            concurrency_limit=1,
        )
    )

    # ============================================================================
    # Upload Flow Deployments
    # ============================================================================

    upload_to_linc_batch_event_deployment = upload_to_linc_batch_event_flow.to_deployment(
        name="upload_to_linc_batch_event_flow",
        tags=["event-driven", "tile-batch", "upload-to-linc"],
        triggers=[
            get_event_trigger(BATCH_ARCHIVED, project_name=project_name),
        ],
    )

    upload_mosaic_enface_to_dandi_event_deployment = (
        upload_mosaic_enface_to_dandi_event_flow.to_deployment(
            name="upload_mosaic_enface_to_dandi_event_flow",
            tags=["event-driven", "upload", "dandi", "enface"],
            triggers=[
                get_event_trigger(MOSAIC_STITCHED, project_name=project_name),
            ],
        )
    )

    upload_mosaic_volume_to_dandi_event_deployment = (
        upload_mosaic_volume_to_dandi_event_flow.to_deployment(
            name="upload_mosaic_volume_to_dandi_event_flow",
            tags=["event-driven", "upload", "dandi", "volume"],
            triggers=[
                get_event_trigger(MOSAIC_VOLUME_STITCHED, project_name=project_name),
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
            get_event_trigger(MOSAIC_READY, project_name=project_name),
        ],
        concurrency_limit=1,
    )

    stitch_volume_event_deployment = stitch_volume_event_flow.to_deployment(
        name="stitch_volume_event_flow",
        tags=["event-driven", "mosaic-processing", "volume-stitching"],
        triggers=[
            get_event_trigger(MOSAIC_STITCHED, project_name=project_name),
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
            get_event_trigger(SLICE_READY, project_name=project_name),
        ],
    )

    # ============================================================================
    # State Management Flow Deployments
    # ============================================================================

    unified_state_management_event_deployment = (
        unified_state_management_event_flow.to_deployment(
            name="unified_state_management_event_flow",
            tags=["event-driven", "state-management", "unified"],
            triggers=[
                get_event_trigger(
                    MOSAIC_STITCHED,
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                    },
                    project_name=project_name,
                ),
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
            get_event_trigger(MOSAIC_STITCHED, project_name=project_name),
        ],
    )

    return [
        process_tile_batch_deployment,
        process_tile_batch_event_deployment,
        complex_to_processed_batch_deployment,
        complex_to_processed_batch_event_deployment,
        upload_to_linc_batch_event_deployment,
        upload_mosaic_enface_to_dandi_event_deployment,
        upload_mosaic_volume_to_dandi_event_deployment,
        process_mosaic_event_deployment,
        stitch_volume_event_deployment,
        register_slice_event_deployment,
        unified_state_management_event_deployment,
        slack_enface_notification_deployment,
    ]

register_slice_event_deployment = register_slice_event_flow.to_deployment(
    name="register_slice_event_flow",
    tags=["event-driven", "slice-registration"],
    triggers=[
        get_event_trigger(SLICE_READY),
    ],
)
register_slice_deployment = register_slice_event_flow.to_deployment(
    name="register_slice_flow",
    tags=["slice-registration"],
)

@serve_cli.command
def register():
    serve(
        register_slice_event_deployment,
        register_slice_deployment,
    )


@serve_cli.command
def all(
    register: bool = False,
    project_name: str = None,
    exclude: list[str] = [],
):
    services = build_deployments(project_name=project_name)

    if register:
        services.append(register_slice_event_deployment)
        services.append(register_slice_deployment)

    if exclude:
        exclude_set = set(exclude)
        services = [
            deployment
            for deployment in services
            if getattr(deployment, "name", None) not in exclude_set
        ]

    if not services:
        raise ValueError("No deployments to serve (all services excluded).")

    serve(*services)