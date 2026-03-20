from typing import List, Optional

from cyclopts import App
from prefect import serve

from opticstream.cli.oct import oct_cli
from opticstream.events import (
    BATCH_ARCHIVED,
    BATCH_COMPLEXED,
    BATCH_READY,
    MOSAIC_READY,
    MOSAIC_ENFACE_STITCHED,
    MOSAIC_VOLUME_STITCHED,
    SLICE_READY,
    get_event_trigger,
)
# from opticstream.flows.psoct.mosaic_process_flow import
from opticstream.flows.psoct.tile_batch_complex_flow import (
    process_complex_tile_batch_event as complex_to_processed_batch_event_flow,
    process_complex_tile_batch as complex_to_processed_batch_flow,
)
from opticstream.flows.psoct.tile_batch_process_flow import (
    process_tile_batch_event_flow,
    process_tile_batch_flow,
)
from opticstream.flows.psoct.slice_process_flow import register_slice_event_flow
from opticstream.flows.state_management_flow import unified_state_management_event_flow
from opticstream.flows.upload_flow import (
    upload_mosaic_enface_to_dandi_event_flow,
    upload_mosaic_volume_to_dandi_event_flow,
    upload_to_linc_batch_event_flow,
)
from opticstream.flows.psoct.mosaic_volume_stitch_flow import (
    stitch_volume_flow as stitch_volume_event_flow,
)

serve_cli = oct_cli.command(App(name="serve"))

COMMON_TAGS = ["linc", "psoct"]


def _normalize_project_name(project_name: str) -> Optional[str]:
    """
    Force project_name to be provided.
    If project_name == "all", return None so triggers apply to all projects.
    """
    if not project_name:
        raise ValueError("project_name is required. Use 'all' to target all projects.")
    return None if project_name == "all" else project_name


def build_deployments(
    *,
    project_name: str,
    deployment_name: str = "local",
) -> List:
    """
    Build all standard deployments.

    project_name is required.
    Use project_name='all' to make it None internally.
    """
    normalized_project_name = _normalize_project_name(project_name)

    return [
        # ============================================================================
        # Tile Batch Flow Deployments
        # ============================================================================
        process_tile_batch_flow.to_deployment(
            name=deployment_name,
            tags=["tile-batch", "process-tile-batch", *COMMON_TAGS],
            concurrency_limit=1,
        ),
        process_tile_batch_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "tile-batch", "process-tile-batch", *COMMON_TAGS],
            triggers=[
                get_event_trigger(BATCH_READY, project_name=normalized_project_name),
            ],
            concurrency_limit=1,
        ),
        complex_to_processed_batch_flow.to_deployment(
            name=deployment_name,
            tags=["tile-batch", "complex-to-processed", *COMMON_TAGS],
            concurrency_limit=1,
        ),
        complex_to_processed_batch_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "tile-batch", "complex-to-processed", *COMMON_TAGS],
            triggers=[
                get_event_trigger(BATCH_COMPLEXED, project_name=normalized_project_name),
            ],
            concurrency_limit=1,
        ),
        # ============================================================================
        # Upload Flow Deployments
        # ============================================================================
        upload_to_linc_batch_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "tile-batch", "upload-to-linc", *COMMON_TAGS],
            triggers=[
                get_event_trigger(BATCH_ARCHIVED, project_name=normalized_project_name),
            ],
        ),
        upload_mosaic_enface_to_dandi_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "upload", "dandi", "enface", *COMMON_TAGS],
            triggers=[
                get_event_trigger(MOSAIC_ENFACE_STITCHED, project_name=normalized_project_name),
            ],
        ),
        upload_mosaic_volume_to_dandi_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "upload", "dandi", "volume", *COMMON_TAGS],
            triggers=[
                get_event_trigger(
                    MOSAIC_VOLUME_STITCHED,
                    project_name=normalized_project_name,
                ),
            ],
        ),
        # ============================================================================
        # Mosaic Processing Flow Deployments
        # ============================================================================
        process_mosaic_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "mosaic-processing", "stitching", *COMMON_TAGS],
            triggers=[
                get_event_trigger(MOSAIC_READY, project_name=normalized_project_name),
            ],
            concurrency_limit=1,
        ),
        stitch_volume_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "mosaic-processing", "volume-stitching", *COMMON_TAGS],
            triggers=[
                get_event_trigger(MOSAIC_ENFACE_STITCHED, project_name=normalized_project_name),
            ],
            concurrency_limit=1,
        ),
        # ============================================================================
        # Slice Registration Event Deployment
        # ============================================================================
        register_slice_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "slice-registration", *COMMON_TAGS],
            triggers=[
                get_event_trigger(SLICE_READY, project_name=normalized_project_name),
            ],
        ),
        # ============================================================================
        # State Management Flow Deployments
        # ============================================================================
        unified_state_management_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "state-management", "unified", *COMMON_TAGS],
            triggers=[
                get_event_trigger(
                    MOSAIC_ENFACE_STITCHED,
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                    },
                    project_name=normalized_project_name,
                ),
            ],
            concurrency_limit=1,
        ),
        # ============================================================================
        # Slack Notification Flow Deployments
        # ============================================================================
        slack_enface_notification_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "slack-notifications", "enface-stitched", *COMMON_TAGS],
            triggers=[
                get_event_trigger(MOSAIC_ENFACE_STITCHED, project_name=normalized_project_name),
            ],
        ),
    ]


def build_register_deployments(
    *,
    project_name: str,
    deployment_name: str = "local",
) -> List:
    """
    Build only the two register deployments.
    project_name is required.
    Use project_name='all' to make it None internally.
    """
    normalized_project_name = _normalize_project_name(project_name)

    return [
        register_slice_event_flow.to_deployment(
            name=deployment_name,
            tags=["event-driven", "slice-registration", *COMMON_TAGS],
            triggers=[
                get_event_trigger(SLICE_READY, project_name=normalized_project_name),
            ],
        ),
        register_slice_event_flow.to_deployment(
            name=deployment_name,
            tags=["slice-registration", *COMMON_TAGS],
        ),
    ]


@serve_cli.command
def register(
    project_name: str = "all",
    deployment_name: str = "local",
):
    serve(
        *build_register_deployments(
            project_name=project_name,
            deployment_name=deployment_name,
        )
    )


@serve_cli.command
def all(
    project_name: str,
    deployment_name: str = "local",
    exclude: Optional[list[str]] = None,
):
    services = build_deployments(
        project_name=project_name,
        deployment_name=deployment_name,
    )

    if exclude:
        exclude_set = set(exclude)
        services = [
            deployment
            for deployment in services
            if getattr(deployment, "flow_name", None) not in exclude_set
        ]

    if not services:
        raise ValueError("No deployments to serve (all services excluded).")

    serve(*services)