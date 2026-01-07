"""
Event-driven flows for managing processing state and updating Artifacts.

These flows monitor flag files and update Prefect Artifacts to track progress
at different levels: batch, mosaic, and slice.
"""

from typing import Any, Dict

from prefect import flow
from prefect.events import DeploymentEventTrigger, emit_event
from prefect.logging import get_run_logger

from workflow.events import (
    BATCH_ARCHIVED,
    BATCH_PROCESSED,
    MOSAIC_READY,
    MOSAIC_STITCHED,
    SLICE_READY,
    get_event_trigger,
)
from workflow.tasks.state_management import (
    check_batch_state_task,
    check_mosaic_completion_task,
    check_mosaic_stitched_task,
    check_slice_state_task,
    update_mosaic_artifact_task,
    update_slice_artifact_task,
)


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-manage-batch-state")
def manage_mosaic_batch_state_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
) -> Dict[str, Any]:
    """
    Event-driven flow to manage batch state for a mosaic.

    This flow is triggered by batch completion events and:
    1. Checks batch state via flag files
    2. Updates Prefect Artifact with progress
    3. Emits mosaic.processed event if all batches complete

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier

    Returns
    -------
    Dict[str, Any]
        Dictionary with batch state and completion status
    """
    logger = get_run_logger()
    logger.info(f"Managing batch state for mosaic {mosaic_id}")

    # Check batch state from flag files
    batch_state = check_batch_state_task(
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        project_name=project_name,
    )

    # Update Artifact with progress
    artifact_key = update_mosaic_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_state=batch_state,
    )

    # Check if all batches are complete
    is_complete = check_mosaic_completion_task(
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_state=batch_state,
        project_name=project_name,
    )

    # If complete and not already stitched, emit mosaic.processed event
    if is_complete:
        # Check if already stitched to avoid duplicate events
        is_stitched = check_mosaic_stitched_task(
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
        )

        if not is_stitched:
            logger.info(
                f"All batches processed for mosaic {mosaic_id}. "
                f"Emitting {MOSAIC_READY} event."
            )
            emit_event(
                event=MOSAIC_READY,
                resource={
                    "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
                    "project_name": project_name,
                    "mosaic_id": str(mosaic_id),
                },
                payload={
                    "project_name": project_name,
                    "project_base_path": project_base_path,
                    "mosaic_id": mosaic_id,
                    "total_batches": batch_state["total_batches"],
                    "triggered_by": "state_management_flow",
                },
            )
        else:
            logger.info(f"Mosaic {mosaic_id} already stitched, skipping event emission")

    return {
        "mosaic_id": mosaic_id,
        "batch_state": batch_state,
        "is_complete": is_complete,
        "artifact_key": artifact_key,
    }


@flow
def manage_mosaic_batch_state_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of batch state management.

    Triggered by:
    - linc.oct.batch.processed (after each batch completes)
    - linc.oct.batch.archived (after each batch is archived)

    Parameters
    ----------
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int
        - batch_id: int (optional, not used but included in event)

    Returns
    -------
    Dict[str, Any]
        Result from manage_mosaic_batch_state_flow
    """
    return manage_mosaic_batch_state_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=int(payload["mosaic_id"]),
    )


@flow(flow_run_name="{project_name}-slice-{slice_number}-manage-state")
def manage_slice_state_flow(
    project_name: str,
    project_base_path: str,
    slice_number: int,
) -> Dict[str, Any]:
    """
    Event-driven flow to manage state for a slice (both mosaics).

    This flow:
    1. Checks state of both mosaics in the slice
    2. Updates Prefect Artifact with slice progress
    3. Emits slice.ready event if both mosaics are stitched

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)

    Returns
    -------
    Dict[str, Any]
        Dictionary with slice state and completion status
    """
    logger = get_run_logger()
    logger.info(f"Managing state for slice {slice_number}")

    # Check state of both mosaics
    slice_state = check_slice_state_task(
        project_base_path=project_base_path,
        slice_number=slice_number,
        project_name=project_name,
    )

    # Update Artifact with slice progress
    artifact_key = update_slice_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
        slice_number=slice_number,
        slice_state=slice_state,
    )

    # Check if both mosaics are stitched
    normal_stitched = check_mosaic_stitched_task(
        project_base_path=project_base_path,
        mosaic_id=slice_state["normal_mosaic_id"],
    )

    tilted_stitched = check_mosaic_stitched_task(
        project_base_path=project_base_path,
        mosaic_id=slice_state["tilted_mosaic_id"],
    )

    both_stitched = normal_stitched and tilted_stitched

    # If both mosaics are stitched, emit slice.ready event
    if both_stitched:
        logger.info(
            f"Both mosaics stitched for slice {slice_number}. "
            f"Emitting {SLICE_READY} event."
        )
        emit_event(
            event=SLICE_READY,
            resource={
                "prefect.resource.id": f"slice:{project_name}:slice-{slice_number}",
                "project_name": project_name,
                "slice_number": str(slice_number),
            },
            payload={
                "project_name": project_name,
                "project_base_path": project_base_path,
                "slice_number": slice_number,
                "normal_mosaic_id": slice_state["normal_mosaic_id"],
                "tilted_mosaic_id": slice_state["tilted_mosaic_id"],
                "triggered_by": "state_management_flow",
            },
        )

    return {
        "slice_number": slice_number,
        "slice_state": slice_state,
        "normal_stitched": normal_stitched,
        "tilted_stitched": tilted_stitched,
        "both_stitched": both_stitched,
        "artifact_key": artifact_key,
    }


@flow
def manage_slice_state_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of slice state management.

    Triggered by:
    - linc.oct.mosaic.stitched (after each mosaic is stitched)

    Parameters
    ----------
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int

    Returns
    -------
    Dict[str, Any]
        Result from manage_slice_state_flow
    """
    # Determine slice number from mosaic_id
    # Normal: mosaic_id = 2n-1, so n = (mosaic_id + 1) / 2
    # Tilted: mosaic_id = 2n, so n = mosaic_id / 2
    mosaic_id = int(payload["mosaic_id"])

    if mosaic_id % 2 == 0:
        # Tilted illumination
        slice_number = mosaic_id // 2
    else:
        # Normal illumination
        slice_number = (mosaic_id + 1) // 2

    return manage_slice_state_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        slice_number=slice_number,
    )


@flow
def unified_state_management_event_flow(
    event: str,
    payload: dict,
) -> Dict[str, Any]:
    """
    Unified event flow to handle all state management events with a single concurrency limit.

    This flow routes events to the appropriate handler based on event type:
    - BATCH_PROCESSED, BATCH_ARCHIVED -> manage_mosaic_batch_state_event_flow
    - MOSAIC_STITCHED -> manage_slice_state_event_flow

    By using a single flow with concurrency_limit=1, we ensure that all state
    management operations are serialized, preventing race conditions when multiple
    events arrive simultaneously.

    Parameters
    ----------
    event : str
        The event name that triggered this flow
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int (for batch events)
        - slice_number: int (optional, for slice events)

    Returns
    -------
    Dict[str, Any]
        Result from the appropriate state management flow
    """
    logger = get_run_logger()
    logger.info(f"Unified state management flow triggered by event: {event}")

    # Route to batch state management for batch events
    if event in (BATCH_PROCESSED, BATCH_ARCHIVED):
        logger.info(
            f"Routing to batch state management for mosaic {payload.get('mosaic_id')}"
        )
        return manage_mosaic_batch_state_event_flow(payload)

    # Route to slice state management for mosaic stitched events
    elif event == MOSAIC_STITCHED:
        logger.info(
            f"Routing to slice state management for mosaic {payload.get('mosaic_id')}"
        )
        return manage_slice_state_event_flow(payload)

    else:
        error_msg = f"Unknown event type for state management: {event}"
        logger.error(error_msg)
        raise ValueError(error_msg)


# Deployment configurations
if __name__ == "__main__":
    # Unified deployment for all state management events with concurrency_limit=1
    # This ensures all state management operations are serialized to avoid race conditions
    unified_state_management_event_flow_deployment = (
        unified_state_management_event_flow.to_deployment(
            name="unified_state_management_event_flow",
            tags=["event-driven", "state-management", "unified"],
            triggers=[
                # Trigger when a batch completes processing
                DeploymentEventTrigger(
                    expect={BATCH_PROCESSED},
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                        "payload": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.payload | tojson }}",
                            },
                        },
                    },
                ),
                # Trigger when a batch is archived (for early progress tracking)
                DeploymentEventTrigger(
                    expect={BATCH_ARCHIVED},
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                        "payload": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.payload | tojson }}",
                            },
                        },
                    },
                ),
                # Trigger when a mosaic is stitched
                DeploymentEventTrigger(
                    expect={MOSAIC_STITCHED},
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                        "payload": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.payload | tojson }}",
                            },
                        },
                    },
                ),
            ],
            concurrency_limit=1,
        )
    )

    # Legacy deployments (kept for backward compatibility, but unified flow is recommended)
    manage_mosaic_batch_state_event_flow_deployment = (
        manage_mosaic_batch_state_event_flow.to_deployment(
            name="manage_mosaic_batch_state_event_flow",
            tags=["event-driven", "state-management", "mosaic", "legacy"],
            triggers=[
                # Trigger when a batch completes processing
                get_event_trigger(BATCH_PROCESSED),
                # Also trigger when a batch is archived (for early progress tracking)
                get_event_trigger(BATCH_ARCHIVED),
            ],
            concurrency_limit=1,
        )
    )

    # Deployment for slice state management (triggered by mosaic stitching events)
    manage_slice_state_event_flow_deployment = (
        manage_slice_state_event_flow.to_deployment(
            name="manage_slice_state_event_flow",
            tags=["event-driven", "state-management", "slice", "legacy"],
            triggers=[
                get_event_trigger(MOSAIC_STITCHED),
            ],
            concurrency_limit=1,
        )
    )

    # Deployments are created but not served here
    # Recommended: prefect deploy workflow/flows/state_management_flow.py:unified_state_management_event_flow_deployment
    # Legacy: prefect deploy workflow/flows/state_management_flow.py:manage_mosaic_batch_state_event_flow_deployment
    #         prefect deploy workflow/flows/state_management_flow.py:manage_slice_state_event_flow_deployment
