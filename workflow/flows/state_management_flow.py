"""
Event-driven flows for managing processing state and updating Artifacts.

These flows monitor flag files and update Prefect Artifacts to track progress
at different levels: batch, mosaic, and slice.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from prefect import flow
from prefect.events import emit_event, DeploymentEventTrigger

from workflow.tasks.state_management import (
    check_batch_state_task,
    update_mosaic_artifact_task,
    check_mosaic_completion_task,
    check_slice_state_task,
    update_slice_artifact_task,
    check_mosaic_stitched_task,
)

logger = logging.getLogger(__name__)


@flow(name="manage_mosaic_batch_state_flow")
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
    logger.info(f"Managing batch state for mosaic {mosaic_id}")
    
    # Check batch state from flag files
    batch_state = check_batch_state_task(
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
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
                f"Emitting mosaic.processed event."
            )
            emit_event(
                event="mosaic.processed",
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
                }
            )
        else:
            logger.info(
                f"Mosaic {mosaic_id} already stitched, skipping event emission"
            )
    
    return {
        "mosaic_id": mosaic_id,
        "batch_state": batch_state,
        "is_complete": is_complete,
        "artifact_key": artifact_key,
    }


@flow(name="manage_mosaic_batch_state_event_flow")
def manage_mosaic_batch_state_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of batch state management.
    
    Triggered by:
    - tile_batch.complex2processed.ready (after each batch completes)
    - tile_batch.archived.ready (after each batch is archived)
    
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


@flow(name="manage_slice_state_flow")
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
    logger.info(f"Managing state for slice {slice_number}")
    
    # Check state of both mosaics
    slice_state = check_slice_state_task(
        project_base_path=project_base_path,
        slice_number=slice_number,
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
            f"Emitting slice.ready event."
        )
        emit_event(
            event="slice.ready",
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
            }
        )
    
    return {
        "slice_number": slice_number,
        "slice_state": slice_state,
        "normal_stitched": normal_stitched,
        "tilted_stitched": tilted_stitched,
        "both_stitched": both_stitched,
        "artifact_key": artifact_key,
    }


@flow(name="manage_slice_state_event_flow")
def manage_slice_state_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of slice state management.
    
    Triggered by:
    - mosaic.stitched (after each mosaic is stitched)
    
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


# Deployment configurations
if __name__ == "__main__":
    # Deployment for batch state management (triggered by batch completion events)
    manage_mosaic_batch_state_event_flow_deployment = manage_mosaic_batch_state_event_flow.to_deployment(
        name="manage_mosaic_batch_state_event_flow",
        tags=["event-driven", "state-management", "mosaic"],
        triggers=[
            # Trigger when a batch completes processing
            DeploymentEventTrigger(
                expect={"tile_batch.complex2processed.ready"},
                parameters={
                    "payload": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload | tojson }}",
                        }
                    }
                },
            ),
            # Also trigger when a batch is archived (for early progress tracking)
            DeploymentEventTrigger(
                expect={"tile_batch.archived.ready"},
                parameters={
                    "payload": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload | tojson }}",
                        }
                    }
                },
            ),
        ],
        concurrency_limit=1
    )
    
    # Deployment for slice state management (triggered by mosaic stitching events)
    manage_slice_state_event_flow_deployment = manage_slice_state_event_flow.to_deployment(
        name="manage_slice_state_event_flow",
        tags=["event-driven", "state-management", "slice"],
        triggers=[
            DeploymentEventTrigger(
                expect={"mosaic.stitched"},
                parameters={
                    "payload": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload | tojson }}",
                        }
                    }
                },
            ),
        ],
        concurrency_limit=1

    )
    
    # Deployments are created but not served here
    # Use: prefect deploy workflow/flows/state_management_flow.py:manage_mosaic_batch_state_event_flow_deployment
    #      prefect deploy workflow/flows/state_management_flow.py:manage_slice_state_event_flow_deployment

