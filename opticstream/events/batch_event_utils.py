"""
Batch event emission utilities.

Deduplicates batch event emission to ensure consistent resource IDs
and payload structure across all batch events.
"""

from typing import Any, Dict

from prefect.events import emit_event


def emit_batch_event(
    event_name: str,
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    payload: Dict[str, Any],
) -> None:
    """
    Emit a batch event with consistent resource and payload structure.

    Constructs consistent resource dict with prefect.resource.id format
    and merges common payload fields.

    Parameters
    ----------
    event_name : str
        Event name constant (e.g., BATCH_COMPLEXED)
    project_name : str
        Project name
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier
    payload : Dict[str, Any]
        Additional payload fields (will be merged with common fields)
    """
    resource = {
        "prefect.resource.id": f"batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}",
        "project_name": project_name,
        "mosaic_id": str(mosaic_id),
        "batch_id": str(batch_id),
    }

    # Merge common payload fields with provided payload
    merged_payload = {
        "project_name": project_name,
        "project_base_path": project_base_path,
        "mosaic_id": mosaic_id,
        "batch_id": batch_id,
        **payload,
    }

    emit_event(event=event_name, resource=resource, payload=merged_payload)
