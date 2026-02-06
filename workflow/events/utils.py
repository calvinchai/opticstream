"""
Event utilities for Prefect event-driven deployments.

This module provides utilities for creating event triggers and emitting events
"""

from typing import Any, Dict, Optional

from prefect.events import DeploymentEventTrigger, emit_event


def get_event_trigger(
    event_name: str,
    parameters: Optional[Dict[str, Any]] = None,
    project_name: Optional[str] = None,
) -> DeploymentEventTrigger:
    """
    Get a deployment event trigger for a given event name.

    Automatically converts legacy event names to canonical linc.oct.* format.

    Parameters
    ----------
    event_name : str
        The name of the event to trigger on (may be legacy or canonical format)
    parameters : dict, optional
        Extra parameters to merge into the trigger (e.g. for flow run inputs).
    project_name : str, optional
        When set, only events whose resource has this project_name will trigger
        the deployment. When not set, no project filter is applied.

    Returns
    -------
    DeploymentEventTrigger
        Configured event trigger with Jinja2 parameter extraction
    """
    parameters = parameters or {}
    trigger_params = {
        **parameters,
        "payload": {
            "__prefect_kind": "json",
            "value": {
                "__prefect_kind": "jinja",
                "template": "{{ event.payload | tojson }}",
            },
        },
    }
    if project_name is not None:
        trigger_params["project_name"] = project_name

    # Schema per https://docs.prefect.io/v3/concepts/event-triggers: expect is array of strings, match filters on resource labels
    trigger_kwargs: Dict[str, Any] = {
        "expect": [event_name],
        "parameters": trigger_params,
    }
    if project_name is not None:
        trigger_kwargs["match"] = {"project_name": project_name}

    return DeploymentEventTrigger(**trigger_kwargs)


def emit_mosaic_event(
    event_name: str,
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    payload: Dict[str, Any],
) -> None:
    """
    Emit a mosaic-level event with standardized resource ID construction.

    This helper function standardizes the emission of mosaic events by
    constructing the resource ID consistently and including standard fields.

    Parameters
    ----------
    event_name : str
        Event name constant (e.g., MOSAIC_STITCHED, MOSAIC_VOLUME_STITCHED)
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    payload : Dict[str, Any]
        Event payload dictionary (should include project_name, project_base_path, mosaic_id)

    Examples
    --------
    >>> emit_mosaic_event(
    ...     event_name=MOSAIC_STITCHED,
    ...     project_name="my_project",
    ...     project_base_path="/path/to/project",
    ...     mosaic_id=1,
    ...     payload={
    ...         "project_name": "my_project",
    ...         "project_base_path": "/path/to/project",
    ...         "mosaic_id": 1,
    ...         "enface_outputs": {...},
    ...     },
    ... )
    """
    emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
        },
        payload=payload,
    )


def emit_slice_event(
    event_name: str,
    project_name: str,
    project_base_path: str,
    slice_number: int,
    payload: Dict[str, Any],
) -> None:
    """
    Emit a slice-level event with standardized resource ID construction.

    This helper function standardizes the emission of slice events by
    constructing the resource ID consistently and including standard fields.

    Parameters
    ----------
    event_name : str
        Event name constant (e.g., SLICE_READY, SLICE_REGISTERED)
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
    payload : Dict[str, Any]
        Event payload dictionary (should include project_name, project_base_path, slice_number)

    Examples
    --------
    >>> emit_slice_event(
    ...     event_name=SLICE_REGISTERED,
    ...     project_name="my_project",
    ...     project_base_path="/path/to/project",
    ...     slice_number=1,
    ...     payload={
    ...         "project_name": "my_project",
    ...         "project_base_path": "/path/to/project",
    ...         "slice_number": 1,
    ...         "normal_mosaic_id": 1,
    ...         "tilted_mosaic_id": 2,
    ...         "outputs": {...},
    ...     },
    ... )
    """
    emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"slice:{project_name}:slice-{slice_number}",
            "project_name": project_name,
            "slice_number": str(slice_number),
        },
        payload=payload,
    )
