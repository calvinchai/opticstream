"""
Event utilities for Prefect event-driven deployments.
"""

from typing import Any, Dict, Optional

from prefect.events import DeploymentEventTrigger


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
    # Do not add project_name to trigger_params: event flows take only payload.
    # project_name is used below only for the match filter (which events trigger).

    # Schema per https://docs.prefect.io/v3/concepts/event-triggers: expect is array of strings, match filters on resource labels
    trigger_kwargs: Dict[str, Any] = {
        "expect": [event_name],
        "parameters": trigger_params,
    }
    if project_name is not None:
        trigger_kwargs["match"] = {"linc.opticstream.project": project_name}

    return DeploymentEventTrigger(**trigger_kwargs)
