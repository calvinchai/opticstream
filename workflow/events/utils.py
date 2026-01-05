"""
Event utilities for Prefect event-driven deployments.

This module provides utilities for creating event triggers and emitting events
"""

from prefect.events import DeploymentEventTrigger

from workflow.events.constants import get_event_name


def get_event_trigger(event_name: str) -> DeploymentEventTrigger:
    """
    Get a deployment event trigger for a given event name.
    
    Automatically converts legacy event names to canonical linc.oct.* format.
    
    Parameters
    ----------
    event_name : str
        The name of the event to trigger on (may be legacy or canonical format)
        
    Returns
    -------
    DeploymentEventTrigger
        Configured event trigger with Jinja2 parameter extraction
    """
    canonical_name = get_event_name(event_name)
    return DeploymentEventTrigger(
        expect={canonical_name},
        parameters={
            "payload": {
                "__prefect_kind": "json",
                "value": {
                    "__prefect_kind": "jinja",
                    "template": "{{ event.payload | tojson }}",
                }
            }
        },
    )