from prefect.events import DeploymentEventTrigger


def get_event_trigger(event_name: str) -> DeploymentEventTrigger:
    """
    Get a deployment event trigger for a given event name.
    
    Parameters
    ----------
    event_name : str
        The name of the event to trigger on.
    """
    return DeploymentEventTrigger(
        expect={event_name},
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