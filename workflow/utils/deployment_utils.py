"""
Deployment utilities for Prefect event-driven flows.

This module provides helper functions to create standardized deployment
configurations for event-driven flows, reducing boilerplate code.
"""

from typing import List, Optional

from prefect import Flow
from prefect.events import DeploymentEventTrigger

from workflow.events import get_event_trigger


def create_event_deployment(
    flow: Flow,
    name: str,
    event_name: str,
    tags: Optional[List[str]] = None,
    concurrency_limit: Optional[int] = None,
) -> Flow:
    """
    Create a standard event-driven deployment for a flow.

    This helper creates a deployment configuration for a flow that is triggered
    by a single event. It standardizes the deployment setup and reduces boilerplate.

    Parameters
    ----------
    flow : Flow
        The Prefect flow to create a deployment for
    name : str
        Deployment name
    event_name : str
        Event name to trigger on
    tags : List[str], optional
        Tags for the deployment
    concurrency_limit : int, optional
        Concurrency limit for the deployment

    Returns
    -------
    Flow
        The flow with deployment configuration (for use with to_deployment())

    Examples
    --------
    >>> deployment = create_event_deployment(
    ...     process_tile_batch_event_flow,
    ...     name="process_tile_batch_event_flow",
    ...     event_name=BATCH_READY,
    ...     tags=["event-driven", "tile-batch"],
    ... )
    """
    if tags is None:
        tags = ["event-driven"]

    deployment = flow.to_deployment(
        name=name,
        tags=tags,
        triggers=[get_event_trigger(event_name)],
    )

    if concurrency_limit is not None:
        deployment.concurrency_limit = concurrency_limit

    return deployment


def create_unified_deployment(
    flow: Flow,
    name: str,
    event_names: List[str],
    tags: Optional[List[str]] = None,
    concurrency_limit: Optional[int] = None,
) -> Flow:
    """
    Create a unified deployment for a flow that handles multiple events.

    This helper creates a deployment configuration for flows that need to handle
    multiple event types (like unified_state_management_event_flow).

    Parameters
    ----------
    flow : Flow
        The Prefect flow to create a deployment for
    name : str
        Deployment name
    event_names : List[str]
        List of event names to trigger on
    tags : List[str], optional
        Tags for the deployment
    concurrency_limit : int, optional
        Concurrency limit for the deployment

    Returns
    -------
    Flow
        The flow with deployment configuration (for use with to_deployment())

    Examples
    --------
    >>> deployment = create_unified_deployment(
    ...     unified_state_management_event_flow,
    ...     name="unified_state_management_event_flow",
    ...     event_names=[BATCH_PROCESSED, BATCH_ARCHIVED, MOSAIC_STITCHED],
    ...     tags=["event-driven", "state-management", "unified"],
    ...     concurrency_limit=1,
    ... )
    """
    if tags is None:
        tags = ["event-driven", "unified"]

    triggers = [get_event_trigger(event_name) for event_name in event_names]

    deployment = flow.to_deployment(
        name=name,
        tags=tags,
        triggers=triggers,
    )

    if concurrency_limit is not None:
        deployment.concurrency_limit = concurrency_limit

    return deployment

