"""
Deployment script for register slice event flow only.

This script deploys only the register_slice_event_flow, which is triggered
by the SLICE_READY event when both mosaics for a slice are stitched.

Usage:
    python deploy_register_slice_event.py
"""

from prefect import serve

# Import flow
from workflow.flows.slice_registration_flow import register_slice_event_flow

# Import event utilities
from workflow.events import SLICE_READY, get_event_trigger

# ============================================================================
# Slice Registration Flow Deployment
# ============================================================================

register_slice_event_deployment = register_slice_event_flow.to_deployment(
    name="register_slice_event_flow",
    tags=["event-driven", "slice-registration"],
    triggers=[
        get_event_trigger(SLICE_READY),
    ],
)

# ============================================================================
# Serve Deployment
# ============================================================================

if __name__ == "__main__":
    serve(
        register_slice_event_deployment,
    )

