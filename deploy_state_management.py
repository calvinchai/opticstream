"""
Deployment script for state management flows.

This script deploys event-driven flows that manage processing state
and update Prefect Artifacts.
"""

import sys
from pathlib import Path

# Add workflow directory to path
sys.path.insert(0, str(Path(__file__).parent))

from prefect import serve
from workflow.flows.state_management_flow import (
    manage_mosaic_batch_state_event_flow_deployment,
    manage_slice_state_event_flow_deployment,
)

if __name__ == "__main__":
    serve(
        manage_mosaic_batch_state_event_flow_deployment,
        manage_slice_state_event_flow_deployment,
    )
