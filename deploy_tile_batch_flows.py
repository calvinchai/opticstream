"""
Deployment script for tile batch event-driven flows.

This script creates Prefect deployments with DeploymentEventTrigger for:
1. complex_to_processed_batch_event_flow - triggered by 'tile_batch.complex2processed.ready' event
2. upload_to_linc_batch_event_flow - triggered by 'tile_batch.upload_to_linc.ready' event

Usage:
    python deploy_tile_batch_flows.py

The deployments are configured with DeploymentEventTrigger to automatically trigger
when the respective events are emitted. Event payload parameters are passed to
the flows using Jinja templates.

Based on Prefect 3.0 documentation:
- https://docs-3.prefect.io/v3/how-to-guides/automations/passing-event-payloads-to-flows
- https://docs-3.prefect.io/v3/how-to-guides/automations/chaining-deployments-with-events
"""

import argparse
from prefect import serve
from prefect.deployments import Deployment
from prefect.events import DeploymentEventTrigger
from workflow.flows.tile_batch_flow import (
    complex_to_processed_batch_event_flow,
    upload_to_linc_batch_event_flow,
)


def create_deployments_serve():
    """Create deployments using serve() with DeploymentEventTrigger."""
    complex_deployment = complex_to_processed_batch_event_flow.to_deployment(
        name="complex_to_processed_batch",
        description="Processes complex data to processed format when triggered by tile_batch.complex2processed.ready event",
        tags=["event-driven", "batch-processing", "complex2processed"],
        triggers=[
            DeploymentEventTrigger(
                expect={"tile_batch.complex2processed.ready"},
                parameters={
                    "project_name": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_name }}",
                    },
                    "project_base_path": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_base_path }}",
                    },
                    "mosaic_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.mosaic_id }}",
                    },
                    "batch_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.batch_id }}",
                    },
                },
            )
        ],
    )
    
    upload_deployment = upload_to_linc_batch_event_flow.to_deployment(
        name="upload_to_linc_batch",
        description="Uploads batch to LINC when triggered by tile_batch.upload_to_linc.ready event",
        tags=["event-driven", "batch-processing", "upload"],
        triggers=[
            DeploymentEventTrigger(
                expect={"tile_batch.upload_to_linc.ready"},
                parameters={
                    "project_name": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_name }}",
                    },
                    "project_base_path": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_base_path }}",
                    },
                    "mosaic_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.mosaic_id }}",
                    },
                    "batch_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.batch_id }}",
                    },
                    "archived_file_paths": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.archived_file_paths }}",
                    },
                },
            )
        ],
    )
    
    serve(complex_deployment, upload_deployment)


def create_deployments_apply():
    """Create deployments using Deployment.build_from_flow() and apply() - more control."""
    complex_deployment = Deployment.build_from_flow(
        flow=complex_to_processed_batch_event_flow,
        name="complex_to_processed_batch",
        description="Processes complex data to processed format when triggered by tile_batch.complex2processed.ready event",
        tags=["event-driven", "batch-processing", "complex2processed"],
        work_queue_name=None,  # Can specify work queue if needed
        triggers=[
            DeploymentEventTrigger(
                expect={"tile_batch.complex2processed.ready"},
                parameters={
                    "project_name": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_name }}",
                    },
                    "project_base_path": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_base_path }}",
                    },
                    "mosaic_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.mosaic_id }}",
                    },
                    "batch_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.batch_id }}",
                    },
                },
            )
        ],
    )
    
    upload_deployment = Deployment.build_from_flow(
        flow=upload_to_linc_batch_event_flow,
        name="upload_to_linc_batch",
        description="Uploads batch to LINC when triggered by tile_batch.upload_to_linc.ready event",
        tags=["event-driven", "batch-processing", "upload"],
        work_queue_name=None,  # Can specify work queue if needed
        triggers=[
            DeploymentEventTrigger(
                expect={"tile_batch.upload_to_linc.ready"},
                parameters={
                    "project_name": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_name }}",
                    },
                    "project_base_path": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.project_base_path }}",
                    },
                    "mosaic_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.mosaic_id }}",
                    },
                    "batch_id": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.batch_id }}",
                    },
                    "archived_file_paths": {
                        "__prefect_kind": "jinja",
                        "template": "{{ event.payload.archived_file_paths }}",
                    },
                },
            )
        ],
    )
    
    complex_deployment.apply()
    upload_deployment.apply()
    
    print("Deployments created successfully!")
    print("\nDeployments are configured with DeploymentEventTrigger.")
    print("They will automatically trigger when the respective events are emitted.")
    print("\nTo test:")
    print("1. Start the serve process: python deploy_tile_batch_flows.py")
    print("2. Emit an event using prefect event emit or from your flow")
    print("3. The deployment will automatically trigger")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deploy tile batch event-driven flows"
    )
    parser.add_argument(
        "--method",
        choices=["serve", "apply"],
        default="serve",
        help="Deployment method: 'serve' (long-running) or 'apply' (one-time)",
    )
    
    args = parser.parse_args()
    
    if args.method == "serve":
        create_deployments_serve()
    else:
        create_deployments_apply()
