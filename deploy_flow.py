from prefect import serve

from workflow.flows.tile_batch_flow import process_tile_batch_flow
from workflow.flows.tile_flow import process_tile_flow
from workflow.flows.upload_flow import upload_flow

from workflow.flows.mosaic_processing_flow import process_mosaic_event_flow
from prefect.events import DeploymentEventTrigger


if __name__ == "__main__":
    serve(
        process_tile_flow.to_deployment(name="process_tile", concurrency_limit=8),
        upload_flow.to_deployment(name="upload", concurrency_limit=12),
        process_tile_batch_flow.to_deployment(name="process_tile_batch", concurrency_limit=2),
        process_mosaic_event_flow.to_deployment(
        name="process_mosaic_event_flow",
        tags=["event-driven", "mosaic-processing", "stitching"],
        triggers=[
            DeploymentEventTrigger(
                expect={"mosaic.processed"},
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
        ],
    )
    
    )
