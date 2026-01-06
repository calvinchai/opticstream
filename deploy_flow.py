from prefect import serve
from prefect.events import DeploymentEventTrigger

from workflow.events import BATCH_ARCHIVED, get_event_trigger
from workflow.flows.mosaic_processing_flow import process_mosaic_event_flow
from workflow.flows.slice_registration_flow import register_slice_event_flow
from workflow.flows.tile_batch_flow import (
    complex_to_processed_batch_event_flow,
    complex_to_processed_batch_flow,
    process_tile_batch_flow,
)
from workflow.flows.upload_flow import (
    upload_to_linc_batch_deployment,
    upload_to_linc_batch_event_flow,
)

process_tile_batch_deployment = process_tile_batch_flow.to_deployment(
    name="process_tile_batch_flow",
    tags=["tile-batch", "process-tile-batch"],
    concurrency_limit=1,
)

# Event-driven deployment that listens for the tile_batch.complex2processed.ready
# event and forwards the event payload into the flow's `payload` parameter.
complex_to_processed_batch_event_deployment = (
    complex_to_processed_batch_event_flow.to_deployment(
        name="complex_to_processed_batch_event_flow",
        tags=["event-driven", "tile-batch", "complex-to-processed"],
        triggers=[
            DeploymentEventTrigger(
                expect={"tile_batch.complex2processed.ready"},
                parameters={
                    "payload": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload | tojson }}",
                        },
                    },
                },
            )
        ],
        concurrency_limit=1,
    )
)

complex_to_processed_batch_deployment = complex_to_processed_batch_flow.to_deployment(
    name="complex_to_processed_batch_flow",
    tags=["tile-batch", "complex-to-processed"],
    concurrency_limit=1,
)

upload_to_linc_batch_event_deployment = upload_to_linc_batch_event_flow.to_deployment(
    name="upload_to_linc_batch_event_flow",
    tags=["event-driven", "tile-batch", "upload-to-linc"],
    triggers=[
        get_event_trigger(BATCH_ARCHIVED),
    ],
)

if __name__ == "__main__":
    serve(
        process_tile_batch_deployment,
        complex_to_processed_batch_deployment,
        complex_to_processed_batch_event_deployment,
        upload_to_linc_batch_deployment,
        upload_to_linc_batch_event_deployment,
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
            concurrency_limit=2,
        ),
        register_slice_event_flow.to_deployment(
            name="register_slice_event_flow",
            tags=["event-driven", "slice-registration"],
            triggers=[
                DeploymentEventTrigger(
                    expect={"slice.ready"},
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
        ),
    )
