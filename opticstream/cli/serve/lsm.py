import prefect

from opticstream.cli.serve.cli import serve_cli
from opticstream.flows.lsm.process_strip_flow import process_strip_flow
from opticstream.flows.lsm.upload_strip_flow import upload_strip_to_dandi_event_flow_deployment, upload_strip_to_dandi_flow_deployment

@serve_cli.command
def lsm(
    concurrent_compress_workers: int = 4,

):
    process_strip_flow_deployment = process_strip_flow.to_deployment(
        name="process_strip_flow_deployment",
        tags=["lsm", "process-strip"],
        concurrency_limit=concurrent_compress_workers,
    )
    prefect.serve(
        process_strip_flow_deployment,
        upload_strip_to_dandi_flow_deployment,
        upload_strip_to_dandi_event_flow_deployment,
    )