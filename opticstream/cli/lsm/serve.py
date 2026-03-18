from __future__ import annotations

import prefect

from opticstream.cli.lsm.cli import lsm_cli
from opticstream.flows.lsm.process_strip_flow import process_strip_event, process_strip
from opticstream.flows.lsm.upload_strip_flow import (
    upload_strip_to_dandi_event_flow_deployment,
    upload_strip_to_dandi_flow_deployment,
)


@lsm_cli.command
def serve(
    concurrent_workers: int = 2,
) -> None:
    process_strip_flow_deployment = process_strip.to_deployment(
        name="process_strip_flow_deployment",
        tags=["lsm", "process-strip"],
        concurrency_limit=concurrent_workers,
        work_pool_name="default11",
        work_queue_name="low-io",
    )
    process_strip_event_flow_deployment = process_strip_event.to_deployment(
        name="process_strip_event_flow_deployment",
        tags=["lsm", "process-strip"],
        concurrency_limit=concurrent_workers,
    )
    prefect.serve(
        process_strip_flow_deployment,
        process_strip_event_flow_deployment,
        upload_strip_to_dandi_flow_deployment,
        upload_strip_to_dandi_event_flow_deployment,
    )

