from __future__ import annotations

import prefect

from opticstream.cli.lsm.cli import lsm_cli
from opticstream.flows.lsm import (
    lsm_strip_update_event_flow_deployment,
    upload_strip_to_dandi_event_flow_deployment,
    upload_strip_to_dandi_flow_deployment,
)
from opticstream.flows.lsm.strip_process_flow import (
    process_strip,
    process_strip_event_to_deployment,
)


@lsm_cli.command
def serve(
    concurrent_workers: int = 2,
) -> None:
    process_strip_flow_deployment = process_strip.to_deployment(
        name="process_strip_flow_deployment",
        tags=["lsm", "process-strip"],
        concurrency_limit=concurrent_workers
    )
    process_strip_event_flow_deployment = process_strip_event_to_deployment(
        concurrent_workers=concurrent_workers,
    )
    prefect.serve(
        process_strip_flow_deployment,
        process_strip_event_flow_deployment,
        upload_strip_to_dandi_flow_deployment,
        upload_strip_to_dandi_event_flow_deployment,
        lsm_strip_update_event_flow_deployment,
    )

