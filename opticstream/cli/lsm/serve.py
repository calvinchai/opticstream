from __future__ import annotations

import prefect

from opticstream.cli.lsm.cli import lsm_cli
from opticstream.flows.lsm.channel_process_flow import (
    to_deployment as channel_process_deployments,
)
from opticstream.flows.lsm.channel_upload_flow import (
    to_deployment as channel_upload_deployments,
)
from opticstream.flows.lsm.channel_volume_flow import (
    to_deployment as channel_volume_deployments,
)
from opticstream.flows.lsm.strip_process_flow import (
    to_deployment as strip_process_deployments,
)
from opticstream.flows.lsm.strip_upload_flow import (
    to_deployment as strip_upload_deployments,
)
from opticstream.utils.runtime_paths import chdir_to_opticstream_install_root


@lsm_cli.command
def serve(
    concurrent_workers: int = 2,
    serve_all: bool = False,
) -> None:
    chdir_to_opticstream_install_root()
    deployments: list = []

    # Hierarchy: strip -> channel -> upload
    # deployments.extend(
    #     strip_process_deployments(
    #         deployment_name="local",
    #         concurrent_workers=concurrent_workers,
    #         extra_tags=("lsm",),
    #     )
    # )
    deployments.extend(
        strip_upload_deployments(
            deployment_name="local",
            extra_tags=("lsm",),
        )
    )
    if serve_all:
        deployments.extend(
            channel_process_deployments(
                deployment_name="local",
                extra_tags=("lsm",),
            )
        )
        deployments.extend(
            channel_volume_deployments(
                deployment_name="local",
                extra_tags=("lsm",),
            )
        )
        deployments.extend(
            channel_upload_deployments(
                deployment_name="local",
                extra_tags=("lsm",),
            )
        )

    prefect.serve(*deployments)
