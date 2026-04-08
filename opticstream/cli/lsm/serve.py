from __future__ import annotations

from cyclopts import App
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
from opticstream.flows.lsm.strip_archive_flow import archive_strip
from opticstream.flows.lsm.strip_process_flow import (
    to_deployment as strip_process_deployments,
)
from opticstream.flows.lsm.strip_upload_flow import (
    to_deployment as strip_upload_deployments,
)
from opticstream.utils.runtime_paths import chdir_to_opticstream_install_root

serve = lsm_cli.command(App(name="serve"))


@serve.command
def all(
    concurrent_workers: int = 2,
    process:bool = False,
) -> None:
    chdir_to_opticstream_install_root()
    deployments: list = []
    if process:
    # Hierarchy: strip -> channel -> upload
        deployments.extend(
            strip_process_deployments(
                deployment_name="local",
                concurrent_workers=concurrent_workers,
                extra_tags=("lsm",),
            )
        )
    deployments.extend(
        strip_upload_deployments(
            deployment_name="local",
            extra_tags=("lsm",),
        )
    )
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

@serve.command
def process(
    concurrent_workers: int = 2,
):
    chdir_to_opticstream_install_root()
    strip_process_deployments(
        deployment_name="local",
        concurrent_workers=concurrent_workers,
        extra_tags=("lsm",),
    ).serve()

@serve.command
def archive():
    chdir_to_opticstream_install_root()
    archive_strip.serve(con)