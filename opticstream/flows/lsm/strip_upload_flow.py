import os
import shlex
from typing import Any, Dict, List

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import ShellOperation

from opticstream.config.lsm_scan_config import LSMScanConfig, get_lsm_scan_config
from opticstream.events import get_event_trigger
from opticstream.flows.lsm.event import STRIP_COMPRESSED
from opticstream.flows.lsm.paths import strip_zarr_output_path
from opticstream.flows.lsm.state_guards import (
    force_rerun_from_payload,
    prepare_idempotent_strip_milestone,
)
from opticstream.flows.lsm.utils import strip_ident_from_payload
from opticstream.state.lsm_project_state import LSM_STATE_SERVICE, LSMStripId
from opticstream.config.constants import DANDI_API_TOKEN_BLOCK_NAME, LINC_API_TOKEN_BLOCK_NAME

@task(tags=["dandi-upload"], retries=1)
def upload_to_dandi_task(
    file_path: str, dandi_instance: str = "linc", dandi_bin: str = "dandi"
) -> None:
    """
    Upload the file to DANDI.
    """
    logger = get_run_logger()
    if dandi_instance == "linc":
        env = {
            "LINC_API_KEY": Secret.load(LINC_API_TOKEN_BLOCK_NAME, validate=False).get(),
            "DANDI_API_KEY": Secret.load(LINC_API_TOKEN_BLOCK_NAME, validate=False).get(),
        }
    elif dandi_instance == "dandi":
        env = {
            "DANDI_API_KEY": Secret.load(DANDI_API_TOKEN_BLOCK_NAME, validate=False).get(),
        }
    env["DANDI_DEVEL"] = "1"
    command = f"{shlex.quote(dandi_bin)} upload "
    if dandi_instance != "dandi":
        command += f"-i {shlex.quote(dandi_instance)} "
    command += (
        f"{shlex.quote(file_path)} -J 10:10 --allow-any-path --existing overwrite --validation skip"
    )
    logger.info(command)
    with ShellOperation(
        commands=[command],
        env=env,
        working_dir=os.path.dirname(file_path),
    ) as upload_operation:
        upload_operation_process = upload_operation.trigger()
        upload_operation_process.wait_for_completion()
        logger.info(upload_operation_process.fetch_result())


@flow(flow_run_name="upload-to-dandi-{strip_ident}")
def upload_strip_to_dandi_flow(
    strip_ident: LSMStripId,
    output_path: str,
    dandi_instance: str = "linc",
    dandi_bin: str = "dandi",
    force_rerun: bool = False,
) -> None:
    """
    Upload the strip to DANDI.
    """
    logger = get_run_logger()
    logger.info(f"Uploading {strip_ident} to DANDI")
    strip_view = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
    uploaded = bool(strip_view and strip_view.uploaded)
    if (
        prepare_idempotent_strip_milestone(
            logger,
            milestone_done=uploaded,
            force_rerun=force_rerun,
            skip_log=(
                f"{strip_ident} already marked uploaded; skipping upload because force_rerun is False"
            ),
            force_log=f"{strip_ident} already marked uploaded; forcing rerun",
            strip_ident=strip_ident,
            reset_strip=lambda s: s.reset_uploaded(),
        )
        == "skip"
    ):
        return
    upload_to_dandi_task(output_path, dandi_instance, dandi_bin)
    with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident) as strip_state:
        strip_state.set_uploaded(True)
    logger.info(f"Successfully uploaded {strip_ident} to DANDI")


def resolve_config(
    payload: Dict[str, Any], keys: List[str]
) -> tuple[Dict[str, Any], LSMScanConfig]:
    """
    Resolve configuration values from payload and project config.

    Priority: payload[key] → project_config.key → omit key

    Returns resolved dict and the loaded scan config (single load per call).
    """
    strip_ident = strip_ident_from_payload(payload)
    project_name = strip_ident.project_name
    cfg = get_lsm_scan_config(
        project_name, override_config_name=payload.get("override_config")
    )
    resolved: Dict[str, Any] = {}
    for key in keys:
        if key in payload:
            resolved[key] = payload[key]
        elif hasattr(cfg, key):
            resolved[key] = getattr(cfg, key)
    return resolved, cfg


@flow
def upload_strip_to_dandi_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event wrapper flow for uploading the strip to DANDI.

    Payload requires ``strip_ident`` (dict); zarr path is derived from config.
    """
    strip_ident = strip_ident_from_payload(payload)
    config, cfg = resolve_config(payload, ["dandi_instance", "dandi_bin"])
    output_path = strip_zarr_output_path(strip_ident, cfg)
    return upload_strip_to_dandi_flow(
        strip_ident=strip_ident,
        output_path=output_path,
        force_rerun=force_rerun_from_payload(payload),
        **config,
    )

def to_deployment():
    upload_strip_to_dandi_flow_deployment = upload_strip_to_dandi_flow.to_deployment(
        name="upload_strip_to_dandi_flow"
    )
    upload_strip_to_dandi_event_flow_deployment = (
        upload_strip_to_dandi_event_flow.to_deployment(
            name="upload_strip_to_dandi_event_flow",
            triggers=[
                get_event_trigger(STRIP_COMPRESSED),
            ],
        )
    )
    return upload_strip_to_dandi_flow_deployment, upload_strip_to_dandi_event_flow_deployment