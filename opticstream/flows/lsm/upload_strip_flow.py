import os
from typing import Any, Dict, List
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import ShellOperation

from opticstream.config.lsm_scan_config import LSMScanConfig
from opticstream.events import get_event_trigger
from opticstream.flows.lsm.event import STRIP_COMPRESSED
from opticstream.state.lsm_project_state import LSMStripId
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
    command = f"{dandi_bin} upload "
    if dandi_instance != "dandi":
        command += f"-i {dandi_instance} "
    command += (
        f"{file_path} -J 10:10 --allow-any-path --existing overwrite --validation skip"
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
) -> None:
    """
    Upload the strip to DANDI.
    """
    logger = get_run_logger()
    logger.info("Uploading %s to DANDI", strip_ident)
    upload_to_dandi_task(output_path, dandi_instance, dandi_bin)
    logger.info(
        "Successfully uploaded %s to DANDI",
        strip_ident,
    )


def resolve_config(payload: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """
    Resolve configuration values from payload and project config.

    Priority: payload[key] → project_config.key → omit key

    Does not apply defaults - defaults must be in processing flow signature.

    Parameters
    ----------
    payload : Dict[str, Any]
        Event payload dictionary (must contain "project_name")
    keys : List[str]
        List of configuration keys to resolve

    Returns
    -------
    Dict[str, Any]
        Dictionary with resolved configuration values (only keys that were found)
    """
    project_name = payload["project_name"]
    project_config = LSMScanConfig.load(f"{project_name}-lsm-config")

    resolved = {}
    for key in keys:
        if key in payload:
            value = payload[key]
        elif project_config is not None and hasattr(project_config, key):
            value = getattr(project_config, key)
        else:
            continue  # Omit key if not found
    return resolved


@flow
def upload_strip_to_dandi_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event wrapper flow for uploading the strip to DANDI.
    """
    config = resolve_config(payload, ["dandi_instance", "dandi_bin"])
    strip_ident = LSMStripId(
        project_name=payload["project_name"],
        slice_id=payload["slice_id"],
        strip_id=payload["strip_id"],
        channel_id=payload.get("camera_id") or payload.get("channel_id") or 1,
    )
    return upload_strip_to_dandi_flow(
        strip_ident=strip_ident,
        output_path=payload["output_path"],
        **config,
    )


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
