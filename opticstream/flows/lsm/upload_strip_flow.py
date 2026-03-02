

import os
from typing import Any, Dict, List
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import ShellOperation

from opticstream.config.blocks import LSMScanConfig
from opticstream.events import get_event_trigger
from opticstream.flows.lsm.event import STRIP_COMPRESSED


@task(tags=["dandi-upload"], retries=1)
def upload_to_dandi_task(file_path: str, 
dandi_instance: str = "linc",
dandi_bin: str = "dandi") -> None:
    """
    Upload the file to DANDI.
    """
    logger = get_run_logger()
    if dandi_instance == "linc":
         env={
            "LINC_API_KEY": Secret.load("linc-api-key", validate=False).get(),
            "DANDI_API_KEY": Secret.load("linc-api-key", validate=False).get()
        }
    elif dandi_instance == "dandi":
        env={
            "DANDI_API_KEY": Secret.load("dandi-api-key", validate=False).get(),
        }
    env["DANDI_DEVEL"] = "1"
    command = f"{dandi_bin} upload " 
    if dandi_instance != "dandi":
        command += f"-i {dandi_instance} "
    command += f"'{file_path}' -J 10:10 --allow-any-path --existing overwrite --validation skip"
    with ShellOperation(
        commands=[command],
        env=env,
        working_dir=os.path.dirname(file_path),
    ) as upload_operation:
        upload_operation.trigger()

@flow(flow_run_name="{project_name}-process-slice-{slice_number}-strip-{strip_number}-upload-to-dandi")
def upload_strip_to_dandi_flow(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    dandi_instance: str = "linc",
    dandi_bin: str = "dandi") -> None:
    """
    Upload the strip to DANDI.
    """
    logger = get_run_logger()
    logger.info(f"Uploading strip {strip_number} of slice {slice_number} to DANDI")
    upload_to_dandi_task(strip_path, dandi_instance, dandi_bin)
    logger.info(f"Successfully uploaded strip {strip_number} of slice {slice_number} to DANDI")


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



@flow(flow_run_name="{project_name}-process-slice-{slice_number}-strip-{strip_number}-upload-to-dandi-event")
def upload_strip_to_dandi_event_flow(payload: Dict[str, Any]) -> None:
    """
    Event wrapper flow for uploading the strip to DANDI.
    """
    config = resolve_config(payload, ["dandi_instance", "dandi_bin"])

    return upload_strip_to_dandi_flow(
        **config,
    )

upload_strip_to_dandi_flow_deployment = upload_strip_to_dandi_flow.to_deployment(
    name="upload_strip_to_dandi_flow"
)
upload_strip_to_dandi_event_flow_deployment = upload_strip_to_dandi_event_flow.to_deployment(
    name="upload_strip_to_dandi_event_flow",
    triggers=[
        get_event_trigger(STRIP_COMPRESSED),
    ],
)
