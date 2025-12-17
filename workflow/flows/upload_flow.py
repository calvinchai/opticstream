import logging
from typing import Any, Dict, List

import prefect
from prefect import flow
from prefect.events import DeploymentEventTrigger

from workflow.tasks.upload import (upload_to_dandi_task, upload_to_linc_batch_task,
                                   upload_to_linc_task)
from workflow.tasks.utils import get_mosaic_paths

logger = logging.getLogger(__name__)


@flow(name="upload_flow")
def upload_flow(
    file_path: str,
    instance="linc"
):
    if instance == "linc":

        task = upload_to_linc_task.submit(file_path)
    elif instance == "dandi":

        task = upload_to_dandi_task.submit(file_path)
    else:
        raise ValueError(f"Invalid instance: {instance}")
    task.wait()
    return True


@flow(name="upload_to_linc_batch_flow")
def upload_to_linc_batch_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    archived_file_paths: List[str],
):
    """
    Event-driven flow triggered by 'tile_batch.upload_to_linc.ready' event.
    Runs upload_to_linc_batch_task with the list of archived files from the event
    payload.
    """
    # Use slice-based structure
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    state_path.mkdir(parents=True, exist_ok=True)

    batch_uploaded_path = state_path / f"batch-{batch_id}.uploaded"
    if batch_uploaded_path.exists():
        logger.info(f"Batch {batch_id} already uploaded")
        return

    if not archived_file_paths:
        logger.warning(
            f"No archived files provided for batch {batch_id} in mosaic {mosaic_id}")
        return

    logger.info(
        f"Uploading {len(archived_file_paths)} archived files for batch {batch_id} in "
        f"mosaic {mosaic_id}")

    # Run upload_to_linc_batch_task with the file list from event payload
    upload_to_linc_batch_task(file_list=archived_file_paths)

    # Mark batch as uploaded
    batch_uploaded_path.touch()
    logger.info(f"Batch {batch_id} uploaded successfully")

    return {
        "mosaic_id": mosaic_id,
        "batch_id": batch_id,
        "uploaded": True,
        "files_uploaded": len(archived_file_paths),
    }


@flow
def upload_to_linc_batch_event_flow(
    payload: Dict[str, Any],
):
    return upload_to_linc_batch_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        batch_id=payload["batch_id"],
        archived_file_paths=payload["archived_file_paths"],
    )


upload_to_linc_batch_deployment = upload_to_linc_batch_flow.to_deployment(
    name="upload_to_linc_batch_flow",
    tags=["tile-batch", "upload-to-linc"],
)

if __name__ == "__main__":
    upload_to_linc_batch_flow_deployment = upload_to_linc_batch_flow.to_deployment(
        name="upload_to_linc_batch_flow",
    )
    upload_to_linc_batch_event_flow_deployment = (
        upload_to_linc_batch_event_flow.to_deployment(
            name="upload_to_linc_batch_payload_flow",
            triggers=[
                DeploymentEventTrigger(
                    expect={"tile_batch.upload_to_linc.ready"},
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
        ))
    prefect.serve(upload_to_linc_batch_flow_deployment,
                  upload_to_linc_batch_event_flow_deployment)
