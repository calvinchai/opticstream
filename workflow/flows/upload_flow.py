from typing import Any, Dict, List

import prefect
from prefect import flow
from prefect.events import emit_event
from prefect.logging import get_run_logger

from workflow.events import BATCH_ARCHIVED, BATCH_UPLOADED, get_event_trigger
from workflow.state.flags import UPLOADED, get_batch_flag_path
from workflow.tasks.upload import (
    upload_to_dandi_task,
    upload_to_linc_batch_task,
    upload_to_linc_task,
)
from workflow.tasks.utils import get_mosaic_paths


@flow(name="upload_flow")
def upload_flow(file_path: str, instance="linc"):
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
    Event-driven flow triggered by 'linc.oct.batch.archived' event (Section 6.2, 11).
    Runs upload_to_linc_batch_task with the list of archived files from the event
    payload. Emits 'linc.oct.batch.uploaded' event upon completion.
    """
    logger = get_run_logger()
    # Use slice-based structure
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    state_path.mkdir(parents=True, exist_ok=True)

    batch_uploaded_path = get_batch_flag_path(state_path, batch_id, UPLOADED)
    if batch_uploaded_path.exists():
        logger.info(f"Batch {batch_id} already uploaded")
        return

    if not archived_file_paths:
        logger.warning(
            f"No archived files provided for batch {batch_id} in mosaic {mosaic_id}"
        )
        return

    logger.info(
        f"Uploading {len(archived_file_paths)} archived files for batch {batch_id} in "
        f"mosaic {mosaic_id}"
    )

    # Run upload_to_linc_batch_task with the file list from event payload
    upload_to_linc_batch_task(file_list=archived_file_paths)

    # Mark batch as uploaded
    batch_uploaded_path.touch()
    logger.info(f"Batch {batch_id} uploaded successfully")

    # Emit upload completion event (per Section 6.2)
    emit_event(
        event=BATCH_UPLOADED,
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "batch_id": batch_id,
            "files_uploaded": len(archived_file_paths),
        },
    )

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
                get_event_trigger(BATCH_ARCHIVED),
            ],
        )
    )
    prefect.serve(
        upload_to_linc_batch_flow_deployment, upload_to_linc_batch_event_flow_deployment
    )
