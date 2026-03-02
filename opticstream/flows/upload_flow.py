from typing import Any, Dict, List

import prefect
from prefect import flow
from prefect.events import emit_event
from prefect.logging import get_run_logger

from opticstream.events import (
    BATCH_ARCHIVED,
    BATCH_UPLOADED,
    MOSAIC_STITCHED,
    MOSAIC_VOLUME_STITCHED,
    get_event_trigger,
)
from opticstream.state import UPLOADED, get_batch_flag_path
from opticstream.tasks.common_tasks import (
    upload_to_dandi_task,
    upload_to_linc_batch_task,
    upload_to_linc_task,
)
from opticstream.utils.utils import (
    get_mosaic_paths,
)


@flow
def upload_flow(file_path: str, instance="linc"):
    if instance == "linc":
        task = upload_to_linc_task.submit(file_path)
    elif instance == "dandi":
        task = upload_to_dandi_task.submit(file_path)
    else:
        raise ValueError(f"Invalid instance: {instance}")
    task.wait()
    return True


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-upload-to-linc")
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
        resource={
            "prefect.resource.id": f"batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
            "batch_id": str(batch_id),
        },
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


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-upload-enface-to-dandi")
def upload_mosaic_enface_to_dandi_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    enface_outputs: Dict[str, str],
):
    """
    Event-driven flow triggered by 'linc.oct.mosaic.stitched' event.
    Uploads stitched enface nifti files to DANDI.

    The enface files are symlinked to the DANDI directory, so we upload
    the DANDI slice directory containing the symlinks.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    enface_outputs : Dict[str, str]
        Dictionary mapping modality to output file paths
    """

    logger = get_run_logger()
    logger.info(
        f"Uploading enface files for mosaic {mosaic_id} to DANDI from {list(enface_outputs.values())}"
    )

    upload_to_linc_batch_task(file_list=list(enface_outputs.values()), realpath=False)
    logger.info(f"Successfully uploaded enface files for mosaic {mosaic_id} to DANDI")

    return {
        "mosaic_id": mosaic_id,
        "uploaded": True,
        "files_uploaded": len(enface_outputs),
    }


@flow
def upload_mosaic_enface_to_dandi_event_flow(
    payload: Dict[str, Any],
):
    """
    Event wrapper flow for uploading enface files to DANDI.
    """
    return upload_mosaic_enface_to_dandi_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        enface_outputs=payload["symlink_targets"],
    )


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-upload-volume-to-dandi")
def upload_mosaic_volume_to_dandi_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    volume_outputs: Dict[str, str],
):
    """
    Event-driven flow triggered by 'linc.oct.mosaic.volume_stitched' event.
    Uploads stitched volume zarr files to DANDI.

    The volume files are written directly to the DANDI directory, so we upload
    the DANDI slice directory containing the volume files.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    volume_outputs : Dict[str, str]
        Dictionary mapping modality to volume file paths
    """
    from pathlib import Path

    logger = get_run_logger()
    logger.info(f"Uploading volume files for mosaic {mosaic_id} to DANDI")

    # Collect all volume file paths and verify they exist
    file_paths = []
    for modality, volume_path in volume_outputs.items():
        volume_path_obj = Path(volume_path)
        if volume_path_obj.exists():
            file_paths.append(str(volume_path_obj))
        else:
            logger.warning(
                f"Volume file does not exist for modality {modality}: {volume_path}"
            )

    if not file_paths:
        logger.warning(f"No volume files to upload for mosaic {mosaic_id}")
        return

    upload_to_linc_batch_task(file_list=file_paths, realpath=False)
    logger.info(f"Successfully uploaded volume files for mosaic {mosaic_id} to DANDI")


@flow
def upload_mosaic_volume_to_dandi_event_flow(
    payload: Dict[str, Any],
):
    """
    Event wrapper flow for uploading volume files to DANDI.
    """
    return upload_mosaic_volume_to_dandi_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        volume_outputs=payload["volume_outputs"],
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
    upload_mosaic_enface_to_dandi_event_flow_deployment = (
        upload_mosaic_enface_to_dandi_event_flow.to_deployment(
            name="upload_mosaic_enface_to_dandi_event_flow",
            triggers=[
                get_event_trigger(MOSAIC_STITCHED),
            ],
        )
    )
    upload_mosaic_volume_to_dandi_event_flow_deployment = (
        upload_mosaic_volume_to_dandi_event_flow.to_deployment(
            name="upload_mosaic_volume_to_dandi_event_flow",
            triggers=[
                get_event_trigger(MOSAIC_VOLUME_STITCHED),
            ],
        )
    )
    prefect.serve(
        upload_to_linc_batch_flow_deployment,
        upload_to_linc_batch_event_flow_deployment,
        upload_mosaic_enface_to_dandi_event_flow_deployment,
        upload_mosaic_volume_to_dandi_event_flow_deployment,
    )
