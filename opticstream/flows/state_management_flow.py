"""
Event-driven flows for managing processing state and updating Artifacts.

These flows monitor flag files and update Prefect Artifacts to track progress
at different levels: batch, mosaic, and slice.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from prefect import flow, task
from prefect.artifacts import create_table_artifact
from prefect.events import DeploymentEventTrigger, emit_event
from prefect.logging import get_run_logger

from opticstream.events import (
    BATCH_ARCHIVED,
    BATCH_PROCESSED,
    MOSAIC_READY,
    MOSAIC_STITCHED,
    SLICE_READY,
    get_event_trigger,
)
from opticstream.state.oct_project_state import (
    OCT_STATE_SERVICE,
)
from opticstream.state.project_state_core import ProcessingState


@task(task_run_name="{project_name}-mosaic-{mosaic_id}-check-batch-state")
def check_batch_state_task(
    project_base_path: str,
    mosaic_id: int,
    project_name: Optional[str] = None,
) -> Dict[str, int]:
    """
    Check batch state by scanning flag files.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    project_name : str, optional
        Project name for retrieving grid size from project variables

    Returns
    -------
    Dict[str, int]
        Dictionary with counts for each batch state:
        - total_batches: Total number of batches (from grid_size_x)
        - started_batches: Number of batches started
        - archived_batches: Number of batches archived
        - processed_batches: Number of batches processed
        - uploaded_batches: Number of batches uploaded
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    mosaic_view = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    )
    if mosaic_view is None:
        logger.warning(f"No OCT state found for mosaic {mosaic_id} in project {project_name}")
        return {
            "total_batches": 0,
            "started_batches": 0,
            "archived_batches": 0,
            "processed_batches": 0,
            "uploaded_batches": 0,
        }

    batches = list(mosaic_view.batches.values())
    total_batches = len(batches)
    started_batches = sum(
        1 for b in batches if b.processing_state != ProcessingState.PENDING
    )
    archived_batches = sum(1 for b in batches if b.archived)
    processed_batches = sum(1 for b in batches if b.processed)
    uploaded_batches = sum(1 for b in batches if b.uploaded)

    if total_batches == 0:
        logger.warning(f"No batches found in OCT state for mosaic {mosaic_id}")

    return {
        "total_batches": total_batches,
        "started_batches": started_batches,
        "archived_batches": archived_batches,
        "processed_batches": processed_batches,
        "uploaded_batches": uploaded_batches,
    }


@task(task_run_name="{project_name}-mosaic-{mosaic_id}-update-artifact")
def update_mosaic_artifact_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_state: Optional[Dict[str, int]] = None,
) -> str:
    """
    Update Prefect Artifact with mosaic batch progress.

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_state : Dict[str, int], optional
        Batch state dictionary (if None, will be recovered from flag files)

    Returns
    -------
    str
        Artifact key
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    mosaic_view = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    )
    if mosaic_view is None:
        logger.warning(f"No OCT state found for mosaic {mosaic_id} in project {project_name}")
        return ""

    batches = list(mosaic_view.batches.values())
    total_batches = len(batches)
    processed_batches = sum(1 for b in batches if b.processed)
    archived_batches = sum(1 for b in batches if b.archived)
    uploaded_batches = sum(1 for b in batches if b.uploaded)
    progress_percentage = (
        processed_batches / total_batches * 100 if total_batches > 0 else 0.0
    )

    # Create table artifact with progress information
    artifact_key = (
        f"{project_name.lower().replace('_', '-')}-mosaic-{mosaic_id}-progress"
    )

    # Calculate percentages
    started_pct = (
        (
            sum(
                1 for b in batches if b.processing_state != ProcessingState.PENDING
            )
            / total_batches
            * 100
        )
        if total_batches > 0
        else 0.0
    )
    archived_pct = (
        (archived_batches / total_batches * 100) if total_batches > 0 else 0.0
    )
    uploaded_pct = (
        (uploaded_batches / total_batches * 100) if total_batches > 0 else 0.0
    )

    # Create table data as list of dictionaries
    table_data = [
        {"State": "Total Batches", "Count": total_batches, "Percentage": "100.0%"},
        {
            "State": "Started",
            "Count": sum(
                1 for b in batches if b.processing_state != ProcessingState.PENDING
            ),
            "Percentage": f"{started_pct:.1f}%",
        },
        {
            "State": "Archived",
            "Count": archived_batches,
            "Percentage": f"{archived_pct:.1f}%",
        },
        {
            "State": "Processed",
            "Count": processed_batches,
            "Percentage": f"{progress_percentage:.1f}%",
        },
        {
            "State": "Uploaded",
            "Count": uploaded_batches,
            "Percentage": f"{uploaded_pct:.1f}%",
        },
    ]

    status_text = (
        "✅ COMPLETE - All batches processed"
        if total_batches > 0 and processed_batches == total_batches
        else "⏳ IN PROGRESS - Processing batches..."
    )
    milestones = [
        "✅" if progress_percentage >= 25 else "⏳",
        "✅" if progress_percentage >= 50 else "⏳",
        "✅" if progress_percentage >= 75 else "⏳",
        "✅" if progress_percentage >= 100 else "⏳",
    ]

    description = f"""Mosaic {mosaic_id} Processing Progress

Project: {project_name}
Mosaic ID: {mosaic_id}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Status: {status_text}

Milestones:
- {milestones[0]} 25% Complete
- {milestones[1]} 50% Complete
- {milestones[2]} 75% Complete
- {milestones[3]} 100% Complete
"""

    create_table_artifact(
        key=artifact_key,
        table=table_data,
        description=description,
    )

    logger.info(
        f"Updated artifact {artifact_key}: {processed_batches}/{total_batches} batches processed ({progress_percentage:.1f}%)"
    )

    return artifact_key


@task(task_run_name="{project_name}-update-all-mosaics-artifact")
def update_project_mosaic_artifact_task(
    project_name: str,
    project_base_path: str,
) -> str:
    """
    Update Prefect Artifact with unified table showing all mosaics in the project.

    This creates a single table artifact that displays all mosaics with their
    batch progress and status information (enface stitched, 3D volume stitched, etc.).

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project

    Returns
    -------
    str
        Artifact key
    """
    logger = get_run_logger()

    # Use OCT project state as the authoritative source
    project_view = OCT_STATE_SERVICE.peek_project_by_parts(project_name)

    # Collect state for each mosaic
    table_data = []
    for slice_view in project_view.slices.values():
        for mosaic_view in slice_view.mosaics.values():
            batches = list(mosaic_view.batches.values())
            total_batches = len(batches)
            started_batches = sum(
                1 for b in batches if b.processing_state != ProcessingState.PENDING
            )
            archived_batches = sum(1 for b in batches if b.archived)
            processed_batches = sum(1 for b in batches if b.processed)
            uploaded_batches = sum(1 for b in batches if b.uploaded)

            # Format status columns as checkmarks
            enface_stitched = "✅" if mosaic_view.enface_stitched else "⏳"
            volume_stitched = "✅" if mosaic_view.volume_stitched else "⏳"
            volume_uploaded = "✅" if mosaic_view.volume_uploaded else "⏳"

            table_data.append(
                {
                    "Mosaic ID": mosaic_view.mosaic_id,
                    "Total Batches": total_batches,
                    "Started": started_batches,
                    "Archived": archived_batches,
                    "Processed": processed_batches,
                    "Uploaded": uploaded_batches,
                    "Enface Stitched": enface_stitched,
                    "3D Volume Stitched": volume_stitched,
                    "3D Volume Uploaded": volume_uploaded,
                }
            )

    if not table_data:
        logger.warning(f"No valid mosaic states found for project {project_name}")
        return ""

    # Create unified artifact key
    artifact_key = f"{project_name.lower().replace('_', '-')}-all-mosaics-progress"

    # Calculate summary statistics
    total_mosaics = len(table_data)
    complete_mosaics = sum(
        1
        for row in table_data
        if row["Processed"] == row["Total Batches"] and row["Total Batches"] > 0
    )
    enface_stitched_count = sum(1 for row in table_data if row["Enface Stitched"] == "✅")
    volume_stitched_count = sum(
        1 for row in table_data if row["3D Volume Stitched"] == "✅"
    )
    volume_uploaded_count = sum(
        1 for row in table_data if row["3D Volume Uploaded"] == "✅"
    )

    description = f"""All Mosaics Processing Progress

Project: {project_name}
Total Mosaics: {total_mosaics}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Summary:
- Complete Mosaics: {complete_mosaics}/{total_mosaics}
- Enface Stitched: {enface_stitched_count}/{total_mosaics}
- 3D Volume Stitched: {volume_stitched_count}/{total_mosaics}
- 3D Volume Uploaded: {volume_uploaded_count}/{total_mosaics}
"""

    create_table_artifact(
        key=artifact_key,
        table=table_data,
        description=description,
    )

    logger.info(
        f"Updated unified artifact {artifact_key}: {total_mosaics} mosaics, "
        f"{complete_mosaics} complete, {enface_stitched_count} enface stitched, "
        f"{volume_stitched_count} volume stitched, {volume_uploaded_count} volume uploaded"
    )

    return artifact_key


@task(task_run_name="{project_name}-mosaic-{mosaic_id}-check-completion")
def check_mosaic_completion_task(
    project_base_path: str,
    mosaic_id: int,
    batch_state: Optional[Dict[str, int]] = None,
    project_name: Optional[str] = None,
) -> bool:
    """
    Check if all batches in a mosaic are processed.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_state : Dict[str, int], optional
        Batch state dictionary (if None, will be recovered from flag files)
    project_name : str, optional
        Project name for retrieving grid size from project variables

    Returns
    -------
    bool
        True if all batches are processed, False otherwise
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    mosaic_view = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    )
    if mosaic_view is None:
        logger.warning(f"No OCT state found for mosaic {mosaic_id} in project {project_name}")
        return False

    is_complete = mosaic_view.all_batches_done()

    if is_complete:
        logger.info(
            f"Mosaic {mosaic_id}: all batches finished according to OCT project state"
        )
    else:
        logger.debug(f"Mosaic {mosaic_id}: batches not yet all finished")

    return is_complete


@task(task_run_name="{project_name}-slice-{slice_number}-check-state")
def check_slice_state_task(
    project_base_path: str,
    slice_number: int,
    project_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Check state of both mosaics in a slice.

    Slice n has mosaics 2n-1 (normal) and 2n (tilted).

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
    project_name : str, optional
        Project name for retrieving grid size from project variables

    Returns
    -------
    Dict[str, Any]
        Dictionary with:
        - normal_mosaic_id: Normal mosaic ID (2n-1)
        - tilted_mosaic_id: Tilted mosaic ID (2n)
        - normal_mosaic_state: Batch state for normal mosaic
        - tilted_mosaic_state: Batch state for tilted mosaic
        - normal_complete: True if normal mosaic is complete
        - tilted_complete: True if tilted mosaic is complete
        - both_complete: True if both mosaics are complete
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    # Derive normal and tilted mosaic IDs for this slice
    normal_mosaic_id = 2 * slice_number - 1
    tilted_mosaic_id = 2 * slice_number

    normal_mosaic = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=normal_mosaic_id,
    )
    tilted_mosaic = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=tilted_mosaic_id,
    )

    normal_complete = normal_mosaic.all_batches_done() if normal_mosaic else False
    tilted_complete = tilted_mosaic.all_batches_done() if tilted_mosaic else False

    both_complete = normal_complete and tilted_complete

    logger.info(
        f"Slice {slice_number}: normal_complete={normal_complete}, "
        f"tilted_complete={tilted_complete}, both_complete={both_complete}"
    )

    return {
        "normal_mosaic_id": normal_mosaic_id,
        "tilted_mosaic_id": tilted_mosaic_id,
        "normal_complete": normal_complete,
        "tilted_complete": tilted_complete,
        "both_complete": both_complete,
    }


@task(task_run_name="{project_name}-slice-{slice_number}-update-artifact")
def update_slice_artifact_task(
    project_name: str,
    project_base_path: str,
    slice_number: int,
    slice_state: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Update Prefect Artifact with slice progress (both mosaics).

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number
    slice_state : Dict[str, Any], optional
        Slice state dictionary (if None, will be recovered from flag files)

    Returns
    -------
    str
        Artifact key
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    if slice_state is None:
        slice_state = check_slice_state_task(
            project_base_path=project_base_path,
            slice_number=slice_number,
            project_name=project_name,
        )

    normal_mosaic_id = slice_state["normal_mosaic_id"]
    tilted_mosaic_id = slice_state["tilted_mosaic_id"]

    normal_mosaic = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=normal_mosaic_id,
    )
    tilted_mosaic = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=tilted_mosaic_id,
    )

    normal_batches = list(normal_mosaic.batches.values()) if normal_mosaic else []
    tilted_batches = list(tilted_mosaic.batches.values()) if tilted_mosaic else []

    normal_total = len(normal_batches)
    tilted_total = len(tilted_batches)
    normal_processed = sum(1 for b in normal_batches if b.processed)
    tilted_processed = sum(1 for b in tilted_batches if b.processed)

    normal_progress = (
        normal_processed / normal_total * 100 if normal_total > 0 else 0.0
    )
    tilted_progress = (
        tilted_processed / tilted_total * 100 if tilted_total > 0 else 0.0
    )

    artifact_key = (
        f"{project_name.lower().replace('_', '-')}-slice-{slice_number}-progress"
    )

    # Create table data as list of dictionaries
    table_data = [
        {
            "Mosaic": f"Normal (Mosaic {normal_mosaic_id})",
            "State": "Total Batches",
            "Count": normal_total,
            "Percentage": "100.0%",
        },
        {
            "Mosaic": f"Normal (Mosaic {normal_mosaic_id})",
            "State": "Processed",
            "Count": normal_processed,
            "Percentage": f"{normal_progress:.1f}%",
        },
        {
            "Mosaic": f"Tilted (Mosaic {tilted_mosaic_id})",
            "State": "Total Batches",
            "Count": tilted_total,
            "Percentage": "100.0%",
        },
        {
            "Mosaic": f"Tilted (Mosaic {tilted_mosaic_id})",
            "State": "Processed",
            "Count": tilted_processed,
            "Percentage": f"{tilted_progress:.1f}%",
        },
    ]

    normal_status = (
        "✅ COMPLETE" if normal_mosaic and normal_mosaic.all_batches_done() else "⏳ IN PROGRESS"
    )
    tilted_status = (
        "✅ COMPLETE" if tilted_mosaic and tilted_mosaic.all_batches_done() else "⏳ IN PROGRESS"
    )
    overall_status = (
        "✅ READY FOR REGISTRATION - Both mosaics complete"
        if slice_state["both_complete"]
        else "⏳ WAITING - Processing mosaics..."
    )

    description = f"""Slice {slice_number} Processing Progress

Project: {project_name}
Slice Number: {slice_number}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Normal Illumination (Mosaic {normal_mosaic_id}): {normal_status}
Tilted Illumination (Mosaic {tilted_mosaic_id}): {tilted_status}

Overall Slice Status: {overall_status}
"""

    create_table_artifact(
        key=artifact_key,
        table=table_data,
        description=description,
    )

    logger.info(
        f"Updated artifact {artifact_key}: Normal={normal_progress:.1f}%, Tilted={tilted_progress:.1f}%"
    )

    return artifact_key


@task(task_run_name="mosaic-{mosaic_id}-check-stitched")
def check_mosaic_stitched_task(
    project_base_path: str,
    mosaic_id: int,
    project_name: Optional[str] = None,
) -> bool:
    """
    Check if mosaic has been stitched by looking for stitched output files and flag files.

    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier

    Returns
    -------
    bool
        True if mosaic is stitched, False otherwise
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    mosaic_view = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    )
    if mosaic_view is None:
        logger.debug(
            f"Mosaic {mosaic_id} has no OCT state yet for project {project_name}"
        )
        return False

    if mosaic_view.enface_stitched:
        logger.info(f"Mosaic {mosaic_id} is stitched according to OCT project state")
        return True

    logger.debug(f"Mosaic {mosaic_id} not yet stitched in OCT project state")
    return False


@task(
    task_run_name="{project_name}-mosaic-{mosaic_id}-check-completion-and-emit-mosaic-ready"
)
def check_completion_and_emit_mosaic_ready_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
) -> bool:
    """
    Consolidated task to:
    1. Check if all batches in a mosaic are complete
    2. If complete and not already stitched, emit MOSAIC_READY

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier

    Returns
    -------
    bool
        True if all batches are complete, False otherwise
    """
    logger = get_run_logger()

    if project_name is None:
        raise ValueError("project_name is required for OCT project state access")

    mosaic_view = OCT_STATE_SERVICE.read_mosaic_by_parts(
        project_name=project_name,
        mosaic_id=mosaic_id,
    )
    if mosaic_view is None:
        logger.debug(
            f"Mosaic {mosaic_id}: no OCT state yet for project {project_name}"
        )
        return False

    total_batches = len(mosaic_view.batches)
    is_complete = mosaic_view.all_batches_done()

    if not is_complete:
        logger.debug(
            f"Mosaic {mosaic_id}: batches not yet all finished in OCT project state"
        )
        return False

    logger.info(
        f"Mosaic {mosaic_id}: all batches finished according to OCT project state"
    )

    # Check if already stitched to avoid duplicate events
    is_stitched = check_mosaic_stitched_task.fn(
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        project_name=project_name,
    )

    if is_stitched:
        logger.info(f"Mosaic {mosaic_id} already stitched, skipping {MOSAIC_READY} emission")
        return True

    logger.info(
        f"All batches processed for mosaic {mosaic_id} and not stitched yet. "
        f"Emitting {MOSAIC_READY} event."
    )
    emit_event(
        event=MOSAIC_READY,
        resource={
            "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
            "project_name": project_name,
            "mosaic_id": str(mosaic_id),
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "total_batches": total_batches,
            "triggered_by": "batch_flows",
        },
    )

    return True


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-manage-batch-state")
def manage_mosaic_batch_state_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
) -> Dict[str, Any]:
    """
    Event-driven flow to manage batch state for a mosaic.

    This flow is triggered by batch completion events and:
    1. Checks batch state via flag files
    2. Updates Prefect Artifact with progress
    3. Emits mosaic.processed event if all batches complete

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier

    Returns
    -------
    Dict[str, Any]
        Dictionary with batch state and completion status
    """
    logger = get_run_logger()
    logger.info(f"Managing batch state for mosaic {mosaic_id}")

    # Check batch state from flag files
    batch_state = check_batch_state_task(
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        project_name=project_name,
    )

    # Update unified project artifact with all mosaics
    artifact_key = update_project_mosaic_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
    )

    # Check if all batches are complete (via OCT project state)
    is_complete = check_mosaic_completion_task(
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_state=batch_state,
        project_name=project_name,
    )

    # If complete and not already stitched, emit mosaic.processed event
    if is_complete:
        # Check if already stitched to avoid duplicate events
        is_stitched = check_mosaic_stitched_task(
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            project_name=project_name,
        )

        if not is_stitched:
            logger.info(
                f"All batches processed for mosaic {mosaic_id}. "
                f"Emitting {MOSAIC_READY} event."
            )
            emit_event(
                event=MOSAIC_READY,
                resource={
                    "prefect.resource.id": f"mosaic:{project_name}:mosaic-{mosaic_id}",
                    "project_name": project_name,
                    "mosaic_id": str(mosaic_id),
                },
                payload={
                    "project_name": project_name,
                    "project_base_path": project_base_path,
                    "mosaic_id": mosaic_id,
                    "total_batches": batch_state["total_batches"],
                    "triggered_by": "state_management_flow",
                },
            )
        else:
            logger.info(f"Mosaic {mosaic_id} already stitched, skipping event emission")

    return {
        "mosaic_id": mosaic_id,
        "batch_state": batch_state,
        "is_complete": is_complete,
        "artifact_key": artifact_key,
    }


@flow
def manage_mosaic_batch_state_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of batch state management.

    Triggered by:
    - linc.oct.batch.processed (after each batch completes)
    - linc.oct.batch.archived (after each batch is archived)

    Parameters
    ----------
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int
        - batch_id: int (optional, not used but included in event)

    Returns
    -------
    Dict[str, Any]
        Result from manage_mosaic_batch_state_flow
    """
    return manage_mosaic_batch_state_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=int(payload["mosaic_id"]),
    )


@flow(flow_run_name="{project_name}-slice-{slice_number}-manage-state")
def manage_slice_state_flow(
    project_name: str,
    project_base_path: str,
    slice_number: int,
) -> Dict[str, Any]:
    """
    Event-driven flow to manage state for a slice (both mosaics).

    This flow:
    1. Checks state of both mosaics in the slice
    2. Updates Prefect Artifact with slice progress
    3. Emits slice.ready event if both mosaics are stitched

    Parameters
    ----------
    project_name : str
        Project identifier
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)

    Returns
    -------
    Dict[str, Any]
        Dictionary with slice state and completion status
    """
    logger = get_run_logger()
    logger.info(f"Managing state for slice {slice_number}")

    # Check state of both mosaics
    slice_state = check_slice_state_task(
        project_base_path=project_base_path,
        slice_number=slice_number,
        project_name=project_name,
    )

    # Update Artifact with slice progress
    slice_artifact_key = update_slice_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
        slice_number=slice_number,
        slice_state=slice_state,
    )

    # Also update unified project mosaic artifact (includes stitching status)
    unified_artifact_key = update_project_mosaic_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
    )

    # Check if both mosaics are stitched
    normal_stitched = check_mosaic_stitched_task(
        project_base_path=project_base_path,
        mosaic_id=slice_state["normal_mosaic_id"],
        project_name=project_name,
    )

    tilted_stitched = check_mosaic_stitched_task(
        project_base_path=project_base_path,
        mosaic_id=slice_state["tilted_mosaic_id"],
        project_name=project_name,
    )

    both_stitched = normal_stitched and tilted_stitched

    # If both mosaics are stitched, emit slice.ready event
    if both_stitched:
        logger.info(
            f"Both mosaics stitched for slice {slice_number}. "
            f"Emitting {SLICE_READY} event."
        )
        emit_event(
            event=SLICE_READY,
            resource={
                "prefect.resource.id": f"slice:{project_name}:slice-{slice_number}",
                "project_name": project_name,
                "slice_number": str(slice_number),
            },
            payload={
                "project_name": project_name,
                "project_base_path": project_base_path,
                "slice_number": slice_number,
                "normal_mosaic_id": slice_state["normal_mosaic_id"],
                "tilted_mosaic_id": slice_state["tilted_mosaic_id"],
                "triggered_by": "state_management_flow",
            },
        )

    return {
        "slice_number": slice_number,
        "slice_state": slice_state,
        "normal_stitched": normal_stitched,
        "tilted_stitched": tilted_stitched,
        "both_stitched": both_stitched,
        "slice_artifact_key": slice_artifact_key,
        "unified_artifact_key": unified_artifact_key,
    }


@flow
def manage_slice_state_event_flow(
    payload: dict,
) -> Dict[str, Any]:
    """
    Wrapper flow for event-driven triggering of slice state management.

    Triggered by:
    - linc.oct.mosaic.stitched (after each mosaic is stitched)

    Parameters
    ----------
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int

    Returns
    -------
    Dict[str, Any]
        Result from manage_slice_state_flow
    """
    # Determine slice number from mosaic_id
    # Normal: mosaic_id = 2n-1, so n = (mosaic_id + 1) / 2
    # Tilted: mosaic_id = 2n, so n = mosaic_id / 2
    mosaic_id = int(payload["mosaic_id"])

    if mosaic_id % 2 == 0:
        # Tilted illumination
        slice_number = mosaic_id // 2
    else:
        # Normal illumination
        slice_number = (mosaic_id + 1) // 2

    return manage_slice_state_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        slice_number=slice_number,
    )


@flow
def unified_state_management_event_flow(
    event: str,
    payload: dict,
) -> Dict[str, Any]:
    """
    Unified event flow to handle all state management events with a single concurrency limit.

    This flow routes events to the appropriate handler based on event type:
    - BATCH_PROCESSED, BATCH_ARCHIVED -> manage_mosaic_batch_state_event_flow
    - MOSAIC_STITCHED -> manage_slice_state_event_flow

    By using a single flow with concurrency_limit=1, we ensure that all state
    management operations are serialized, preventing race conditions when multiple
    events arrive simultaneously.

    Parameters
    ----------
    event : str
        The event name that triggered this flow
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int (for batch events)
        - slice_number: int (optional, for slice events)

    Returns
    -------
    Dict[str, Any]
        Result from the appropriate state management flow
    """
    logger = get_run_logger()
    logger.info(f"Unified state management flow triggered by event: {event}")

    # Route to batch state management for batch events
    if event in (BATCH_PROCESSED, BATCH_ARCHIVED):
        logger.info(
            f"Routing to batch state management for mosaic {payload.get('mosaic_id')}"
        )
        return manage_mosaic_batch_state_event_flow(payload)

    # Route to slice state management for mosaic stitched events
    elif event == MOSAIC_STITCHED:
        logger.info(
            f"Routing to slice state management for mosaic {payload.get('mosaic_id')}"
        )
        return manage_slice_state_event_flow(payload)

    else:
        error_msg = f"Unknown event type for state management: {event}"
        logger.error(error_msg)
        raise ValueError(error_msg)


# Deployment configurations
if __name__ == "__main__":
    # Unified deployment for all state management events with concurrency_limit=1
    # This ensures all state management operations are serialized to avoid race conditions
    unified_state_management_event_flow_deployment = (
        unified_state_management_event_flow.to_deployment(
            name="unified_state_management_event_flow",
            tags=["event-driven", "state-management", "unified"],
            triggers=[
                # Trigger when a batch completes processing
                DeploymentEventTrigger(
                    expect={BATCH_PROCESSED},
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                        "payload": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.payload | tojson }}",
                            },
                        },
                    },
                ),
                # Trigger when a batch is archived (for early progress tracking)
                DeploymentEventTrigger(
                    expect={BATCH_ARCHIVED},
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                        "payload": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.payload | tojson }}",
                            },
                        },
                    },
                ),
                # Trigger when a mosaic is stitched
                DeploymentEventTrigger(
                    expect={MOSAIC_STITCHED},
                    parameters={
                        "event": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.event }}",
                            },
                        },
                        "payload": {
                            "__prefect_kind": "json",
                            "value": {
                                "__prefect_kind": "jinja",
                                "template": "{{ event.payload | tojson }}",
                            },
                        },
                    },
                ),
            ],
            concurrency_limit=1,
        )
    )

    # Legacy deployments (kept for backward compatibility, but unified flow is recommended)
    manage_mosaic_batch_state_event_flow_deployment = (
        manage_mosaic_batch_state_event_flow.to_deployment(
            name="manage_mosaic_batch_state_event_flow",
            tags=["event-driven", "state-management", "mosaic", "legacy"],
            triggers=[
                # Trigger when a batch completes processing
                get_event_trigger(BATCH_PROCESSED),
                # Also trigger when a batch is archived (for early progress tracking)
                get_event_trigger(BATCH_ARCHIVED),
            ],
            concurrency_limit=1,
        )
    )

    # Deployment for slice state management (triggered by mosaic stitching events)
    manage_slice_state_event_flow_deployment = (
        manage_slice_state_event_flow.to_deployment(
            name="manage_slice_state_event_flow",
            tags=["event-driven", "state-management", "slice", "legacy"],
            triggers=[
                get_event_trigger(MOSAIC_STITCHED),
            ],
            concurrency_limit=1,
        )
    )

    # Deployments are created but not served here
    # Recommended: prefect deploy workflow/flows/state_management_flow.py:unified_state_management_event_flow_deployment
    # Legacy: prefect deploy workflow/flows/state_management_flow.py:manage_mosaic_batch_state_event_flow_deployment
    #         prefect deploy workflow/flows/state_management_flow.py:manage_slice_state_event_flow_deployment
