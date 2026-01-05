"""
Tasks for managing processing state and updating Prefect Artifacts.

These tasks check flag files and update Artifacts to track progress
at different levels: batch, mosaic, and slice.

State can be stored in Prefect Variables for faster retrieval, with flag files
as the authoritative source that is checked when Variables are stale or missing.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from workflow.state import BatchState, MosaicState, ProjectState, SliceState
from workflow.tasks.utils import get_mosaic_paths


@task(name="check_batch_state")
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
    mosaic_state = MosaicState(project_base_path, mosaic_id, project_name)
    
    if mosaic_state.total_batches == 0:
        logger.warning(f"No batches found for mosaic {mosaic_id} at {mosaic_state.state_path}")
    
    return mosaic_state.to_dict()


@task(name="update_mosaic_artifact")
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
    
    # Always recover state from flag files (authoritative source)
    mosaic_state = MosaicState(project_base_path, mosaic_id, project_name)
    
    total_batches = mosaic_state.total_batches
    processed_batches = mosaic_state.processed_batches
    archived_batches = mosaic_state.archived_batches
    uploaded_batches = mosaic_state.uploaded_batches
    progress_percentage = mosaic_state.get_progress_percentage()

    # Create table artifact with progress information
    artifact_key = f"{project_name}_mosaic_{mosaic_id}_progress"

    # Calculate percentages
    started_pct = (mosaic_state.started_batches / total_batches * 100) if total_batches > 0 else 0.0
    archived_pct = (archived_batches / total_batches * 100) if total_batches > 0 else 0.0
    uploaded_pct = (uploaded_batches / total_batches * 100) if total_batches > 0 else 0.0

    # Create table data as list of dictionaries
    table_data = [
        {
            "State": "Total Batches",
            "Count": total_batches,
            "Percentage": "100.0%"
        },
        {
            "State": "Started",
            "Count": mosaic_state.started_batches,
            "Percentage": f"{started_pct:.1f}%"
        },
        {
            "State": "Archived",
            "Count": archived_batches,
            "Percentage": f"{archived_pct:.1f}%"
        },
        {
            "State": "Processed",
            "Count": processed_batches,
            "Percentage": f"{progress_percentage:.1f}%"
        },
        {
            "State": "Uploaded",
            "Count": uploaded_batches,
            "Percentage": f"{uploaded_pct:.1f}%"
        }
    ]

    status_text = "✅ COMPLETE - All batches processed" if mosaic_state.is_complete() else "⏳ IN PROGRESS - Processing batches..."
    milestones = [
        "✅" if progress_percentage >= 25 else "⏳",
        "✅" if progress_percentage >= 50 else "⏳",
        "✅" if progress_percentage >= 75 else "⏳",
        "✅" if progress_percentage >= 100 else "⏳"
    ]
    
    description = f"""Mosaic {mosaic_id} Processing Progress

Project: {project_name}
Mosaic ID: {mosaic_id}
Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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


@task(name="check_mosaic_completion")
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
    
    # Recover state from flag files
    mosaic_state = MosaicState(project_base_path, mosaic_id, project_name)
    
    is_complete = mosaic_state.is_complete()

    if is_complete:
        logger.info(
            f"Mosaic {mosaic_id}: All {mosaic_state.processed_batches}/{mosaic_state.total_batches} batches processed")
    else:
        logger.debug(
            f"Mosaic {mosaic_id}: {mosaic_state.processed_batches}/{mosaic_state.total_batches} batches processed")

    return is_complete


@task(name="check_slice_state")
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
    # Recover slice state from flag files
    slice_state = SliceState(project_base_path, slice_number, project_name)
    
    return slice_state.to_dict()


@task(name="update_slice_artifact")
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
    
    # Recover slice state from flag files
    slice_state_obj = SliceState(project_base_path, slice_number, project_name)
    
    normal_mosaic_id = slice_state_obj.normal_mosaic_id
    tilted_mosaic_id = slice_state_obj.tilted_mosaic_id
    normal_mosaic = slice_state_obj.normal_mosaic
    tilted_mosaic = slice_state_obj.tilted_mosaic

    normal_progress = normal_mosaic.get_progress_percentage()
    tilted_progress = tilted_mosaic.get_progress_percentage()

    artifact_key = f"{project_name}_slice_{slice_number}_progress"

    # Create table data as list of dictionaries
    table_data = [
        {
            "Mosaic": f"Normal (Mosaic {normal_mosaic_id})",
            "State": "Total Batches",
            "Count": normal_mosaic.total_batches,
            "Percentage": "100.0%"
        },
        {
            "Mosaic": f"Normal (Mosaic {normal_mosaic_id})",
            "State": "Processed",
            "Count": normal_mosaic.processed_batches,
            "Percentage": f"{normal_progress:.1f}%"
        },
        {
            "Mosaic": f"Tilted (Mosaic {tilted_mosaic_id})",
            "State": "Total Batches",
            "Count": tilted_mosaic.total_batches,
            "Percentage": "100.0%"
        },
        {
            "Mosaic": f"Tilted (Mosaic {tilted_mosaic_id})",
            "State": "Processed",
            "Count": tilted_mosaic.processed_batches,
            "Percentage": f"{tilted_progress:.1f}%"
        }
    ]

    normal_status = "✅ COMPLETE" if normal_mosaic.is_complete() else "⏳ IN PROGRESS"
    tilted_status = "✅ COMPLETE" if tilted_mosaic.is_complete() else "⏳ IN PROGRESS"
    overall_status = "✅ READY FOR REGISTRATION - Both mosaics complete" if slice_state_obj.both_mosaics_complete() else "⏳ WAITING - Processing mosaics..."
    
    description = f"""Slice {slice_number} Processing Progress

Project: {project_name}
Slice Number: {slice_number}
Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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


@task(name="check_mosaic_stitched")
def check_mosaic_stitched_task(
    project_base_path: str,
    mosaic_id: int,
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
    
    # Recover state from flag files
    # Note: project_name not available in this task, will use fallback method
    mosaic_state = MosaicState(project_base_path, mosaic_id, project_name=None)
    
    # Check flag file first (authoritative source)
    if mosaic_state.stitched:
        logger.info(f"Mosaic {mosaic_id} is stitched (flag file exists)")
        return True
    
    # Fallback: Check for AIP file as indicator of stitching completion
    _, stitched_path, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    aip_file = stitched_path / f"mosaic_{mosaic_id:03d}_aip.nii"
    is_stitched = aip_file.exists()

    if is_stitched:
        logger.info(f"Mosaic {mosaic_id} is stitched (found {aip_file})")
    else:
        logger.debug(f"Mosaic {mosaic_id} not yet stitched (missing flag file and {aip_file})")

    return is_stitched


@task(name="refresh_and_save_mosaic_state")
def refresh_and_save_mosaic_state_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
) -> bool:
    """
    Refresh mosaic state from flag files and save to project state variable.
    
    This task should be called after state changes to update the cached
    state in the project state variable ({project_name}.state) for faster
    retrieval in subsequent flow runs.
    
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
        True if state was successfully saved to project state variable
    """
    logger = get_run_logger()
    logger.info(f"Refreshing and saving state for mosaic {mosaic_id}")
    
    # Load state from flag files (authoritative source)
    mosaic_state = MosaicState(project_base_path, mosaic_id, project_name)
    mosaic_state.refresh()
    
    # Load project state and update the mosaic
    project_state = ProjectState(project_base_path, project_name, use_variable=True)
    
    # Get the slice containing this mosaic
    from workflow.tasks.utils import mosaic_id_to_slice_number
    slice_number = mosaic_id_to_slice_number(mosaic_id)
    slice_state = project_state.get_slice_state(slice_number)
    
    if slice_state is None:
        # Create new slice state if it doesn't exist
        slice_state = SliceState(project_base_path, slice_number, project_name)
        project_state.slices[slice_number] = slice_state
        project_state.slice_numbers = sorted(project_state.slices.keys())
    
    # Update the appropriate mosaic
    if mosaic_id == slice_state.normal_mosaic_id:
        target_mosaic = slice_state.normal_mosaic
    elif mosaic_id == slice_state.tilted_mosaic_id:
        target_mosaic = slice_state.tilted_mosaic
    else:
        logger.warning(f"Mosaic {mosaic_id} doesn't match expected pattern for slice {slice_number}")
        return False
    
    # Copy all state attributes from refreshed mosaic to target mosaic
    target_mosaic.started_batches = mosaic_state.started_batches
    target_mosaic.archived_batches = mosaic_state.archived_batches
    target_mosaic.processed_batches = mosaic_state.processed_batches
    target_mosaic.uploaded_batches = mosaic_state.uploaded_batches
    target_mosaic.total_batches = mosaic_state.total_batches
    target_mosaic.batch_states = mosaic_state.batch_states
    target_mosaic.started = mosaic_state.started
    target_mosaic.stitched = mosaic_state.stitched
    target_mosaic.volume_stitched = mosaic_state.volume_stitched
    target_mosaic.volume_uploaded = mosaic_state.volume_uploaded
    
    # Save updated project state to variable
    success = project_state.save_to_variable()
    
    if success:
        logger.info(f"Successfully saved mosaic {mosaic_id} state to project state variable")
    else:
        logger.warning(f"Failed to save mosaic {mosaic_id} state to project state variable")
    
    return success


@task(name="refresh_and_save_slice_state")
def refresh_and_save_slice_state_task(
    project_name: str,
    project_base_path: str,
    slice_number: int,
) -> bool:
    """
    Refresh slice state from flag files and save to project state variable.
    
    This task should be called after state changes to update the cached
    state in the project state variable ({project_name}.state) for faster
    retrieval in subsequent flow runs.
    
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
    bool
        True if state was successfully saved to project state variable
    """
    logger = get_run_logger()
    logger.info(f"Refreshing and saving state for slice {slice_number}")
    
    # Load state from flag files (authoritative source)
    slice_state = SliceState(project_base_path, slice_number, project_name)
    slice_state.refresh()
    
    # Load project state and update the slice
    project_state = ProjectState(project_base_path, project_name, use_variable=True)
    project_state.slices[slice_number] = slice_state
    
    # Update slice_numbers if needed
    if slice_number not in project_state.slice_numbers:
        project_state.slice_numbers = sorted(project_state.slices.keys())
    
    # Save updated project state to variable
    success = project_state.save_to_variable()
    
    if success:
        logger.info(f"Successfully saved slice {slice_number} state to project state variable")
    else:
        logger.warning(f"Failed to save slice {slice_number} state to project state variable")
    
    return success

