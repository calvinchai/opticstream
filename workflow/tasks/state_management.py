"""
Tasks for managing processing state and updating Prefect Artifacts.

These tasks check flag files and update Artifacts to track progress
at different levels: batch, mosaic, and slice.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime

from prefect import task
from prefect.artifacts import create_markdown_artifact

logger = logging.getLogger(__name__)


@task(name="check_batch_state")
def check_batch_state_task(
    project_base_path: str,
    mosaic_id: int,
) -> Dict[str, int]:
    """
    Check batch state by scanning flag files.
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
        
    Returns
    -------
    Dict[str, int]
        Dictionary with counts for each batch state:
        - total_batches: Total number of batches (from .started files)
        - started_batches: Number of batches started
        - archived_batches: Number of batches archived
        - processed_batches: Number of batches processed
        - uploaded_batches: Number of batches uploaded
    """
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id:03d}"
    state_path = mosaic_path / "state"
    
    if not state_path.exists():
        logger.warning(f"State path does not exist: {state_path}")
        return {
            "total_batches": 0,
            "started_batches": 0,
            "archived_batches": 0,
            "processed_batches": 0,
            "uploaded_batches": 0,
        }
    
    # Count flag files
    started_files = list(state_path.glob("batch-*.started"))
    archived_files = list(state_path.glob("batch-*.archived"))
    processed_files = list(state_path.glob("batch-*.processed"))
    uploaded_files = list(state_path.glob("batch-*.uploaded"))
    
    total_batches = len(started_files)
    
    return {
        "total_batches": total_batches,
        "started_batches": len(started_files),
        "archived_batches": len(archived_files),
        "processed_batches": len(processed_files),
        "uploaded_batches": len(uploaded_files),
    }


@task(name="update_mosaic_artifact")
def update_mosaic_artifact_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_state: Dict[str, int],
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
    batch_state : Dict[str, int]
        Batch state dictionary from check_batch_state_task
        
    Returns
    -------
    str
        Artifact key
    """
    total_batches = batch_state["total_batches"]
    processed_batches = batch_state["processed_batches"]
    archived_batches = batch_state["archived_batches"]
    uploaded_batches = batch_state["uploaded_batches"]
    
    if total_batches == 0:
        progress_percentage = 0.0
    else:
        progress_percentage = (processed_batches / total_batches) * 100
    
    # Create markdown artifact with progress information
    artifact_key = f"{project_name}_mosaic_{mosaic_id}_progress"
    
    markdown_content = f"""# Mosaic {mosaic_id} Processing Progress

**Project**: {project_name}  
**Mosaic ID**: {mosaic_id}  
**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Batch Progress

| State | Count | Percentage |
|-------|-------|------------|
| **Total Batches** | {total_batches} | 100% |
| Started | {batch_state['started_batches']} | {(batch_state['started_batches']/total_batches*100) if total_batches > 0 else 0:.1f}% |
| Archived | {archived_batches} | {(archived_batches/total_batches*100) if total_batches > 0 else 0:.1f}% |
| **Processed** | **{processed_batches}** | **{progress_percentage:.1f}%** |
| Uploaded | {uploaded_batches} | {(uploaded_batches/total_batches*100) if total_batches > 0 else 0:.1f}% |

## Status

{"✅ **COMPLETE** - All batches processed" if processed_batches >= total_batches and total_batches > 0 else "⏳ **IN PROGRESS** - Processing batches..."}

## Milestones

- {"✅" if progress_percentage >= 25 else "⏳"} 25% Complete
- {"✅" if progress_percentage >= 50 else "⏳"} 50% Complete  
- {"✅" if progress_percentage >= 75 else "⏳"} 75% Complete
- {"✅" if progress_percentage >= 100 else "⏳"} 100% Complete
"""
    
    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_content,
        description=f"Processing progress for mosaic {mosaic_id}",
    )
    
    logger.info(
        f"Updated artifact {artifact_key}: {processed_batches}/{total_batches} batches processed ({progress_percentage:.1f}%)"
    )
    
    return artifact_key


@task(name="check_mosaic_completion")
def check_mosaic_completion_task(
    project_base_path: str,
    mosaic_id: int,
    batch_state: Dict[str, int],
) -> bool:
    """
    Check if all batches in a mosaic are processed.
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_state : Dict[str, int]
        Batch state dictionary from check_batch_state_task
        
    Returns
    -------
    bool
        True if all batches are processed, False otherwise
    """
    total_batches = batch_state["total_batches"]
    processed_batches = batch_state["processed_batches"]
    
    if total_batches == 0:
        return False
    
    is_complete = processed_batches >= total_batches
    
    if is_complete:
        logger.info(f"Mosaic {mosaic_id}: All {processed_batches}/{total_batches} batches processed")
    else:
        logger.debug(f"Mosaic {mosaic_id}: {processed_batches}/{total_batches} batches processed")
    
    return is_complete


@task(name="check_slice_state")
def check_slice_state_task(
    project_base_path: str,
    slice_number: int,
) -> Dict[str, any]:
    """
    Check state of both mosaics in a slice.
    
    Slice n has mosaics 2n-1 (normal) and 2n (tilted).
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
        
    Returns
    -------
    Dict[str, any]
        Dictionary with:
        - normal_mosaic_id: Normal mosaic ID (2n-1)
        - tilted_mosaic_id: Tilted mosaic ID (2n)
        - normal_mosaic_state: Batch state for normal mosaic
        - tilted_mosaic_state: Batch state for tilted mosaic
        - normal_complete: True if normal mosaic is complete
        - tilted_complete: True if tilted mosaic is complete
        - both_complete: True if both mosaics are complete
    """
    normal_mosaic_id = 2 * slice_number - 1
    tilted_mosaic_id = 2 * slice_number
    
    # Call tasks directly (Prefect handles execution)
    normal_state = check_batch_state_task(
        project_base_path=project_base_path,
        mosaic_id=normal_mosaic_id,
    )
    
    tilted_state = check_batch_state_task(
        project_base_path=project_base_path,
        mosaic_id=tilted_mosaic_id,
    )
    
    # Check completion status
    normal_complete = check_mosaic_completion_task(
        project_base_path=project_base_path,
        mosaic_id=normal_mosaic_id,
        batch_state=normal_state,
    )
    
    tilted_complete = check_mosaic_completion_task(
        project_base_path=project_base_path,
        mosaic_id=tilted_mosaic_id,
        batch_state=tilted_state,
    )
    
    return {
        "slice_number": slice_number,
        "normal_mosaic_id": normal_mosaic_id,
        "tilted_mosaic_id": tilted_mosaic_id,
        "normal_mosaic_state": normal_state,
        "tilted_mosaic_state": tilted_state,
        "normal_complete": normal_complete,
        "tilted_complete": tilted_complete,
        "both_complete": normal_complete and tilted_complete,
    }


@task(name="update_slice_artifact")
def update_slice_artifact_task(
    project_name: str,
    project_base_path: str,
    slice_number: int,
    slice_state: Dict[str, any],
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
    slice_state : Dict[str, any]
        Slice state dictionary from check_slice_state_task
        
    Returns
    -------
    str
        Artifact key
    """
    normal_mosaic_id = slice_state["normal_mosaic_id"]
    tilted_mosaic_id = slice_state["tilted_mosaic_id"]
    normal_state = slice_state["normal_mosaic_state"]
    tilted_state = slice_state["tilted_mosaic_state"]
    
    normal_progress = (
        (normal_state["processed_batches"] / normal_state["total_batches"] * 100)
        if normal_state["total_batches"] > 0
        else 0.0
    )
    tilted_progress = (
        (tilted_state["processed_batches"] / tilted_state["total_batches"] * 100)
        if tilted_state["total_batches"] > 0
        else 0.0
    )
    
    artifact_key = f"{project_name}_slice_{slice_number}_progress"
    
    markdown_content = f"""# Slice {slice_number} Processing Progress

**Project**: {project_name}  
**Slice Number**: {slice_number}  
**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Mosaic Status

### Normal Illumination (Mosaic {normal_mosaic_id})

| State | Count | Percentage |
|-------|-------|------------|
| Total Batches | {normal_state['total_batches']} | 100% |
| Processed | {normal_state['processed_batches']} | {normal_progress:.1f}% |

**Status**: {"✅ COMPLETE" if slice_state['normal_complete'] else "⏳ IN PROGRESS"}

### Tilted Illumination (Mosaic {tilted_mosaic_id})

| State | Count | Percentage |
|-------|-------|------------|
| Total Batches | {tilted_state['total_batches']} | 100% |
| Processed | {tilted_state['processed_batches']} | {tilted_progress:.1f}% |

**Status**: {"✅ COMPLETE" if slice_state['tilted_complete'] else "⏳ IN PROGRESS"}

## Overall Slice Status

{"✅ **READY FOR REGISTRATION** - Both mosaics complete" if slice_state['both_complete'] else "⏳ **WAITING** - Processing mosaics..."}
"""
    
    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_content,
        description=f"Processing progress for slice {slice_number}",
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
    Check if mosaic has been stitched by looking for stitched output files.
    
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
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id:03d}"
    stitched_path = mosaic_path / "stitched"
    
    # Check for AIP file as indicator of stitching completion
    aip_file = stitched_path / f"mosaic_{mosaic_id:03d}_aip.nii"
    
    is_stitched = aip_file.exists()
    
    if is_stitched:
        logger.info(f"Mosaic {mosaic_id} is stitched (found {aip_file})")
    else:
        logger.debug(f"Mosaic {mosaic_id} not yet stitched (missing {aip_file})")
    
    return is_stitched

