


import logging
import os
import os.path as op
from pathlib import Path
import subprocess
from typing import List

from prefect import flow, task
import prefect
from prefect.blocks.core import Block
from prefect.events import DeploymentEventTrigger, emit_event


logger = logging.getLogger(__name__)
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from workflow.tasks.tile_processing import archive_tile_task

class Cube(Block):
    edge_length_inches: float

@task(name="spectral_to_complex_batch_task")
def spectral_to_complex_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],):
    return
    args = []
    disp_comp_file = '/autofs/cluster/octdata2/users/Hui/tools/dg_utils/spectralprocess/dispComp/mineraloil_LSM03/dispersion_compensation_LSM03_mineraloil_20240829/LSM03_mineral_oil_placecorrectionmeanall2.dat'
    aline_length = 200 if mosaic_id % 2 == 0 else 350
    bline_length = 350
    for file in file_list:
        tile_index = int(os.path.basename(file).split("_")[3])
        output_path = Path(project_base_path) / f"mosaic-{mosaic_id:03d}" / "complex" / f"mosaic-{mosaic_id:03d}_image_{tile_index:04d}_complex.nii"
        args.append(f"{file} {output_path} {mosaic_id} {disp_comp_file} {aline_length} {bline_length}")
    result = subprocess.run(["matlab", "-batch", "addpath(genpath('/homes/5/kc1708/localhome/code/psoct-renew/'));s2c_batch_stdin"], 
    input="\n".join(args), capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error converting spectral to complex: {result.stderr}")
        raise ValueError(f"Error converting spectral to complex: {result.stderr}")
    emit_event(
        event="tile_batch.complex2processed.ready",
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
        }
    )
    return result.stdout

@task(name="complex_to_processed_batch_task")
def complex_to_processed_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int):
    pass

@task(name="archive_tile_batch_task")
def archive_tile_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
    compressed_base_path: str = None):
    """
    Archive tiles in a batch one by one.
    
    Parameters
    ----------
    project_name : str
        Project name
    project_base_path : str
        Base path for the project
    mosaic_id : int
        Mosaic identifier
    batch_id : int
        Batch identifier
    file_list : List[str]
        List of file paths to archive
    compressed_base_path : str, optional
        Base path for compressed files. If None, uses project_base_path.
    """
    if compressed_base_path is None:
        compressed_base_path = str(Path(project_base_path) / "archived")
    
    # Ensure compressed_base_path directory exists
    os.makedirs(compressed_base_path, exist_ok=True)
    
    # Determine acquisition type based on mosaic_id
    titled_illumination = mosaic_id % 2 == 0
    acq = "tilted" if titled_illumination else "normal"
    slice_id = (mosaic_id + 1) // 2
    
    # Store list of archived file paths for upload
    archived_file_paths = []
    
    # Process each file one by one
    for file_path in file_list:
        # Extract tile_index from filename (similar to spectral_to_complex_batch_task)
        tile_index = int(os.path.basename(file_path).split("_")[3])
        
        # Generate archived tile name (same pattern as tile_flow.py)
        archived_tile_name = f"{project_name}_sample-slice-{slice_id:03d}_chunk-{tile_index:04d}_acq-{acq}_OCT.nii.gz"
        archived_tile_path = op.join(compressed_base_path, archived_tile_name)
        
        # Archive the tile (synchronous, one by one)
        logger.info(f"Archiving tile {tile_index:04d} from batch {batch_id} in mosaic {mosaic_id:03d}")
        
        # archive_tile_task(file_path, archived_tile_path)
        
        # Store the archived file path
        archived_file_paths.append(archived_tile_path)
        break # TODO: Remove this after testing
        
    
    logger.info(f"Completed archiving {len(file_list)} tiles for batch {batch_id} in mosaic {mosaic_id}")
    emit_event(
        event="tile_batch.upload_to_linc.ready",
        resource={
            "prefect.resource.id": "{{ event.resource.id }}",
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "batch_id": batch_id,
            "archived_file_paths": archived_file_paths,
        }
    )
    return f"Archived {len(file_list)} tiles"



@flow(name="process_tile_batch_flow")
def process_tile_batch_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str]):
    
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id:03d}" 
    mosaic_path.mkdir(parents=True, exist_ok=True)
    batch_started_path = mosaic_path / "state" / f"batch-{batch_id}.started"
    (mosaic_path / "state").mkdir(parents=True, exist_ok=True)
    # if batch_started_path.exists():
    #     logger.info(f"Batch {batch_id} already started")
    #     return
    batch_started_path.touch()
    
    # Run archive and spectral2complex in parallel
    archive_future = archive_tile_batch_task.submit(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        file_list=file_list
    )
    spectral_to_complex_future = spectral_to_complex_batch_task.submit(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        file_list=file_list
    )
    
    # Wait for both to complete
    archive_result = archive_future.wait()
    spectral_to_complex_result = spectral_to_complex_future.wait()
    
    # Mark batch as archived and converted
    batch_archived_path = mosaic_path / "state" / f"batch-{batch_id}.archived"
    batch_archived_path.touch()
    # batch_complex_path = mosaic_path / "state" / f"batch-{batch_id}.complex"
    # batch_complex_path.touch()
    
    # Emit archived event to trigger state management flow
    emit_event(
        event="tile_batch.archived.ready",
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
        }
    )
    
    logger.info(
        f"Emitted events for batch {batch_id} in mosaic {mosaic_id}. "
        f"Waiting for complex2processed and upload_to_linc flows to be triggered."
    )
    
    return {
        "archive_result": archive_result,
        "spectral_to_complex_result": spectral_to_complex_result,
    }


@flow(name="complex_to_processed_batch_event_flow")
def complex_to_processed_batch_event_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
):
    """
    Event-driven flow triggered by 'tile_batch.complex2processed.ready' event.
    Runs complex_to_processed_batch_task and checks if all batches are processed.
    """
    mosaic_path = Path(project_base_path) / f"mosaic-{mosaic_id:03d}"
    state_path = mosaic_path / "state"
    state_path.mkdir(parents=True, exist_ok=True)
    
    batch_processed_path = state_path / f"batch-{batch_id}.processed"
    if batch_processed_path.exists():
        logger.info(f"Batch {batch_id} already processed")
    else:
        # Run complex_to_processed_batch_task
        complex_to_processed_batch_task(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id
        )
        
        # Mark batch as processed
        batch_processed_path.touch()
        logger.info(f"Batch {batch_id} processed successfully")
    
    # Check if all batches in the mosaic are processed
    # Find all batch started files to determine total batches
    batch_started_files = list(state_path.glob("batch-*.started"))
    total_batches = len(batch_started_files)
    
    if total_batches == 0:
        logger.warning(f"No batch files found for mosaic {mosaic_id}")
        return
    
    # Count processed batches
    batch_processed_files = list(state_path.glob("batch-*.processed"))
    processed_count = len(batch_processed_files)
    
    logger.info(
        f"Mosaic {mosaic_id}: {processed_count}/{total_batches} batches processed"
    )
    
    # If all batches are processed, emit mosaic_processed event
    if processed_count >= total_batches:
        logger.info(
            f"All batches processed for mosaic {mosaic_id}. "
            f"Emitting mosaic_processed event."
        )
        emit_event(
            event="mosaic.processed",
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
            }
        )
    
    return {
        "mosaic_id": mosaic_id,
        "batch_id": batch_id,
        "processed_count": processed_count,
        "total_batches": total_batches,
        "all_processed": processed_count >= total_batches,
    }


if __name__ == "__main__":
    from workflow.flows.upload_flow import upload_to_linc_batch_flow, upload_to_linc_batch_event_flow
    
    process_tile_batch_flow_deployment = process_tile_batch_flow.to_deployment(
        name="process_tile_batch_flow"
        )
    upload_to_linc_batch_flow_deployment = upload_to_linc_batch_flow.to_deployment(
        name="upload_to_linc_batch_flow",
    )
    upload_to_linc_batch_event_flow_deployment = upload_to_linc_batch_event_flow.to_deployment(
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
    )
    prefect.serve(process_tile_batch_flow_deployment, upload_to_linc_batch_flow_deployment, upload_to_linc_batch_event_flow_deployment)
# Note: After complex2processed flow completes (triggered by event), 
# it checks if all batches in the mosaic are processed.
# If all batches are processed, it emits the mosaic_processed event,
# which will then trigger the mosaic level processing flow.