


import logging
import os
import os.path as op
from pathlib import Path
import subprocess
from typing import Any, Dict, List

from prefect import flow, task
import prefect
from prefect.blocks.core import Block
from prefect.events import DeploymentEventTrigger, emit_event

from workflow.tasks.utils import mosaic_id_to_slice_number, get_mosaic_paths


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
    args = []
    disp_comp_file = '/autofs/cluster/octdata2/users/Hui/tools/dg_utils/spectralprocess/dispComp/mineraloil_LSM03/dispersion_compensation_LSM03_mineraloil_20240829/LSM03_mineral_oil_placecorrectionmeanall2.dat'
    aline_length = 200 if mosaic_id % 2 == 0 else 350
    bline_length = 350
    # Use slice-based structure
    _, _, complex_path, _ = get_mosaic_paths(project_base_path, mosaic_id)
    complex_path.mkdir(parents=True, exist_ok=True)
    file_list_str = ",".join(f"'{file}'" for file in file_list) 
    file_list_str = f"{{{file_list_str}}}"
    spectral2complex_batch_cmd = f"spectral2complex_batch({file_list_str}, '{complex_path}/', {aline_length}, {bline_length})"
    print(f"Spectral to complex batch command: {spectral2complex_batch_cmd}")
    result = subprocess.run(["matlab", "-batch", "addpath(genpath('/homes/5/kc1708/localhome/code/psoct-renew/'));" + spectral2complex_batch_cmd],)
    if result.returncode != 0:
        logger.error(f"Error converting spectral to complex: {result.stderr}")
        raise ValueError(f"Error converting spectral to complex: {result.stderr}")
    complex_file_list = []
    for spectral_file in file_list:
        complex_filename = op.basename(spectral_file)
        # mosaic_001_image_0000_spectral_0000.nii -> mosaic_001_image_0000_complex.nii also second number alway padding to 4 digits
    for spectral_file in file_list:
        base_name = op.basename(spectral_file)
        # Replace anything starting with 'spectral' before the extension with 'complex'
        # This corresponds to: regexprep(base_name, 'spectral.*$', 'complex')
        if 'spectral' in base_name:
            # Find start of 'spectral', remove everything from there to extension, append 'complex'
            idx = base_name.find('spectral')
            name_no_ext = base_name[:idx] + 'complex'
            ext = op.splitext(base_name)[1]
            new_base = name_no_ext + ext
        else:
            new_base = base_name

        # Extract name before extension for further regex
        name_no_ext = op.splitext(new_base)[0]

        # Attempt to match '^mosaic_(\d{3})_image_(\d{3})_complex$'
        import re
        match = re.match(r'^mosaic_(\d{3})_image_(\d{3})_complex$', name_no_ext)
        if match:
            mosaic_str = match.group(1)
            image_idx = int(match.group(2))
            image_str = f"{image_idx:04d}"
            # Reconstruct name with 4-digit image index and original extension
            name_no_ext = f"mosaic_{mosaic_str}_image_{image_str}_complex"
            new_base = name_no_ext + op.splitext(new_base)[1]
        complex_file_list.append(str(Path(complex_path) / new_base))

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
            "file_list": complex_file_list,
        }
    )
    return result.stdout

@task(name="complex_to_processed_batch_task")
def complex_to_processed_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],):
    
    processed_path, _, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    processed_path.mkdir(parents=True, exist_ok=True)
    file_list_str = []
    for file in file_list:
        file_list_str.append(f"'{file.replace('spectral', 'complex')}'")
    file_list_str = f"{{{','.join(file_list_str)}}}"
    complex2processed_batch_cmd = f"complex2processed_batch({file_list_str}, '{processed_path}/', \"find\", 80 )"
    print(complex2processed_batch_cmd)
    result = subprocess.run(["matlab", "-batch", "addpath(genpath('/homes/5/kc1708/localhome/code/psoct-renew/'));" + complex2processed_batch_cmd],)
    if result.returncode != 0:
        logger.error(f"Error converting complex to processed: {result.stderr} {complex2processed_batch_cmd}")
        raise ValueError(f"Error converting complex to processed: {result.stderr} {complex2processed_batch_cmd}")
    return result.stdout

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
    archive_future = []
    # Process each file one by one
    for file_path in file_list:
        # Extract tile_index from filename (similar to spectral_to_complex_batch_task)
        tile_index = int(os.path.basename(file_path).split("_")[3])
        
        # Generate archived tile name (same pattern as tile_flow.py)
        archived_tile_name = f"{project_name}_sample-slice-{slice_id:03d}_chunk-{tile_index:04d}_acq-{acq}_OCT.nii.gz"
        archived_tile_path = op.join(compressed_base_path, archived_tile_name)
        
        # Archive the tile (synchronous, one by one)
        logger.info(f"Archiving tile {tile_index:04d} from batch {batch_id} in mosaic {mosaic_id:03d}")
        
        archive_future.append(archive_tile_task.submit(file_path, archived_tile_path))
        
        # Store the archived file path
        archived_file_paths.append(archived_tile_path)

    for future in archive_future:
        future.wait()
    
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
    file_list: List[str],
    archive: bool = True,
    convert: bool = True):
    
    # Use slice-based structure
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    state_path.mkdir(parents=True, exist_ok=True)
    batch_started_path = state_path / f"batch-{batch_id}.started"
    # if batch_started_path.exists():
    #     logger.info(f"Batch {batch_id} already started")
    #     return
    batch_started_path.touch()
    
    # Run archive and spectral2complex in parallel (if enabled)
    archive_future = None
    spectral_to_complex_future = None
    
    if archive:
        archive_future = archive_tile_batch_task.submit(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            file_list=file_list
        )
    
    if convert:
        spectral_to_complex_future = spectral_to_complex_batch_task.submit(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            file_list=file_list
        )
    
    # Wait for both to complete (if they were started)
    archive_result = None
    spectral_to_complex_result = None
    
    if archive_future is not None:
        archive_result = archive_future.wait()
        # Mark batch as archived
        batch_archived_path = state_path / f"batch-{batch_id}.archived"
        batch_archived_path.touch()
        
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
    
    if spectral_to_complex_future is not None:
        spectral_to_complex_result = spectral_to_complex_future.wait()
    
    logger.info(
        f"Emitted events for batch {batch_id} in mosaic {mosaic_id}. "
        f"Waiting for complex2processed and upload_to_linc flows to be triggered."
    )
    
    return {
        "archive_result": archive_result,
        "spectral_to_complex_result": spectral_to_complex_result,
    }

@flow
def process_tile_batch_event_flow(
    payload: Dict[str, Any]):
    process_tile_batch_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        batch_id=payload["batch_id"],
        file_list=payload["file_list"],
        archive=payload.get("archive", True),
        convert=payload.get("convert", True),
    )

@flow(name="complex_to_processed_batch_flow")
def complex_to_processed_batch_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
):
    """
    Event-driven flow triggered by 'tile_batch.complex2processed.ready' event.
    Runs complex_to_processed_batch_task and checks if all batches are processed.
    """
    # Use slice-based structure
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    state_path.mkdir(parents=True, exist_ok=True)
    
    batch_processed_path = state_path / f"batch-{batch_id}.processed"
    # if batch_processed_path.exists():
    #     logger.info(f"Batch {batch_id} already processed")
    # else:
        # Run complex_to_processed_batch_task
    complex_to_processed_batch_task(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        file_list=file_list,
    )
    
    # Mark batch as processed
    batch_processed_path.touch()
    logger.info(f"Batch {batch_id} processed successfully")
    emit_event(
        event="tile_batch.processed.ready",
        resource={
            "prefect.resource.id": f"batch:{project_name}:mosaic-{mosaic_id}:batch-{batch_id}",
        },
        payload={
            "project_name": project_name,
            "project_base_path": project_base_path,
            "mosaic_id": mosaic_id,
            "batch_id": batch_id,
        }
    )
    return True

@flow
def complex_to_processed_batch_event_flow(
    payload: Dict[str, Any]):
    complex_to_processed_batch_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        batch_id=payload["batch_id"],
        file_list=payload["file_list"],
    )



# process_tile_batch_event_deployment = process_tile_batch_event_flow.to_deployment(
#     name="process_tile_batch_event_flow",
#     tags=["event-driven", "tile-batch", "process-tile-batch"],
#     triggers=[
#         DeploymentEventTrigger(
#             expect={"tile_batch.archived.ready"},
#         )
#     ]
# )

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