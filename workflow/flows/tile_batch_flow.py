import os
import os.path as op
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import prefect
from prefect import flow, task
from prefect.events import emit_event
from prefect.logging import get_run_logger

from workflow.config.constants import TileSavingType
from workflow.config.project_config import get_project_config_block
from workflow.events import (
    BATCH_ARCHIVED,
    BATCH_READY,
    BATCH_PROCESSED,
    BATCH_COMPLEXED,
    get_event_trigger,
)
from workflow.state.flags import (
    ARCHIVED,
    PROCESSED,
    STARTED,
    get_batch_flag_path,
    get_batch_flag_path_from_project,
)
from workflow.tasks.utils import get_mosaic_paths
import re

from workflow.tasks.tile_processing import archive_tile_task


@task(
    task_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-spectral-to-complex"
)
def spectral_to_complex_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
    aline_length: int = 200,
    bline_length: int = 350,
):
    logger = get_run_logger()

    # Use slice-based structure
    _, _, complex_path, _ = get_mosaic_paths(project_base_path, mosaic_id)
    complex_path.mkdir(parents=True, exist_ok=True)
    file_list_str = ",".join(f"'{file}'" for file in file_list)
    file_list_str = f"{{{file_list_str}}}"
    spectral2complex_batch_cmd = f"spectral2complex_batch({file_list_str}, '{complex_path}/', {aline_length}, {bline_length})"
    print(f"Spectral to complex batch command: {spectral2complex_batch_cmd}")
    result = subprocess.run(
        [
            "matlab",
            "-batch",
            "addpath(genpath('/homes/5/kc1708/localhome/code/psoct-renew/'));"
            + spectral2complex_batch_cmd,
        ],
    )
    if result.returncode != 0:
        logger.error(f"Error converting spectral to complex: {result.stderr}")
        raise ValueError(f"Error converting spectral to complex: {result.stderr}")
    complex_file_list = []
    # mosaic_001_image_0000_spectral_0000.nii -> mosaic_001_image_0000_complex.nii also second number alway padding to 4 digits
    for spectral_file in file_list:
        base_name = op.basename(spectral_file)
        # Replace anything starting with 'spectral' before the extension with 'complex'
        # This corresponds to: regexprep(base_name, 'spectral.*$', 'complex')
        if "spectral" in base_name:
            # Find start of 'spectral', remove everything from there to extension, append 'complex'
            idx = base_name.find("spectral")
            name_no_ext = base_name[:idx] + "complex"
            ext = op.splitext(base_name)[1]
            new_base = name_no_ext + ext
        else:
            new_base = base_name

        # Extract name before extension for further regex
        name_no_ext = op.splitext(new_base)[0]

        # Attempt to match '^mosaic_(\d{3})_image_(\d{3})_complex$'
        match = re.match(r"^mosaic_(\d{3})_image_(\d{3,4})_complex$", name_no_ext)
        if match:
            mosaic_str = match.group(1)
            image_idx = int(match.group(2))
            image_str = f"{image_idx:04d}"
            # Reconstruct name with 4-digit image index and original extension
            name_no_ext = f"mosaic_{mosaic_str}_image_{image_str}_complex"
            new_base = name_no_ext + op.splitext(new_base)[1]
        complex_file_list.append(str(Path(complex_path) / new_base))


    emit_event(
        event=BATCH_COMPLEXED,
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
        },
    )
    return result.stdout


@task(
    task_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-complex-to-complex"
)
def complex_to_complex_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
):
    """
    Link the complex data to the raw data.
    """
    get_run_logger()
    _, _, complex_path, _ = get_mosaic_paths(project_base_path, mosaic_id)
    complex_path.mkdir(parents=True, exist_ok=True)
    complex_file_list = []
    for complex_file in file_list:
        name = op.basename(complex_file)
        name_no_ext = op.splitext(name)[0]
        match = re.match(r"^mosaic_(\d{3})_image_(\d{3,4}).*$", name_no_ext)
        if not match:
            raise ValueError(f"Invalid complex file name: {complex_file}")
        mosaic_str = match.group(1)
        image_idx = int(match.group(2))
        image_str = f"{image_idx:04d}"
        raw_file = f"mosaic_{mosaic_str}_image_{image_str}_complex.nii"
        raw_file_path = str(Path(complex_path) / raw_file)
        os.symlink(complex_file, raw_file_path)
        complex_file_list.append(raw_file_path)

    emit_event(
        event=BATCH_COMPLEXED,
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
        },
    )
    return complex_file_list


@task(
    task_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-complex-to-processed"
)
def complex_to_processed_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
):
    logger = get_run_logger()
    processed_path, _, _, _ = get_mosaic_paths(project_base_path, mosaic_id)
    processed_path.mkdir(parents=True, exist_ok=True)
    file_list_str = []
    for file in file_list:
        file_list_str.append(f"'{file.replace('spectral', 'complex')}'")
    file_list_str = f"{{{','.join(file_list_str)}}}"
    complex2processed_batch_cmd = (
        f"complex2processed_batch({file_list_str}, '{processed_path}/', \"find\", 80 )"
    )
    print(complex2processed_batch_cmd)
    result = subprocess.run(
        [
            "matlab",
            "-batch",
            "addpath(genpath('/homes/5/kc1708/localhome/code/psoct-renew/'));"
            + complex2processed_batch_cmd,
        ],
    )
    if result.returncode != 0:
        logger.error(
            f"Error converting complex to processed: {result.stderr} {complex2processed_batch_cmd}"
        )
        raise ValueError(
            f"Error converting complex to processed: {result.stderr} {complex2processed_batch_cmd}"
        )
    return result.stdout


@task(
    task_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-archive"
)
def archive_tile_batch_task(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
    compressed_base_path: str = None,
):
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
    logger = get_run_logger()
    if compressed_base_path is None:
        compressed_base_path = str(Path(project_base_path) / "archived")

    # Ensure compressed_base_path directory exists
    os.makedirs(compressed_base_path, exist_ok=True)

    # Determine acquisition type based on mosaic_id
    titled_illumination = mosaic_id % 2 == 0
    acq = "tilted" if titled_illumination else "normal"
    slice_id = (mosaic_id + 1) // 2
    batch_archived_path = get_batch_flag_path_from_project(
        project_base_path, mosaic_id, batch_id, ARCHIVED
    )
    if batch_archived_path.exists():
        logger.info(f"Batch {batch_id} already archived")
        return f"Batch {batch_id} already archived"
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
        logger.info(
            f"Archiving tile {tile_index:04d} from batch {batch_id} in mosaic {mosaic_id:03d}"
        )

        archive_future.append(archive_tile_task.submit(file_path, archived_tile_path))

        # Store the archived file path
        archived_file_paths.append(archived_tile_path)

    for future in archive_future:
        future.wait()

    logger.info(
        f"Completed archiving {len(file_list)} tiles for batch {batch_id} in mosaic {mosaic_id}"
    )
    
    batch_archived_path.touch()
    emit_event(
        event=BATCH_ARCHIVED,
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
            "archived_file_paths": archived_file_paths,
        },
    )
    return f"Archived {len(file_list)} tiles"


@flow(flow_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}")
def process_tile_batch_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
    archive: bool = True,
    convert: bool = True,
    tile_saving_type: TileSavingType = TileSavingType.SPECTRAL,
):
    """
    Process a batch of tiles.
    For spectral data, the flow will convert the spectral data to complex data and archive the tiles.
    For complex data, the flow will link the complex data to the raw data and archive the tiles.
    Once the complex data is ready, the flow will convert the complex data to processed data.

    """
    logger = get_run_logger()
    logger.info(f"Processing batch {batch_id} in mosaic {mosaic_id}")

    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    state_path.mkdir(parents=True, exist_ok=True)
    batch_started_path = get_batch_flag_path(state_path, batch_id, STARTED)
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
            file_list=file_list,
        )

    if convert:
        if tile_saving_type == TileSavingType.SPECTRAL:
            project_config = get_project_config_block(project_name)
            is_tilted = mosaic_id % 2 == 0

            # Handle tile_size values with defaults
            if project_config is None:
                from workflow.config.blocks import PSOCTScanConfig

                # Helper to get field default (supports both Pydantic v1 and v2)
                def _get_field_default(model_class, field_name: str):
                    if (
                        hasattr(model_class, "model_fields")
                        and field_name in model_class.model_fields
                    ):
                        field_info = model_class.model_fields[field_name]
                        if hasattr(field_info, "default"):
                            return field_info.default
                    if (
                        hasattr(model_class, "__fields__")
                        and field_name in model_class.__fields__
                    ):
                        field_info = model_class.__fields__[field_name]
                        if hasattr(field_info, "default"):
                            return field_info.default
                    return None

                tile_size_x_tilted = (
                    _get_field_default(PSOCTScanConfig, "tile_size_x_tilted") or 200
                )
                tile_size_x_normal = (
                    _get_field_default(PSOCTScanConfig, "tile_size_x_normal") or 350
                )
                tile_size_y = _get_field_default(PSOCTScanConfig, "tile_size_y") or 350

                aline_length = tile_size_x_tilted if is_tilted else tile_size_x_normal
                bline_length = tile_size_y
                logger.warning(
                    f"Using default tile_size values (aline_length={aline_length}, "
                    f"bline_length={bline_length}) from class defaults (config block not found)"
                )
            else:
                aline_length = (
                    project_config.tile_size_x_tilted
                    if is_tilted
                    else project_config.tile_size_x_normal
                )
                bline_length = project_config.tile_size_y

            spectral_to_complex_future = spectral_to_complex_batch_task.submit(
                project_name=project_name,
                project_base_path=project_base_path,
                mosaic_id=mosaic_id,
                batch_id=batch_id,
                file_list=file_list,
                aline_length=aline_length,
                bline_length=bline_length,
            )
        elif tile_saving_type == TileSavingType.COMPLEX:
            spectral_to_complex_future = complex_to_complex_batch_task.submit(
                project_name=project_name,
                project_base_path=project_base_path,
                mosaic_id=mosaic_id,
                batch_id=batch_id,
                file_list=file_list,
            )
    # Wait for both to complete (if they were started)
    archive_result = None
    spectral_to_complex_result = None

    if archive_future is not None:
        archive_result = archive_future.wait()

    if spectral_to_complex_future is not None:
        spectral_to_complex_result = spectral_to_complex_future.wait()
    
    return {
        "archive_result": archive_result,
        "spectral_to_complex_result": spectral_to_complex_result,
    }


@flow
def process_tile_batch_event_flow(payload: Dict[str, Any]):
    """
    Wrapper flow for event-driven triggering.
    Loads config block and provides defaults using priority: payload → block → class defaults.
    """
    from prefect.logging import get_run_logger

    logger = get_run_logger()

    project_name = payload["project_name"]

    # Load config block
    project_config = get_project_config_block(project_name)
    if project_config is None:
        logger.warning(
            f"Project config block for '{project_name}' not found. "
            f"Using defaults from payload or class defaults."
        )

    # Handle tile_saving_type (payload → block → class default)
    tile_saving_type = payload.get("tile_saving_type")
    if tile_saving_type is None:
        if project_config:
            tile_saving_type = project_config.tile_saving_type
        else:
            tile_saving_type = TileSavingType.SPECTRAL
    else:
        # Convert string to enum if needed
        if isinstance(tile_saving_type, str):
            tile_saving_type = TileSavingType[tile_saving_type.upper()]

    process_tile_batch_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        batch_id=payload["batch_id"],
        file_list=payload["file_list"],
        archive=payload.get("archive", True),
        convert=payload.get("convert", True),
        tile_saving_type=tile_saving_type,
    )


@flow(
    flow_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-complex-to-processed"
)
def complex_to_processed_batch_flow(
    project_name: str,
    project_base_path: str,
    mosaic_id: int,
    batch_id: int,
    file_list: List[str],
):
    """
    Event-driven flow triggered by event.
    Runs complex_to_processed_batch_task and checks if all batches are processed.
    """
    logger = get_run_logger()
    # Use slice-based structure
    _, _, _, state_path = get_mosaic_paths(project_base_path, mosaic_id)
    state_path.mkdir(parents=True, exist_ok=True)

    batch_processed_path = get_batch_flag_path(state_path, batch_id, PROCESSED)
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
        event=BATCH_PROCESSED,
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
        },
    )
    return True


@flow
def complex_to_processed_batch_event_flow(payload: Dict[str, Any]):
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
    from workflow.flows.upload_flow import (
        upload_to_linc_batch_flow,
        upload_to_linc_batch_event_flow,
    )

    process_tile_batch_flow_deployment = process_tile_batch_flow.to_deployment(
        name="process_tile_batch_flow"
    )

    # Deployment for complex-to-processed batch flow (triggered by BATCH_READY event)
    complex_to_processed_batch_event_flow_deployment = (
        complex_to_processed_batch_event_flow.to_deployment(
            name="complex_to_processed_batch_event_flow",
            tags=["event-driven", "tile-batch", "complex-to-processed"],
            triggers=[
                get_event_trigger(BATCH_READY),
            ],
        )
    )

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
        process_tile_batch_flow_deployment,
        complex_to_processed_batch_event_flow_deployment,
        upload_to_linc_batch_flow_deployment,
        upload_to_linc_batch_event_flow_deployment,
    )
# Note: After complex2processed flow completes (triggered by event),
# it checks if all batches in the mosaic are processed.
# If all batches are processed, it emits the mosaic_processed event,
# which will then trigger the mosaic level processing flow.
