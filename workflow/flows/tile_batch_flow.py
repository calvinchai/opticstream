import os
import os.path as op
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
import re

from prefect import flow, task
from prefect.events import emit_event
from prefect.logging import get_run_logger

from workflow.config.constants import TileSavingType
from workflow.config.project_config import (
    get_project_config_block,
    resolve_config_param,
    resolve_tile_saving_type,
)
from workflow.events import (
    BATCH_ARCHIVED,
    BATCH_PROCESSED,
    BATCH_COMPLEXED,
)
from workflow.state.flags import (
    ARCHIVED,
    PROCESSED,
    STARTED,
    get_batch_flag_path,
    get_batch_flag_path_from_project,
)
from workflow.tasks.utils import get_mosaic_paths


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
    archive_tile_name_format: str,
    archive_path: str,
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
    archive_tile_name_format : str
        Format string for archived tile names. Should contain placeholders:
        {project_name}, {slice_id}, {tile_id}, {acq}
    archive_path : str
        Base path for archived files.
    """
    logger = get_run_logger()
    # Ensure archive_path directory exists
    os.makedirs(archive_path, exist_ok=True)

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
        # Generate archived tile name using format string
        archived_tile_name = archive_tile_name_format.format(
            project_name=project_name,
            slice_id=slice_id,
            tile_id=tile_index,  # Map tile_index to tile_id for format string
            acq=acq
        )
        archived_tile_path = op.join(archive_path, archived_tile_name)

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
    archive_tile_name_format: str = "{project_name}_sample-slice-{slice_id:02d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii.gz",
    archive_path: Optional[str] = None,
    tile_size_x_tilted: int = 200,
    tile_size_x_normal: int = 350,
    tile_size_y: int = 350,
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

    # Validate archive_path if archive is enabled
    if archive:
        if archive_path is None:
            archive_path = str(Path(project_base_path) / "archived")
            logger.info(f"archive_path not provided, using default: {archive_path}")
 
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
            archive_tile_name_format=archive_tile_name_format,
            archive_path=archive_path,
        )

    if convert:
        if tile_saving_type == TileSavingType.SPECTRAL:
            is_tilted = mosaic_id % 2 == 0

            aline_length = tile_size_x_tilted if is_tilted else tile_size_x_normal
            bline_length = tile_size_y

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
    states = []
    if archive_future is not None:
        archive_future.wait()
        states.append(archive_future.state)
    if spectral_to_complex_future is not None:
        spectral_to_complex_future.wait()
        states.append(spectral_to_complex_future.state)

    return states

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

    # Resolve parameters using helper function
    tile_saving_type = resolve_tile_saving_type(payload, project_config)

    archive_tile_name_format = resolve_config_param(
        payload,
        "archive_tile_name_format",
        project_config,
        config_attr="tile_archive_format",
    )

    tile_size_x_tilted = resolve_config_param(
        payload,
        "tile_size_x_tilted",
        project_config,
        config_attr="tile_size_x_tilted",
    )

    tile_size_x_normal = resolve_config_param(
        payload,
        "tile_size_x_normal",
        project_config,
        config_attr="tile_size_x_normal",
    )

    tile_size_y = resolve_config_param(
        payload,
        "tile_size_y",
        project_config,
        config_attr="tile_size_y",
    )

    # Handle archive_path (only needed if archive is enabled)
    archive = payload.get("archive", True)
    archive_path = None
    if archive:
        archive_path = resolve_config_param(
            payload,
            "archive_path",
            project_config,
            config_attr="archive_path",
        )
        if archive_path is None:
            # Default to project_base_path / "archived" if archive is enabled
            archive_path = str(Path(payload["project_base_path"]) / "archived")

    process_tile_batch_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        batch_id=payload["batch_id"],
        file_list=payload["file_list"],
        archive=archive,
        convert=payload.get("convert", True),
        tile_saving_type=tile_saving_type,
        archive_tile_name_format=archive_tile_name_format,
        archive_path=archive_path,
        tile_size_x_tilted=tile_size_x_tilted,
        tile_size_x_normal=tile_size_x_normal,
        tile_size_y=tile_size_y,
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
    if batch_processed_path.exists():
        logger.info(f"Batch {batch_id} already processed")
    else:
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
