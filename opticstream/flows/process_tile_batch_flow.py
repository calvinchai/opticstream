import os
import os.path as op
from pathlib import Path
from typing import Any, Dict, List, Optional

from prefect import flow, task
from prefect.logging import get_run_logger

from opticstream.config.constants import TileSavingType
from opticstream.config.project_config import resolve_config
from opticstream.events import (
    BATCH_ARCHIVED,
    BATCH_COMPLEXED,
)
from opticstream.events.batch_event_utils import emit_batch_event
from opticstream.flows.state_management_flow import (
    check_completion_and_emit_mosaic_ready_task,
    update_mosaic_artifact_task,
)
from opticstream.state import (
    is_batch_archived,
    mark_batch_archived,
    mark_batch_started,
)
from opticstream.utils.filename_utils import (
    complex_to_complex_filename,
    extract_tile_index_from_filename,
    spectral_to_complex_filename,
)
from opticstream.tasks.common_tasks import archive_tile_task
from opticstream.utils.matlab_execution import run_matlab_batch_command
from opticstream.utils.utils import get_mosaic_paths


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
    logger.info(f"Spectral to complex batch command: {spectral2complex_batch_cmd}")
    run_matlab_batch_command(spectral2complex_batch_cmd)

    complex_file_list = []
    for spectral_file in file_list:
        complex_file = spectral_to_complex_filename(spectral_file, complex_path)
        complex_file_list.append(complex_file)
    for complex_file in complex_file_list:
        complex_file_path = Path(complex_file)
        if not complex_file_path.is_file():
            logger.error(f"Complex file {complex_file} does not exist")
            raise FileNotFoundError(f"Complex file {complex_file} does not exist")
        if complex_file_path.stat().st_size < 256 * 1024 * 1024:
            logger.error(f"Complex file {complex_file} is too small")
            raise ValueError(f"Complex file {complex_file} is too small")
    return complex_file_list


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
        # Normalize complex filename and construct full path
        raw_file_path = complex_to_complex_filename(complex_file, complex_path)
        os.symlink(complex_file, raw_file_path)
        complex_file_list.append(raw_file_path)

    return complex_file_list


@task(task_run_name="{project_name}-mosaic-{mosaic_id}-batch-{batch_id}-archive")
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

    Returns
    -------
    List[str]
        List of archived file paths
    """
    logger = get_run_logger()
    # Ensure archive_path directory exists
    os.makedirs(archive_path, exist_ok=True)

    # Determine acquisition type based on mosaic_id
    titled_illumination = mosaic_id % 2 == 0
    acq = "tilted" if titled_illumination else "normal"
    slice_id = (mosaic_id + 1) // 2

    # Store list of archived file paths for upload
    archived_file_paths = []
    archive_future = []
    # Process each file one by one
    for file_path in file_list:
        # Extract tile_index from filename
        tile_index = extract_tile_index_from_filename(file_path)
        # Generate archived tile name using format string
        archived_tile_name = archive_tile_name_format.format(
            project_name=project_name,
            slice_id=slice_id,
            tile_id=tile_index,  # Map tile_index to tile_id for format string
            acq=acq,
        )
        archived_tile_path = op.join(archive_path, archived_tile_name)
        Path(archived_tile_path).parent.mkdir(parents=True, exist_ok=True)
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
    mark_batch_archived(project_base_path, mosaic_id, batch_id)
    for archived_file_path in archived_file_paths:
        archived_file_path_path = Path(archived_file_path)
        if not archived_file_path_path.is_file():
            logger.error(f"Archived file {archived_file_path} does not exist")
            raise FileNotFoundError(
                f"Archived file {archived_file_path} does not exist"
            )
        if archived_file_path_path.stat().st_size < 256 * 1024 * 1024:
            logger.error(f"Archived file {archived_file_path} is too small")
            raise ValueError(f"Archived file {archived_file_path} is too small")
    mark_batch_archived(project_base_path, mosaic_id, batch_id)
    emit_batch_event(
        event_name=BATCH_ARCHIVED,
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        payload={"archived_file_paths": archived_file_paths},
    )
    update_mosaic_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
    )
    return archived_file_paths


def _determine_processing_mode(
    convert: bool,
    tile_saving_type: TileSavingType,
    mosaic_id: int,
    tile_size_x_tilted: int,
    tile_size_x_normal: int,
    tile_size_y: int,
) -> Dict[str, Any]:
    """
    Determine processing mode parameters for spectral-to-complex conversion.

    Parameters
    ----------
    convert : bool
        Whether to convert
    tile_saving_type : TileSavingType
        Type of tile saving (SPECTRAL or COMPLEX)
    mosaic_id : int
        Mosaic identifier
    tile_size_x_tilted : int
        Tile size X for tilted illumination
    tile_size_x_normal : int
        Tile size X for normal illumination
    tile_size_y : int
        Tile size Y

    Returns
    -------
    Dict[str, Any]
        Dictionary with 'mode' ('spectral', 'complex', or None) and parameters
    """
    if not convert:
        return {"mode": None}

    if tile_saving_type == TileSavingType.SPECTRAL:
        is_tilted = mosaic_id % 2 == 0
        aline_length = tile_size_x_tilted if is_tilted else tile_size_x_normal
        bline_length = tile_size_y
        return {
            "mode": "spectral",
            "aline_length": aline_length,
            "bline_length": bline_length,
        }
    elif tile_saving_type == TileSavingType.COMPLEX:
        return {"mode": "complex"}

    return {"mode": None}


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
    archive_tile_name_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii.gz",
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
    mark_batch_started(project_base_path, mosaic_id, batch_id)

    # Validate archive_path if archive is enabled
    if archive:
        if archive_path is None:
            archive_path = str(Path(project_base_path) / "archived")
            logger.info(f"archive_path not provided, using default: {archive_path}")

    # Determine processing mode
    processing_mode = _determine_processing_mode(
        convert,
        tile_saving_type,
        mosaic_id,
        tile_size_x_tilted,
        tile_size_x_normal,
        tile_size_y,
    )

    # Run archive and spectral2complex in parallel (if enabled)
    archive_future = None
    spectral_to_complex_future = None

    if archive and not is_batch_archived(project_base_path, mosaic_id, batch_id):
        archive_future = archive_tile_batch_task.submit(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            file_list=file_list,
            archive_tile_name_format=archive_tile_name_format,
            archive_path=archive_path,
        )

    if processing_mode["mode"] == "spectral":
        spectral_to_complex_future = spectral_to_complex_batch_task.submit(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            file_list=file_list,
            aline_length=processing_mode["aline_length"],
            bline_length=processing_mode["bline_length"],
        )
    elif processing_mode["mode"] == "complex":
        spectral_to_complex_future = complex_to_complex_batch_task.submit(
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            file_list=file_list,
        )

    # Wait for tasks and emit events
    states = []
    archived_file_paths = None
    complex_file_list = None

    if archive_future is not None:
        archive_future.wait()
        states.append(archive_future.state)
        
        archived_file_paths = archive_future.result()

    if spectral_to_complex_future is not None:
        spectral_to_complex_future.wait()
        states.append(spectral_to_complex_future.state)
        complex_file_list = spectral_to_complex_future.result()
        
        emit_batch_event(
            event_name=BATCH_COMPLEXED,
            project_name=project_name,
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            batch_id=batch_id,
            payload={"file_list": complex_file_list},
        )

    update_mosaic_artifact_task(
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
    )
    
    return states


@flow
def process_tile_batch_event_flow(payload: Dict[str, Any]):
    """
    Wrapper flow for event-driven triggering.
    Resolves config from payload and project config, then calls process_tile_batch_flow.
    """
    # Resolve config parameters
    config = resolve_config(
        payload=payload,
        keys=[
            "tile_saving_type",
            "archive_tile_name_format",
            "tile_size_x_tilted",
            "tile_size_x_normal",
            "tile_size_y",
            "archive_path",
        ],
    )

    # Handle archive_path default if archive is enabled
    archive = payload.get("archive", True)
    if archive and "archive_path" not in config:
        config["archive_path"] = str(Path(payload["project_base_path"]) / "archived")

    process_tile_batch_flow(
        project_name=payload["project_name"],
        project_base_path=payload["project_base_path"],
        mosaic_id=payload["mosaic_id"],
        batch_id=payload["batch_id"],
        file_list=payload["file_list"],
        archive=archive,
        convert=payload.get("convert", True),
        **config,
    )
