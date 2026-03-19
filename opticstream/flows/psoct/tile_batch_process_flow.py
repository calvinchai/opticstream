from prefect import flow, get_run_logger

from opticstream.config.psoct_scan_config import TileSavingType
from opticstream.state.oct_project_state import OCTBatchId
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel

@flow(flow_run_name="process-tile-batch {batch_id}")
def process_tile_batch_flow(
    batch_id: OCTBatchId,
    config: PSOCTScanConfigModel,
    force_rerun: bool = False,
):
    # check rerun
    
    
    # archive raw tiles task


    # 3 kinds of processing 
    # spectral2processed
    # spectral2complex complex2processed
    # complex2processed 

    # upload to dandi (separated files)

    logger = get_run_logger()
    logger.info(f"Processing batch ")

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
    pass