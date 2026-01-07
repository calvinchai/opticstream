from typing import Any, Dict, List

from prefect import flow, task
from prefect.logging import get_run_logger

from workflow.events import BATCH_PROCESSED
from workflow.events.batch_event_utils import emit_batch_event
from workflow.state.batch_state_utils import is_batch_processed, mark_batch_processed
from workflow.utils.filename_utils import replace_spectral_with_complex_in_path
from workflow.utils.matlab_execution import run_matlab_batch_command
from workflow.utils.utils import get_mosaic_paths


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
        file_list_str.append(f"'{replace_spectral_with_complex_in_path(file)}'")
    file_list_str = f"{{{','.join(file_list_str)}}}"
    complex2processed_batch_cmd = (
        f"complex2processed_batch({file_list_str}, '{processed_path}/', \"find\", 80 )"
    )
    logger.info(complex2processed_batch_cmd)
    run_matlab_batch_command(complex2processed_batch_cmd)
    return None


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

    if is_batch_processed(project_base_path, mosaic_id, batch_id):
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
    mark_batch_processed(project_base_path, mosaic_id, batch_id)
    logger.info(f"Batch {batch_id} processed successfully")
    emit_batch_event(
        event_name=BATCH_PROCESSED,
        project_name=project_name,
        project_base_path=project_base_path,
        mosaic_id=mosaic_id,
        batch_id=batch_id,
        payload={},
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
