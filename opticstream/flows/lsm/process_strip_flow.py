from prefect import get_run_logger, task


@task(task_run_name="{project_name}-compress-slice-{slice_number}-strip-{strip_number}")
def compress_strip_task(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    output_path: str,
    
) -> None:
    """
    Compress a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Compressing strip {strip_number} of slice {slice_number}")
    

def process_strip_flow(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    output_path: str,
    mip_output_path: str,

) -> None:
    """
    Process a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Processing strip {strip_number} of slice {slice_number}")
    