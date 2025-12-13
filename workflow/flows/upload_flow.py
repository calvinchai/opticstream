from typing import List
from prefect import flow
from workflow.tasks.upload import upload_to_linc_task, upload_to_dandi_task


@flow(name="upload_flow")
def upload_flow(
    file_path: str,
    instance = "linc"
):
    if instance == "linc":
        
        task = upload_to_linc_task.submit(file_path)
    elif instance == "dandi":
        
        task =upload_to_dandi_task.submit(file_path)
    else:
        raise ValueError(f"Invalid instance: {instance}")
    task.wait()
    return True
    