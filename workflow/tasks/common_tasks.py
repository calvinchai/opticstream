"""
Common tasks shared across multiple flows.

This module contains tasks that are used by more than one flow to avoid duplication.
"""

import gzip
import hashlib
import os
from typing import List

from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import ShellOperation


@task(tags=["psoct-data-archive"])
def archive_tile_task(input_path: str, output_path: str, output_sha256: bool = True):
    """gzip the file"""
    logger = get_run_logger()
    if not output_path.endswith(".gz"):
        output_path += ".gz"
    
    # Initialize SHA-256 hasher if needed
    hasher = hashlib.sha256() if output_sha256 else None
    chunk_size = 8192 * 1024
    
    with gzip.open(output_path, "wb", compresslevel=3) as f:
        with open(input_path, "rb") as f_in:
            # Read in chunks and hash during compression
            for chunk in iter(lambda: f_in.read(chunk_size), b""):
                if output_sha256:
                    hasher.update(chunk)  # Hash the uncompressed content
                f.write(chunk)  # Write to gzip (compresses it)
    
    logger.info(f"Archived tile {input_path} to {output_path}")
    
    # Write hash file if requested
    if output_sha256:
        # Remove .gz extension to create hash filename
        hash_path = output_path[:-3] + ".sha256" if output_path.endswith(".gz") else output_path + ".sha256"
        hash_digest = hasher.hexdigest()
        with open(hash_path, "w") as hash_file:
            hash_file.write(hash_digest)
        logger.info(f"SHA-256 hash: {hash_digest} written to {hash_path}")


@task(tags=["dandi-upload"], retries=1)
def upload_to_dandi_task(file_path: str) -> None:
    """
    Upload the file to DANDI.
    """
    from prefect_shell import shell_run_command
    shell_run_command(f"conda run -n dandi dandi upload {file_path} -J 10:10")


@task(tags=["dandi-upload"], retries=1)
def upload_to_linc_task(file_path: str) -> None:
    """
    Upload the file to LINC.
    """
    logger = get_run_logger()
    with ShellOperation(
        commands=[
            f"/autofs/space/aspasia_002/users/code/miniforge3/envs/dandi-linc/bin"
            f"/dandi upload -i linc '{file_path}' -J 10:10 --allow-any-path "
            f"--existing overwrite --validation skip"
        ],
        env={
            "LINC_API_KEY": Secret.load("linc-api-key", validate=False).get(),
            "DANDI_API_KEY": Secret.load("linc-api-key", validate=False).get(),
            "DANDI_DEVEL": "1",
        },
        working_dir=os.path.dirname(file_path),
    ) as upload_operation:
        upload_process = upload_operation.trigger()
        upload_process.wait_for_completion()
        output = upload_process.fetch_result()
        logger.info(f"Upload output: {output}")


@task(tags=["dandi-upload"], retries=1)
def upload_to_linc_batch_task(file_list: List[str], realpath: bool = True) -> None:
    """
    Upload the file to LINC.
    """
    logger = get_run_logger()
    # TODO: use realpath only for debugging; remove this once paths are confirmed
    # Join file paths with spaces, each enclosed in single quotes
    file_paths_str = " ".join(
        f"'{os.path.realpath(file_path)}'" if realpath else f"'{file_path}'" for file_path in file_list
    )
    with ShellOperation(
        commands=[
            f"/autofs/space/aspasia_002/users/code/miniforge3/envs/dandi-linc/bin"
            f"/dandi upload -i linc -J 15:15 --allow-any-path --existing overwrite "
            f"--validation skip {file_paths_str}"
        ],
        env={
            "LINC_API_KEY": Secret.load("linc-api-key", validate=False).get(),
            "DANDI_API_KEY": Secret.load("linc-api-key", validate=False).get(),
            "DANDI_DEVEL": "1",
        },
        working_dir=os.path.dirname(file_list[0]) if file_list else os.getcwd(),
    ) as upload_operation:
        upload_process = upload_operation.trigger()
        upload_process.wait_for_completion()
        output = upload_process.fetch_result()
        logger.info(f"Upload output: {output}")


@task
def submit_upload_to_linc_task(file_path: str) -> None:
    #     flow_run = run_deployment(
    #     name="upload_flow/upload",
    #     parameters={
    #         "file_paths": [file_path],
    #         "instance": "linc"
    #     },
    #     timeout=0
    # )
    os.system(
        f"prefect deployment run 'upload_flow/upload' --param file_path='{file_path}' "
        f"--param instance=linc"
    )
    # return flow_run

