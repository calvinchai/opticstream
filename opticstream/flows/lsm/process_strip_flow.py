import os
from pathlib import Path
import shutil
from prefect import flow, get_run_logger, task
from niizarr.multizarr import ZarrConfig
from typing import Optional
import os.path as op

from prefect.events import emit_event

from opticstream.flows.lsm.event import STRIP_COMPRESSED


@task(task_run_name="{project_name}-compress-slice-{slice_number}-strip-{strip_number}")
def compress_strip_task(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    info_file: str,
    output_path: str,
    zarr_config: "ZarrConfig",
    mip_output_path: Optional[str] = None,
) -> None:
    """
    Compress a strip of a slice.
    """
    from linc_convert.modalities.lsm import strip

    logger = get_run_logger()
    logger.info(f"Compressing strip {strip_number} of slice {slice_number}")
    strip.convert(
        inp=strip_path,
        info_file=info_file,
        mip_image_output=output_path,
        out=output_path,
        zarr_config=zarr_config,
    )
    logger.info(
        f"Compressed strip {strip_number} of slice {slice_number} to {output_path}"
    )
    emit_event(
        event_name=STRIP_COMPRESSED,
        resource={
            "prefect.resource.id": f"strip:{project_name}:strip-{strip_number}",
            "project_name": project_name,
            "slice_number": slice_number,
            "strip_number": strip_number,
        },
        payload={
            "project_name": project_name,
            "slice_number": slice_number,
            "strip_number": strip_number,
            "output_path": output_path,
        },
    )


@task(task_run_name="{project_name}-backup-slice-{slice_number}-strip-{strip_number}")
def backup_strip_task(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    output_path: str,
) -> None:
    """
    Backup a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Backing up strip {strip_number} of slice {slice_number}")
    # copy the strip to the backup path
    shutil.copytree(strip_path, output_path)
    logger.info(
        f"Backed up strip {strip_number} of slice {slice_number} to {output_path}"
    )


def get_directory_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


@task
def check_compressed_result(
    project_name: str,
    slice_number: int,
    strip_number: int,
    output_path: str,
    zarr_size_threshold: int = 10**9,
) -> bool:
    """
    Check if the compressed strip is valid.
    """
    logger = get_run_logger()
    logger.info(
        f"Checking if the compressed strip {strip_number} of slice {slice_number} is valid"
    )
    # check if the output path exists
    if not os.path.exists(output_path):
        logger.error(
            f"Compressed strip {strip_number} of slice {slice_number} does not exist"
        )
        return False
    # check if the size of the zarr file is greater than the threshold
    if get_directory_size(output_path) < zarr_size_threshold:
        logger.error(
            f"Compressed strip {strip_number} of slice {slice_number} is smaller than the threshold"
        )
        return False
    return True


def compare_dirs_by_size(dir1, dir2):
    for root, _, files in os.walk(dir1):
        for file in files:
            path1 = os.path.join(root, file)

            # Get relative path to recreate structure in dir2
            relative_path = os.path.relpath(path1, dir1)
            path2 = os.path.join(dir2, relative_path)

            # Check if file exists in dir2
            if not os.path.exists(path2):
                print(f"Missing in dir2: {relative_path}")
                return False

            # Compare file sizes
            if os.path.getsize(path1) != os.path.getsize(path2):
                print(f"Size mismatch: {relative_path}")
                return False

    # Now check if dir2 has extra files
    for root, _, files in os.walk(dir2):
        for file in files:
            path2 = os.path.join(root, file)
            relative_path = os.path.relpath(path2, dir2)
            path1 = os.path.join(dir1, relative_path)

            if not os.path.exists(path1):
                print(f"Extra file in dir2: {relative_path}")
                return False

    return True


@task
def check_backup_result(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    backup_path: Optional[str] = None,
) -> bool:
    """
    Check if the backup strip is valid.
    """
    logger = get_run_logger()
    logger.info(
        f"Checking if the backup strip {strip_number} of slice {slice_number} is valid"
    )
    if backup_path is None:
        logger.warning(
            f"Backup path is not set for strip {strip_number} of slice {slice_number}"
        )
        return True
    # check if the backup path exists
    if not os.path.exists(backup_path):
        logger.error(
            f"Backup strip {strip_number} of slice {slice_number} does not exist"
        )
        return False
    # check if the backup path is the same as the strip path, recursively
    if not compare_dirs_by_size(backup_path, strip_path):
        logger.error(
            f"Backup strip {strip_number} of slice {slice_number} is not the same as the strip path"
        )
        return False
    return True


@task
def delete_strip_task(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    delete_strip: bool = False,
) -> None:
    """
    Delete a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Deleting strip {strip_number} of slice {slice_number}")
    # delete the strip
    shutil.rmtree(strip_path)


@flow(flow_run_name="{project_name}-process-slice-{slice_number}-strip-{strip_number}")
def process_strip_flow(
    project_name: str,
    slice_number: int,
    strip_number: int,
    strip_path: str,
    output_path: str,
    info_file: str,
    zarr_config: "ZarrConfig",
    output_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}.ome.zarr",
    output_mip_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff",
    archive_path: Optional[str] = None,
    output_mip: bool = True,
    delete_strip: bool = False,
) -> None:
    """
    Process a strip of a slice.
    """
    acq = "camera-02" if "C2" in strip_path else "camera-01"
    logger = get_run_logger()
    logger.info(f"Processing strip {strip_number} of slice {slice_number}")
    zarr_output_path = op.join(
        output_path,
        output_format.format(
            project_name=project_name,
            slice_id=slice_number,
            strip_id=strip_number,
            acq=acq,
        ),
    )
    mip_output_path = op.join(
        output_path,
        output_mip_format.format(
            project_name=project_name,
            slice_id=slice_number,
            strip_id=strip_number,
            acq=acq,
        ),
    )
    backup_path = (
        op.join(archive_path, os.path.basename(strip_path))
        if archive_path is not None
        else None
    )

    compress_future = compress_strip_task.submit(
        project_name=project_name,
        slice_number=slice_number,
        strip_number=strip_number,
        strip_path=strip_path,
        info_file=info_file,
        zarr_output_path=zarr_output_path,
        zarr_config=zarr_config,
        mip_output_path=mip_output_path,
    )
    backup_future = backup_strip_task.submit(
        project_name=project_name,
        slice_number=slice_number,
        strip_number=strip_number,
        strip_path=strip_path,
        backup_path=backup_path,
    )
    check_compressed_future = check_compressed_result.submit(
        project_name=project_name,
        slice_number=slice_number,
        strip_number=strip_number,
        output_path=output_path,
        wait_for=[compress_future],
    )
    check_backup_future = check_backup_result.submit(
        project_name=project_name,
        slice_number=slice_number,
        strip_number=strip_number,
        strip_path=strip_path,
        backup_path=backup_path,
        wait_for=[backup_future],
    )
    check_compressed_future.wait()
    check_backup_future.wait()
    logger.info(f"Strip {strip_number} of slice {slice_number} processed")
    return (check_compressed_future, check_backup_future)


process_strip_flow_deployment = process_strip_flow.to_deployment(
    name="process_strip_flow_deployment",
    tags=["lsm", "process-strip"],
    # triggers=[
    #     get_event_trigger(SLICE_READY),
    # ],
)
