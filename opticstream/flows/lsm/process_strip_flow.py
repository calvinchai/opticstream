import os
import os.path as op
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import psutil
from typing import Any, Dict, List, Optional

import dask
from niizarr.multizarr import ZarrConfig
from prefect import flow, get_run_logger, task
from prefect.events import emit_event

from opticstream.config.lsm_scan_config import LSMScanConfigModel, get_lsm_scan_config
from opticstream.flows.lsm.event import STRIP_COMPRESSED
from opticstream.utils.filename_utils import parse_lsm_run_folder_name


@task(
    task_run_name="{project_name}-compress-slice-{slice_id}-strip-{strip_id}-camera-{"
    "camera_id}"
)
def compress_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    info_file: str,
    output_path: str,
    zarr_config: "ZarrConfig",
    num_workers: int = 6,
    cpu_affinity: Optional[List[int]] = None,
    mip_output_path: Optional[str] = None,
    camera_id: Optional[int] = None,
) -> None:
    """
    Compress a strip of a slice.
    """
    from linc_convert.modalities.lsm import strip

    logger = get_run_logger()
    logger.info(f"Compressing strip {strip_id} of slice {slice_id}")

    if cpu_affinity is not None:
        p = psutil.Process(os.getpid())
        p.cpu_affinity(list(range(*cpu_affinity)))
        # set_pid_io_priority(os.getpid(), 1)
        logger.info(f"cpu affinity:{p.cpu_affinity()}")

    zarr_config.overwrite = True
    with dask.config.set(pool=ThreadPoolExecutor(num_workers)):
        strip.convert(
            inp=strip_path,
            info_file=info_file,
            mip_image_output=mip_output_path,
            out=output_path,
            zarr_config=zarr_config,
            nii=False,
        )
    logger.info(f"Compressed strip {strip_id} of slice {slice_id} to {output_path}")

    emit_event(
        STRIP_COMPRESSED,
        resource={
            "prefect.resource.id": f"strip:{project_name}:strip-{strip_id}",
            "project_name": project_name,
            "slice_id": str(slice_id),
            "strip_id": str(strip_id),
        },
        payload={
            "project_name": project_name,
            "slice_id": slice_id,
            "strip_id": strip_id,
            "output_path": output_path,
        },
    )


@task(
    task_run_name="{project_name}-backup-slice-{slice_id}-strip-{strip_id}-camera-{"
    "camera_id}"
)
def backup_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    output_path: str,
    camera_id: Optional[int] = None,
) -> None:
    """
    Backup a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Backing up strip {strip_id} of slice {slice_id}")
    # copy the strip to the backup path
    shutil.copytree(strip_path, output_path, dirs_exist_ok=True)
    logger.info(f"Backed up strip {strip_id} of slice {slice_id} to {output_path}")


def get_directory_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


@task(
    task_run_name="{project_name}-check-compressed-slice-{slice_id}-strip-{"
    "strip_id}-camera-{camera_id}"
)
def check_compressed_result(
    project_name: str,
    slice_id: int,
    strip_id: int,
    output_path: str,
    zarr_size_threshold: int = 10**9,
    camera_id: Optional[int] = None,
) -> bool:
    """
    Check if the compressed strip is valid.
    """
    logger = get_run_logger()
    logger.info(
        f"Checking if the compressed strip {strip_id} of slice {slice_id} is valid"
    )
    # check if the output path exists
    if not os.path.exists(output_path):
        logger.error(f"Compressed strip {strip_id} of slice {slice_id} does not exist")
        return False
    # check if the size of the zarr file is greater than the threshold
    if get_directory_size(output_path) < zarr_size_threshold:
        logger.error(
            f"Compressed strip {strip_id} of slice {slice_id} is smaller than the "
            f"threshold"
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


@task(
    task_run_name="{project_name}-check-backup-slice-{slice_id}-strip-{"
    "strip_id}-camera-{camera_id}"
)
def check_backup_result(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    backup_path: Optional[str] = None,
    camera_id: Optional[int] = None,
) -> bool:
    """
    Check if the backup strip is valid.
    """
    logger = get_run_logger()
    logger.info(f"Checking if the backup strip {strip_id} of slice {slice_id} is valid")
    if backup_path is None:
        logger.warning(
            f"Backup path is not set for strip {strip_id} of slice {slice_id}"
        )
        return True
    # check if the backup path exists
    if not os.path.exists(backup_path):
        logger.error(f"Backup strip {strip_id} of slice {slice_id} does not exist")
        return False
    # check if the backup path is the same as the strip path, recursively
    if not compare_dirs_by_size(backup_path, strip_path):
        logger.error(
            f"Backup strip {strip_id} of slice {slice_id} is not the same as the "
            f"strip path"
        )
        return False
    return True


@task(
    task_run_name="{project_name}-rename-strip-slice-{slice_id}-strip-{"
    "strip_id}-camera-{camera_id}"
)
def rename_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    rename_strip: bool = False,
    camera_id: Optional[int] = None,
) -> None:
    """
    Rename a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Renaming strip {strip_id} of slice {slice_id}")
    if not rename_strip:
        logger.info(f"Skipping renaming of strip {strip_id} of slice {slice_id}")
        return
    parent_path, basename = os.path.split(strip_path)
    processed_folder = op.join(parent_path, "processed")
    os.makedirs(processed_folder, exist_ok=True)
    new_strip_path = op.join(processed_folder, basename)
    os.rename(strip_path, new_strip_path)
    logger.info(f"Renamed strip {strip_id} of slice {slice_id} to {new_strip_path}")


@task(
    task_run_name="{project_name}-delete-strip-slice-{slice_id}-strip-{"
    "strip_id}-camera-{camera_id}"
)
def delete_strip_task(
    project_name: str,
    slice_id: int,
    strip_id: int,
    strip_path: str,
    delete_strip: bool = False,
) -> None:
    """
    Delete a strip of a slice.
    """
    if not delete_strip:
        logger.info(f"Skipping deletion of strip {strip_id} of slice {slice_id}")
        return
    logger = get_run_logger()
    logger.info(f"Deleting strip {strip_id} of slice {slice_id}")
    # delete the strip
    shutil.rmtree(strip_path)


@flow(
    flow_run_name="{project_name}-process-slice-{slice_id}-strip-{strip_id}-camera-{"
    "camera_id}"
)
def process_strip_flow(
    project_name: str,
    strip_path: str,
    slice_id: Optional[int] = None,
    strip_id: Optional[int] = None,
    camera_id: Optional[int] = None,
    scan_config: LSMScanConfigModel = None,
) -> None:
    """
    Process a strip of a slice.
    """
    logger = get_run_logger()
    logger.info(f"Processing strip path: {strip_path}")
    if slice_id is None or strip_id is None:
        logger.info(
            f"Parsing slice_id, strip_id, and camera_id from strip path: {strip_path}"
        )
        run_index, strip_index, camera_index = parse_lsm_run_folder_name(
            os.path.basename(strip_path)
        )
        slice_id = run_index if slice_id is None else slice_id
        strip_id = strip_index if strip_id is None else strip_id
        camera_id = camera_index if camera_id is None else camera_id
        logger.info(
            f"Parsed slice_id: {slice_id}, strip_id: {strip_id}, camera_id: {camera_id}"
        )
    acq = f"camera-{camera_id:02d}" if camera_id is not None else None
    logger.info(f"Processing strip {strip_id} of slice {slice_id}")
    # Resolve output and archive settings from scan_config
    output_path = scan_config.output_path or scan_config.project_base_path
    output_format = (
        scan_config.output_format
        or "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{"
        "acq}.ome.zarr"
    )
    output_mip_format = scan_config.output_mip_format
    archive_path = scan_config.archive_path

    # Resolve processing flags and resources
    generate_zarr = scan_config.generate_zarr
    generate_mip = scan_config.generate_mip
    generate_archive = scan_config.generate_archive
    delete_strip = scan_config.delete_strip
    num_workers = scan_config.num_workers
    cpu_affinity = scan_config.cpu_affinity or None

    zarr_output_path = op.join(
        output_path,
        output_format.format(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            acq=acq,
        ),
    )
    mip_output_path = None
    if generate_mip:
        mip_output_path = op.join(
            output_path,
            output_mip_format.format(
                project_name=project_name,
                slice_id=slice_id,
                strip_id=strip_id,
                acq=acq,
            ),
        )

    backup_path = None
    if generate_archive and archive_path is not None:
        backup_path = op.join(archive_path, os.path.basename(strip_path))

    compress_future = None
    if generate_zarr:
        compress_future = compress_strip_task.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            info_file=scan_config.info_file,
            output_path=zarr_output_path,
            zarr_config=scan_config.zarr_config,
            num_workers=num_workers,
            cpu_affinity=cpu_affinity,
            mip_output_path=mip_output_path,
            camera_id=camera_id,
        )

    backup_future = None
    if backup_path is not None:
        backup_future = backup_strip_task.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            output_path=backup_path,
            camera_id=camera_id,
        )

    check_compressed_future = None
    if compress_future is not None:
        check_compressed_future = check_compressed_result.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            output_path=zarr_output_path,
            camera_id=camera_id,
            wait_for=[compress_future],
        )

    check_backup_future = None
    if backup_future is not None:
        check_backup_future = check_backup_result.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            backup_path=backup_path,
            camera_id=camera_id,
            wait_for=[backup_future],
        )
    delete_strip_future = None
    if delete_strip:
        delete_strip_future = delete_strip_task.submit(
            project_name=project_name,
            slice_id=slice_id,
            strip_id=strip_id,
            strip_path=strip_path,
            delete_strip=delete_strip,
            camera_id=camera_id,
        )

    if check_compressed_future is not None:
        check_compressed_future.wait()
    if check_backup_future is not None:
        check_backup_future.wait()
    if delete_strip_future is not None:
        delete_strip_future.wait()

    logger.info(f"Strip {strip_id} of slice {slice_id} processed")
    return (check_compressed_future, check_backup_future)


@flow
def process_strip_event(payload: Dict[str, Any]) -> None:
    """
    Process a strip of a slice.
    """
    logger = get_run_logger()
    lsm_scan_config = get_lsm_scan_config(
        payload["project_name"],
        override_config_name=payload.get("override_config_name"),
    )
    process_strip_flow(
        project_name=payload["project_name"],
        strip_path=payload["strip_path"],
        slice_id=payload["slice_id"],
        strip_id=payload["strip_id"],
        camera_id=payload["camera_id"],
        scan_config=lsm_scan_config,
    )


process_strip_flow_deployment = process_strip_flow.to_deployment(
    name="process_strip_flow_deployment",
    tags=["lsm", "process-strip"],
)
