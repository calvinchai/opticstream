"""
Prefect workflow that watches a folder for files using watchdog,
emits Prefect events when new files are detected, and processes them
in parallel: saving with .gz postfix and calling spectral2complex.
"""

import fnmatch
import glob
import gzip
import logging
import os
# Import spectral2complex function and load_spectral_file_raw
import sys
import time
from typing import Optional, Set

import numpy as np
from prefect import flow, get_run_logger, task
from prefect.events import emit_event
from prefect.tasks import task_input_hash
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from spectral_raw.spectral2complex import (
    spectral2complex,
    save_jstack_nifti
)

logger = logging.getLogger(__name__)


@task(name="load_spectral_file_raw", cache_key_fn=task_input_hash)
def load_spectral_file_raw_task(
    file_path: str
):
    with open(file_path, 'rb') as f:
        raw = f.read()
    return raw


@task(name="load_spectral_data_raw")
def load_spectral_data_raw_task(
    raw: bytes,
    aline_length: int = 200,
    bline_length: int = 350,
    nifti_header_size: int = 352
):
    data = np.frombuffer(raw[nifti_header_size:], dtype=np.uint16)
    data = data.reshape((2048, aline_length, 2, bline_length), order='F')
    return data


@task(name="save_file_with_gz")
def save_file_with_gz_task(file_content: bytes, output_dir: str, output_file_name: str):
    """
    Save the file to another directory with .gz postfix using gzip directly.
    
    Parameters
    ----------
    file_content : bytes
        Original file content to compress
    output_dir : str
        Output directory path
    output_file_name : str
        Output file name
    """
    task_logger = get_run_logger()
    # Add .gz extension
    output_filename = output_file_name + '.gz'
    output_path = os.path.join(output_dir, output_filename)

    task_logger.info(f"Compressing {output_file_name} to {output_path}")

    # Compress file using gzip directly
    with gzip.open(output_path, 'wb') as f:
        f.write(file_content)

    task_logger.info(f"Successfully saved compressed file: {output_path}")
    return output_path


@task(name="process_with_spectral2complex")
def process_with_spectral2complex_task(
    data: np.ndarray,
    output_dir: str,
    output_file_name: str,
):
    """
    Process the data using spectral2complex function.
    
    Parameters
    ----------
    data : np.ndarray
        The numpy array data from load_spectral_file_raw
        Shape: (bline_length, 2, 2048, aline_length)
    output_file_name : str
        Output file name
    output_dir : str
        Output directory for processed results
    """
    task_logger = get_run_logger()
    task_logger.info(f"Processing with spectral2complex: {output_file_name}")
    task_logger.info(f"Input data shape: {data.shape}, dtype: {data.dtype}")

    if data.ndim != 4:
        raise ValueError(
            f"Expected 4D data, got {data.ndim}D with shape {data.shape}"
        )

    spectral_array = data

    # Process with spectral2complex
    # Using default parameters - user can modify if needed
    processed_data = spectral2complex(
        spectral_array,
        disp_comp_file=None,
        AlineLength=2048,
        AutoCorrPeakCut=24,
        PaddingFactor=1
    )

    task_logger.info(f"Processed data shape: {processed_data.shape}")

    # Save processed output using save_jstack_nifti from spectral2complex module
    os.makedirs(output_dir, exist_ok=True)

    # Generate output filename
    output_filename = output_file_name + '_processed.nii'
    output_path = os.path.join(output_dir, output_filename)

    # Save as NIfTI using save_jstack_nifti
    save_jstack_nifti(processed_data, output_path)

    task_logger.info(f"Successfully saved processed file: {output_path}")
    return output_path


@flow(name="process_single_file_flow")
def process_single_file_flow(
    file_path: str,
    gz_output_dir: str,
    processed_output_dir: str,
    aline_length: int = 200,
    bline_length: int = 350,
    nifti_header_size: int = 352
):
    """
    Process a single file: load with load_spectral_file_raw, then save with .gz and
    process in parallel.
    This flow is triggered by Prefect events when new files are detected.
    
    Parameters
    ----------
    file_path : str
        Path to the file to process
    gz_output_dir : str
        Directory to save the .gz compressed file
    processed_output_dir : str
        Directory to save the processed output
    aline_length : int
        Aline length parameter (default: 200)
    bline_length : int
        Bline length parameter (default: 350)
    nifti_header_size : int
        NIfTI header size in bytes (default: 352)
    """
    flow_logger = get_run_logger()
    flow_logger.info(f"Processing file: {file_path}")

    # Load file with load_spectral_file_raw
    output_file_name = os.path.basename(file_path)

    file_content = load_spectral_file_raw_task(file_path)
    # Run two tasks in parallel using .submit()
    save_future = save_file_with_gz_task.submit(
        file_content=file_content,
        output_dir=gz_output_dir,
        output_file_name=output_file_name
    )
    data = load_spectral_data_raw_task(file_content, aline_length, bline_length,
                                       nifti_header_size)
    process_future = process_with_spectral2complex_task.submit(
        data=data,
        output_dir=processed_output_dir,
        output_file_name=output_file_name
    )

    # Wait for both tasks to complete
    gz_path = save_future.result()
    processed_path = process_future.result()

    flow_logger.info(f"Completed processing {file_path}")
    flow_logger.info(f"  - Compressed file: {gz_path}")
    flow_logger.info(f"  - Processed file: {processed_path}")

    # Emit completion event
    emit_event(
        event="spectral.file.processed",
        resource={
            "prefect.resource.id": f"file:{file_path}",
            "file.path": file_path,
            "file.name": output_file_name,
        },
        payload={
            "gz_path": gz_path,
            "processed_path": processed_path,
        }
    )

    return {
        "gz_path": gz_path,
        "processed_path": processed_path
    }


class SpectralFileHandler(FileSystemEventHandler):
    """File system event handler that emits Prefect events when new files are detected."""

    def __init__(
        self,
        file_pattern: str = "*",
        processed_files: Optional[Set[str]] = None,
        process_existing: bool = True
    ):
        self.file_pattern = file_pattern
        self.processed_files = processed_files if processed_files is not None else set()
        self.process_existing = process_existing
        self.logger = logging.getLogger(__name__)

    def _matches_pattern(self, file_path: str) -> bool:
        """Check if file matches the pattern."""
        if self.file_pattern == "*":
            return True
        filename = os.path.basename(file_path)
        return fnmatch.fnmatch(filename, self.file_pattern)

    def on_created(self, event: FileSystemEvent):
        """Handle file creation events."""
        if event.is_directory:
            return

        file_path = event.src_path

        # Check if file matches pattern
        if not self._matches_pattern(file_path):
            return

        # Check if already processed
        if file_path in self.processed_files:
            return

        # Wait a bit to ensure file is fully written
        time.sleep(0.5)

        # Verify file exists and is a regular file
        if not os.path.isfile(file_path):
            return

        self.logger.info(f"New file detected: {file_path}")

        # Emit Prefect event
        emit_event(
            event="spectral.file.created",
            resource={
                "prefect.resource.id": f"file:{file_path}",
                "file.path": file_path,
                "file.name": os.path.basename(file_path),
            },
            payload={
                "file_path": file_path,
                "file_pattern": self.file_pattern,
            }
        )

        self.processed_files.add(file_path)
        self.logger.info(f"Emitted event for file: {file_path}")


@flow(name="folder_watch_flow", log_prints=True)
def folder_watch_flow(
    watch_directory: str,
    file_pattern: str = "*",
    process_existing: bool = True,
):
    """
    Watch a folder for files using watchdog and emit Prefect events when new files are detected.
    This is a long-running flow that sets up the file watcher.
    
    Parameters
    ----------
    watch_directory : str
        Directory to watch for files
    file_pattern : str
        File pattern to match (e.g., "*.nii", "*.nii.gz")
    process_existing : bool
        If True, emit events for files that already exist in the directory
    """
    flow_logger = get_run_logger()
    flow_logger.info(f"Starting folder watch on: {watch_directory}")
    flow_logger.info(f"  - Pattern: {file_pattern}")
    flow_logger.info(f"  - Process existing: {process_existing}")

    # Ensure watch directory exists
    if not os.path.exists(watch_directory):
        os.makedirs(watch_directory)
        flow_logger.info(f"Created watch directory: {watch_directory}")

    # Track processed files to avoid reprocessing
    processed_files: Set[str] = set()

    # Process existing files if requested
    if process_existing:
        search_path = os.path.join(watch_directory, file_pattern)
        existing_files = glob.glob(search_path)
        existing_files = [f for f in existing_files if os.path.isfile(f)]

        if existing_files:
            flow_logger.info(
                f"Found {len(existing_files)} existing files, emitting events")
            for file_path in existing_files:
                if file_path not in processed_files:
                    emit_event(
                        event="spectral.file.created",
                        resource={
                            "prefect.resource.id": f"file:{file_path}",
                            "file.path": file_path,
                            "file.name": os.path.basename(file_path),
                        },
                        payload={
                            "file_path": file_path,
                            "file_pattern": file_pattern,
                        }
                    )
                    processed_files.add(file_path)
                    flow_logger.info(f"Emitted event for existing file: {file_path}")
        else:
            flow_logger.info("No existing files found")

    # Set up watchdog observer
    event_handler = SpectralFileHandler(
        file_pattern=file_pattern,
        processed_files=processed_files,
        process_existing=False  # Already handled above
    )

    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=False)
    observer.start()

    flow_logger.info("File watcher started, waiting for new files...")

    try:
        # Keep the flow running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        flow_logger.info("Received interrupt signal, stopping watch")
        observer.stop()

    observer.join()
    flow_logger.info("File watcher stopped")


@flow(name="process_file_event_flow")
def process_file_event_flow(
    file_path: str,
    gz_output_dir: str,
    processed_output_dir: str,
    aline_length: int = 200,
    bline_length: int = 350,
    nifti_header_size: int = 352
):
    """
    Event-driven flow that processes a file when triggered by a Prefect event.
    This flow is triggered by the 'spectral.file.created' event.
    
    Parameters
    ----------
    file_path : str
        Path to the file to process (from event payload)
    gz_output_dir : str
        Directory to save .gz compressed files
    processed_output_dir : str
        Directory to save processed outputs
    aline_length : int
        Aline length parameter (default: 200)
    bline_length : int
        Bline length parameter (default: 350)
    nifti_header_size : int
        NIfTI header size in bytes (default: 352)
    """
    return process_single_file_flow(
        file_path=file_path,
        gz_output_dir=gz_output_dir,
        processed_output_dir=processed_output_dir,
        aline_length=aline_length,
        bline_length=bline_length,
        nifti_header_size=nifti_header_size
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Watch folder using watchdog and process files with spectral2complex (event-driven)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Watch command - sets up the file watcher
    watch_parser = subparsers.add_parser("watch", help="Start file watcher")
    watch_parser.add_argument(
        "--watch-dir",
        type=str,
        required=True,
        help="Directory to watch for files"
    )
    watch_parser.add_argument(
        "--pattern",
        type=str,
        default="*",
        help="File pattern to match (default: *)"
    )
    watch_parser.add_argument(
        "--no-process-existing",
        action="store_true",
        help="Skip processing files that already exist"
    )

    # Process command - processes a single file (for testing or manual triggering)
    process_parser = subparsers.add_parser("process", help="Process a single file")
    process_parser.add_argument(
        "--file-path",
        type=str,
        required=True,
        help="Path to file to process"
    )
    process_parser.add_argument(
        "--gz-output-dir",
        type=str,
        required=True,
        help="Directory to save .gz compressed files"
    )
    process_parser.add_argument(
        "--processed-output-dir",
        type=str,
        required=True,
        help="Directory to save processed outputs"
    )
    process_parser.add_argument(
        "--aline-length",
        type=int,
        default=200,
        help="Aline length parameter (default: 200)"
    )
    process_parser.add_argument(
        "--bline-length",
        type=int,
        default=350,
        help="Bline length parameter (default: 350)"
    )
    process_parser.add_argument(
        "--nifti-header-size",
        type=int,
        default=352,
        help="NIfTI header size in bytes (default: 352)"
    )

    # Deploy command - creates deployments
    deploy_parser = subparsers.add_parser("deploy", help="Create Prefect deployments")
    deploy_parser.add_argument(
        "--watch-dir",
        type=str,
        required=True,
        help="Directory to watch for files"
    )
    deploy_parser.add_argument(
        "--gz-output-dir",
        type=str,
        required=True,
        help="Directory to save .gz compressed files"
    )
    deploy_parser.add_argument(
        "--processed-output-dir",
        type=str,
        required=True,
        help="Directory to save processed outputs"
    )
    deploy_parser.add_argument(
        "--pattern",
        type=str,
        default="*",
        help="File pattern to match (default: *)"
    )
    deploy_parser.add_argument(
        "--aline-length",
        type=int,
        default=200,
        help="Aline length parameter (default: 200)"
    )
    deploy_parser.add_argument(
        "--bline-length",
        type=int,
        default=350,
        help="Bline length parameter (default: 350)"
    )
    deploy_parser.add_argument(
        "--nifti-header-size",
        type=int,
        default=352,
        help="NIfTI header size in bytes (default: 352)"
    )

    args = parser.parse_args()

    if args.command == "watch":
        # Run the watch flow
        folder_watch_flow.serve(
            name="spectral-watch-deployment",
            tags=["file-watcher", "spectral-processing"],
            description="Watches folder for files using watchdog and emits Prefect events",
            parameters={
                "watch_directory": args.watch_dir,
                "file_pattern": args.pattern,
                "process_existing": not args.no_process_existing,
            }
        )
    elif args.command == "process":
        # Process a single file
        process_single_file_flow(
            file_path=args.file_path,
            gz_output_dir=args.gz_output_dir,
            processed_output_dir=args.processed_output_dir,
            aline_length=args.aline_length,
            bline_length=args.bline_length,
            nifti_header_size=args.nifti_header_size
        )
    elif args.command == "deploy":
        # Create deployments for both watch and process flows
        from prefect.deployments import Deployment

        # Deployment for watch flow
        watch_deployment = Deployment.build_from_flow(
            flow=folder_watch_flow,
            name="spectral-watch",
            parameters={
                "watch_directory": args.watch_dir,
                "file_pattern": args.pattern,
                "process_existing": True,
            },
            tags=["file-watcher"],
            description="Watches folder for files and emits Prefect events"
        )
        watch_deployment.apply()

        # Deployment for process flow (event-driven)
        process_deployment = Deployment.build_from_flow(
            flow=process_file_event_flow,
            name="spectral-process",
            parameters={
                "gz_output_dir": args.gz_output_dir,
                "processed_output_dir": args.processed_output_dir,
                "aline_length": args.aline_length,
                "bline_length": args.bline_length,
                "nifti_header_size": args.nifti_header_size,
            },
            tags=["spectral-processing"],
            description="Processes files triggered by Prefect events",
            # Note: In Prefect Cloud/Server, you would set up an automation
            # to trigger this deployment on 'spectral.file.created' events
        )
        process_deployment.apply()

        print("Deployments created successfully!")
        print("\nTo set up event-driven processing:")
        print(
            "1. Create an automation in Prefect UI that triggers 'spectral-process' deployment")
        print("2. Set trigger condition: event.spectral.file.created")
        print("3. Pass event.payload.file_path as the file_path parameter")
    else:
        parser.print_help()

