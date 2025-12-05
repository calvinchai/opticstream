"""
Upload Queue Manager for cloud uploads.

Manages upload queue with concurrency control using CLI tools
(e.g., aws s3 cp, gsutil cp, azcopy).
Maximum 5 concurrent uploads at a time.
"""

import queue
import threading
import subprocess
import logging
from typing import Optional, Dict
from pathlib import Path

logger = logging.getLogger(__name__)


class UploadQueueManager:
    """
    Manages upload queue with concurrency control.
    Uses CLI tool for actual uploads (e.g., aws s3 cp, gsutil cp).
    Maximum 5 concurrent uploads at a time.
    """
    
    def __init__(
        self,
        max_concurrent: int = 5,
        cli_tool: str = "aws",
        cli_base_args: Optional[list] = None,
        num_workers: int = 1
    ):
        """
        Initialize upload queue manager.
        
        Parameters
        ----------
        max_concurrent : int
            Maximum number of concurrent uploads (default: 5)
        cli_tool : str
            CLI tool command (default: "aws")
        cli_base_args : list, optional
            Base arguments for CLI tool (e.g., ["s3", "cp"] for aws s3 cp)
        num_workers : int
            Number of background worker threads (default: 1)
        """
        self.queue = queue.Queue()
        self.semaphore = threading.Semaphore(max_concurrent)
        self.cli_tool = cli_tool
        self.cli_base_args = cli_base_args or []
        self.upload_workers = []
        self.running = False
        self.num_workers = num_workers
        
    def start(self):
        """Start background upload workers."""
        if self.running:
            logger.warning("Upload queue manager already running")
            return
        
        self.running = True
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._upload_worker,
                name=f"UploadWorker-{i}",
                daemon=True
            )
            worker.start()
            self.upload_workers.append(worker)
        logger.info(f"Started {self.num_workers} upload worker(s)")
    
    def stop(self):
        """Stop upload workers (waits for queue to empty)."""
        self.running = False
        self.queue.join()  # Wait for all tasks to complete
        logger.info("Upload queue manager stopped")
    
    def enqueue(self, file_path: str, destination: str, metadata: Optional[Dict] = None):
        """
        Add file to upload queue (non-blocking).
        
        Parameters
        ----------
        file_path : str
            Path to file to upload
        destination : str
            Destination path/URL for upload
        metadata : dict, optional
            Additional metadata for logging
        """
        if not Path(file_path).exists():
            logger.error(f"File not found: {file_path}")
            return
        
        self.queue.put({
            "file_path": file_path,
            "destination": destination,
            "metadata": metadata or {}
        })
        logger.debug(f"Enqueued upload: {file_path} -> {destination}")
    
    def _upload_worker(self):
        """Background worker that processes upload queue."""
        while self.running:
            try:
                item = self.queue.get(timeout=1)
                with self.semaphore:  # Limit concurrent uploads
                    self._execute_upload(
                        item["file_path"],
                        item["destination"],
                        item["metadata"]
                    )
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Upload worker error: {e}")
                self.queue.task_done()
    
    def _execute_upload(self, file_path: str, destination: str, metadata: Dict):
        """
        Execute CLI tool for upload.
        
        Parameters
        ----------
        file_path : str
            Path to file to upload
        destination : str
            Destination path/URL
        metadata : dict
            Additional metadata
        """
        try:
            # Build command
            cmd = [self.cli_tool] + self.cli_base_args + [file_path, destination]
            
            logger.info(f"Uploading {file_path} to {destination}")
            
            # Execute CLI tool
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            logger.info(f"Successfully uploaded {file_path}")
            
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Upload failed for {file_path}: {e.stderr}"
            )
            # Could implement retry logic here
        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for {file_path}")
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")


# Global upload queue manager instance
_upload_queue_manager: Optional[UploadQueueManager] = None


def get_upload_queue_manager(
    max_concurrent: int = 5,
    cli_tool: str = "aws",
    cli_base_args: Optional[list] = None
) -> UploadQueueManager:
    """Get or create global upload queue manager (singleton pattern)."""
    global _upload_queue_manager
    
    if _upload_queue_manager is None:
        _upload_queue_manager = UploadQueueManager(
            max_concurrent=max_concurrent,
            cli_tool=cli_tool,
            cli_base_args=cli_base_args
        )
        _upload_queue_manager.start()
    
    return _upload_queue_manager

