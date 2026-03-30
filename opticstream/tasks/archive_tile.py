"""
Common tasks shared across multiple flows.

This module contains tasks that are used by more than one flow to avoid duplication.
"""

import gzip
import hashlib
import os
from pathlib import Path
from typing import List
import os.path as op

from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from prefect_shell import ShellOperation


@task(tags=["psoct-data-archive"])
def archive_file(
    input_path: Path, 
    output_path: Path, 
    *,
    output_sha256: bool = True,
    chunk_size: int = 64 * 1024 * 1024, # 64MB
) -> Path:
    """gzip the file"""
    logger = get_run_logger()
    if not output_path.suffix.lower().endswith(".gz"):
        output_path = output_path.with_suffix(".gz")

    # Initialize SHA-256 hasher if needed
    hasher = hashlib.sha256() if output_sha256 else None

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
        hash_path = (
            # replace .gz extension with .sha256
            output_path.name.replace(".gz", ".sha256")
        )
        hash_path = output_path.parent / hash_path
        hash_digest = hasher.hexdigest()
        with hash_path.open("w") as hash_file:
            hash_file.write(hash_digest)
        logger.info(f"SHA-256 hash: {hash_digest} written to {hash_path}")
    return output_path

