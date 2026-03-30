"""
Archive a tile file.
"""
import gzip
import hashlib
import shutil
from pathlib import Path

from prefect import get_run_logger, task


@task(tags=["psoct-data-archive"])
def archive_file(
    input_path: Path,
    output_path: Path,
    *,
    output_sha256: bool = True,
    chunk_size: int = 64 * 1024 * 1024,  # 64MB
) -> Path:
    """gzip the file"""
    logger = get_run_logger()

    if output_path.suffix.lower() != ".gz":
        output_path = output_path.with_name(output_path.name + ".gz")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    hasher = hashlib.sha256() if output_sha256 else None

    with gzip.open(output_path, "wb", compresslevel=3) as f_out:
        with open(input_path, "rb") as f_in:
            if hasher is None:
                shutil.copyfileobj(f_in, f_out, length=chunk_size)
            else:
                for chunk in iter(lambda: f_in.read(chunk_size), b""):
                    hasher.update(chunk)  # Hash the uncompressed content
                    f_out.write(chunk)    # Write to gzip (compresses it)

    logger.info(f"Archived file {input_path} to {output_path}")

    if hasher is not None:
        hash_path = output_path.with_suffix(".sha256")
        hash_digest = hasher.hexdigest()
        with hash_path.open("w") as hash_file:
            hash_file.write(hash_digest)
        logger.info(f"SHA-256 hash: {hash_digest} written to {hash_path}")

    return output_path