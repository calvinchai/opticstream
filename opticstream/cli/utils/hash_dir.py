from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from .cli import utils_cli


def _hash_file(path: Path, algo: str, chunk_size: int = 8192 * 1024) -> str:
    hasher = hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


@utils_cli.command
def hash_dir(
    *,
    input: str,
    output: str,
    algo: str = "sha256",
) -> None:
    """
    Recursively hash all files in a directory and write results to CSV.

    Parameters
    ----------
    input:
        Input directory to scan recursively.
    output:
        Output CSV file path.
    algo:
        Hash algorithm (e.g. md5, sha1, sha256, sha512).
    """
    input_dir = Path(input).resolve()
    output_csv = Path(output).resolve()

    if not input_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir}")

    try:
        hashlib.new(algo)
    except ValueError as exc:
        raise ValueError(f"Unsupported hash algorithm: {algo}") from exc

    rows = []

    for path in sorted(input_dir.rglob("*")):
        if path.is_file():
            digest = _hash_file(path, algo)
            rows.append(
                {
                    "path": str(path.relative_to(input_dir)),
                    "hash": digest,
                    "algorithm": algo,
                }
            )

    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["path", "hash", "algorithm"])
        writer.writeheader()
        writer.writerows(rows)
