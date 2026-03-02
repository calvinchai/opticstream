#!/usr/bin/env python3

import argparse
import csv
import hashlib
from pathlib import Path


def hash_file(path: Path, algo: str, chunk_size: int = 8192 * 1024) -> str:
    """Compute hash of a file by streaming."""
    hasher = hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def main():
    parser = argparse.ArgumentParser(
        description="Recursively hash all files in a directory and write to CSV"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory to scan recursively",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV file",
    )
    parser.add_argument(
        "--algo",
        default="sha256",
        help="Hash algorithm (md5, sha1, sha256, sha512, etc.)",
    )

    args = parser.parse_args()

    input_dir = Path(args.input).resolve()
    output_csv = Path(args.output).resolve()

    if not input_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir}")

    try:
        hashlib.new(args.algo)
    except ValueError:
        raise ValueError(f"Unsupported hash algorithm: {args.algo}")

    rows = []

    for path in sorted(input_dir.rglob("*")):
        if path.is_file():
            digest = hash_file(path, args.algo)
            rows.append(
                {
                    "path": str(path.relative_to(input_dir)),
                    "hash": digest,
                    "algorithm": args.algo,
                }
            )

    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["path", "hash", "algorithm"]
        )
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
