#!/usr/bin/env python3
"""
Compare two hash files with different naming conventions.

File 1 format: mosaic_XXX_image_YYY_spectral_ZZZZ.nii
File 2 format: rawdata/sub-XXX/voi-YYY/sample-sliceZZ/acq-WWW/sub-XXX_voi-YYY_sample-sliceZZ_acq-WWW_chunk-AAAA_OCT.nii.gz

Conversion rules:
- mosaic number = sliceid*2-(acq==normal)
- chunkid == imageid
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def parse_mosaic_path(path: str) -> Optional[Tuple[int, int]]:
    """
    Parse mosaic format path: mosaic_XXX_image_YYY_spectral_ZZZZ.nii
    
    Returns (mosaic_id, image_id) or None if parsing fails.
    """
    pattern = r'mosaic_(\d+)_image_(\d+)_spectral_\d+\.nii'
    match = re.search(pattern, path)
    if match:
        mosaic_id = int(match.group(1))
        image_id = int(match.group(2))
        return (mosaic_id, image_id)
    return None


def parse_rawdata_path(path: str) -> Optional[Tuple[int, str, int]]:
    """
    Parse rawdata format path: rawdata/sub-XXX/voi-YYY/sample-sliceZZ/acq-WWW/...chunk-AAAA_OCT.nii.gz
    
    Returns (slice_id, acq, chunk_id) or None if parsing fails.
    """
    # Extract slice number from sample-sliceXX
    slice_match = re.search(r'sample-slice(\d+)', path)
    if not slice_match:
        return None
    
    slice_id = int(slice_match.group(1))
    
    # Extract acquisition type (normal or tilted)
    acq_match = re.search(r'acq-(\w+)', path)
    if not acq_match:
        return None
    
    acq = acq_match.group(1)
    
    # Extract chunk number from chunk-XXXX
    chunk_match = re.search(r'chunk-(\d+)', path)
    if not chunk_match:
        return None
    
    chunk_id = int(chunk_match.group(1))
    
    return (slice_id, acq, chunk_id)


def mosaic_to_slice_acq(mosaic_id: int) -> Tuple[int, str]:
    """
    Convert mosaic_id to (slice_id, acq).
    
    Formula: mosaic number = sliceid*2-(acq==normal)
    Reverse: 
    - If mosaic_id is odd: normal, slice_id = (mosaic_id + 1) / 2
    - If mosaic_id is even: tilted, slice_id = mosaic_id / 2
    """
    if mosaic_id % 2 == 0:
        # Even = tilted
        slice_id = mosaic_id // 2
        acq = "tilted"
    else:
        # Odd = normal
        slice_id = (mosaic_id + 1) // 2
        acq = "normal"
    
    return (slice_id, acq)


def slice_acq_to_mosaic(slice_id: int, acq: str) -> int:
    """
    Convert (slice_id, acq) to mosaic_id.
    
    Formula: mosaic number = sliceid*2-(acq==normal)
    """
    if acq == "normal":
        mosaic_id = slice_id * 2 - 1
    else:  # tilted
        mosaic_id = slice_id * 2
    
    return mosaic_id


def load_hash_file(file_path: Path, file_format: str) -> Dict[Tuple, Dict[str, str]]:
    """
    Load hash file and return dictionary keyed by identifiers.
    
    For mosaic format: key is (mosaic_id, image_id)
    For rawdata format: key is (slice_id, acq, chunk_id)
    
    Returns dict mapping key -> {'path': str, 'hash': str, 'algorithm': str}
    """
    data = {}
    
    with open(file_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            print(row)
            path = row.get('path', '') or row.get('asset_path', '')
            
            hash_value = row.get('hash', '') or row.get('sha256', '')
            algorithm = row.get('algorithm', 'sha256')
            if file_format == 'mosaic':
                key = parse_mosaic_path(path)
                if key is None:
                    print(f"Warning: Could not parse mosaic path: {path}", file=sys.stderr)
                    continue
            elif file_format == 'rawdata':
                key = parse_rawdata_path(path)
                if key is None:
                    print(f"Warning: Could not parse rawdata path: {path}", file=sys.stderr)
                    continue
            else:
                raise ValueError(f"Unknown file format: {file_format}")
            
            if key in data:
                print(f"Warning: Duplicate key {key} for path: {path}", file=sys.stderr)
            
            data[key] = {
                'path': path,
                'hash': hash_value,
                'algorithm': algorithm,
            }
    
    return data


def compare_hash_files(
    file1_path: Path,
    file1_format: str,
    file2_path: Path,
    file2_format: str,
) -> Dict[str, List]:
    """
    Compare two hash files and return comparison results.
    
    Returns dictionary with:
    - 'matches': List of matching entries
    - 'mismatches': List of entries with different hashes
    - 'file1_only': List of entries only in file1
    - 'file2_only': List of entries only in file2
    """
    # Load both files
    print(f"Loading {file1_format} file: {file1_path}")
    data1 = load_hash_file(file1_path, file1_format)
    print(f"  Loaded {len(data1)} entries")
    
    print(f"Loading {file2_format} file: {file2_path}")
    data2 = load_hash_file(file2_path, file2_format)
    print(f"  Loaded {len(data2)} entries")
    
    # Convert data2 keys to match data1 format
    # If file2 is rawdata format, convert to (mosaic_id, image_id)
    # If file2 is mosaic format, keep as is
    data2_converted = {}
    
    if file2_format == 'rawdata':
        for (slice_id, acq, chunk_id), info in data2.items():
            mosaic_id = slice_acq_to_mosaic(slice_id, acq)
            image_id = chunk_id  # chunkid == imageid
            key = (mosaic_id, image_id)
            data2_converted[key] = info
    else:
        data2_converted = data2
    
    # Compare
    matches = []
    mismatches = []
    file1_only = []
    file2_only = []
    
    # Check entries in file1
    for key, info1 in data1.items():
        if key in data2_converted:
            info2 = data2_converted[key]
            if info1['hash'].lower() == info2['hash'].lower():
                matches.append({
                    'key': key,
                    'file1_path': info1['path'],
                    'file2_path': info2['path'],
                    'hash': info1['hash'],
                })
            else:
                mismatches.append({
                    'key': key,
                    'file1_path': info1['path'],
                    'file2_path': info2['path'],
                    'file1_hash': info1['hash'],
                    'file2_hash': info2['hash'],
                })
        else:
            file1_only.append({
                'key': key,
                'path': info1['path'],
                'hash': info1['hash'],
            })
    
    # Check entries only in file2
    for key, info2 in data2_converted.items():
        if key not in data1:
            file2_only.append({
                'key': key,
                'path': info2['path'],
                'hash': info2['hash'],
            })
    
    return {
        'matches': matches,
        'mismatches': mismatches,
        'file1_only': file1_only,
        'file2_only': file2_only,
    }


def print_results(results: Dict[str, List], output_file: Optional[Path] = None):
    """Print comparison results to stdout or file."""
    output = sys.stdout if output_file is None else open(output_file, 'w')
    
    try:
        print("=" * 80, file=output)
        print("Hash File Comparison Results", file=output)
        print("=" * 80, file=output)
        print(f"\nMatches: {len(results['matches'])}", file=output)
        print(f"Mismatches: {len(results['mismatches'])}", file=output)
        print(f"File 1 only: {len(results['file1_only'])}", file=output)
        print(f"File 2 only: {len(results['file2_only'])}", file=output)
        
        if results['mismatches']:
            print("\n" + "=" * 80, file=output)
            print("MISMATCHES (Different Hashes)", file=output)
            print("=" * 80, file=output)
            for item in results['mismatches']:
                print(f"\nKey: {item['key']}", file=output)
                print(f"  File 1: {item['file1_path']}", file=output)
                print(f"    Hash: {item['file1_hash']}", file=output)
                print(f"  File 2: {item['file2_path']}", file=output)
                print(f"    Hash: {item['file2_hash']}", file=output)
        
        if results['file1_only']:
            print("\n" + "=" * 80, file=output)
            print("FILE 1 ONLY (Not in File 2)", file=output)
            print("=" * 80, file=output)
            for item in results['file1_only'][:20]:  # Limit to first 20
                print(f"Key: {item['key']} - {item['path']}", file=output)
            if len(results['file1_only']) > 20:
                print(f"... and {len(results['file1_only']) - 20} more", file=output)
        
        if results['file2_only']:
            print("\n" + "=" * 80, file=output)
            print("FILE 2 ONLY (Not in File 1)", file=output)
            print("=" * 80, file=output)
            for item in results['file2_only'][:20]:  # Limit to first 20
                print(f"Key: {item['key']} - {item['path']}", file=output)
            if len(results['file2_only']) > 20:
                print(f"... and {len(results['file2_only']) - 20} more", file=output)
        
        if results['matches']:
            print("\n" + "=" * 80, file=output)
            print(f"MATCHES ({len(results['matches'])} entries)", file=output)
            print("=" * 80, file=output)
            print("(Use --verbose to see all matches)", file=output)
    
    finally:
        if output_file is not None:
            output.close()


def main():
    parser = argparse.ArgumentParser(
        description="Compare two hash files with different naming conventions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
File formats:
  mosaic:   mosaic_XXX_image_YYY_spectral_ZZZZ.nii
  rawdata:  rawdata/sub-XXX/voi-YYY/sample-sliceZZ/acq-WWW/...chunk-AAAA_OCT.nii.gz

Conversion rules:
  - mosaic number = sliceid*2-(acq==normal)
  - chunkid == imageid

Examples:
  %(prog)s --file1 hashes1.csv --format1 mosaic --file2 hashes2.csv --format2 rawdata
  %(prog)s --file1 hashes1.csv --format1 mosaic --file2 hashes2.csv --format2 rawdata --output results.txt
        """
    )
    parser.add_argument(
        '--file1',
        required=True,
        type=Path,
        help='First hash file (CSV format)',
    )
    parser.add_argument(
        '--format1',
        required=True,
        choices=['mosaic', 'rawdata'],
        help='Format of first file',
    )
    parser.add_argument(
        '--file2',
        required=True,
        type=Path,
        help='Second hash file (CSV format)',
    )
    parser.add_argument(
        '--format2',
        required=True,
        choices=['mosaic', 'rawdata'],
        help='Format of second file',
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Output file for results (default: stdout)',
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show all matches in output',
    )
    
    args = parser.parse_args()
    
    # Validate files exist
    if not args.file1.exists():
        print(f"Error: File 1 does not exist: {args.file1}", file=sys.stderr)
        sys.exit(1)
    
    if not args.file2.exists():
        print(f"Error: File 2 does not exist: {args.file2}", file=sys.stderr)
        sys.exit(1)
    
    # Compare files
    results = compare_hash_files(
        args.file1,
        args.format1,
        args.file2,
        args.format2,
    )
    
    # Print results
    print_results(results, args.output)
    
    # Exit with error code if there are mismatches or missing entries
    if results['mismatches'] or results['file1_only'] or results['file2_only']:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

