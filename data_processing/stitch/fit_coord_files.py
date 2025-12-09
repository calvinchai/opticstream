#!/usr/bin/env python3
"""
CLI script to convert tile coordinate YAML files to stitching configuration format.

This script transforms a simple list-based YAML format (like Normal.yaml) into
a structured format with metadata and tiles sections (like normal_stitching.yaml).
"""

import yaml
from pathlib import Path
from typing import Optional, List
from cyclopts import App

app = App(name="fit_coord_files")


def replace_in_filepath(filepath: str, replacements: dict) -> str:
    """Apply string replacements to a filepath."""
    result = filepath
    for old, new in replacements.items():
        if not old:
            continue
        result = result.replace(old, new)
    return result


def convert_yaml_format(
    input_path: str,
    output_path: str,
    filepath_replacements: Optional[dict] = None,
    base_dir: Optional[str] = None,
    mask: Optional[str] = None,
    cropx: Optional[int] = None,
    cropy: Optional[int] = None,
    scan_resolution: Optional[List[float]] = None,
):
    """
    Convert YAML format from simple list to structured format with metadata.
    
    Parameters
    ----------
    input_path : str
        Path to input YAML file (e.g., Normal.yaml)
    output_path : str
        Path to output YAML file (e.g., normal_stitching.yaml)
    filepath_replacements : dict, optional
        Dictionary of string replacements to apply to filepaths
        Keys: old strings, Values: new strings
    base_dir : str, optional
        Base directory path for metadata
    mask : str, optional
        Mask path for metadata
    cropx : int, optional
        Crop X value for metadata
    cropy : int, optional
        Crop Y value for metadata
    scan_resolution : List[float], optional
        Scan resolution array for metadata
    """
    # Load input YAML
    with open(input_path, 'r') as f:
        input_data = yaml.safe_load(f)
    
    # Handle both list format and dict format
    if isinstance(input_data, list):
        tiles = input_data
    elif isinstance(input_data, dict) and 'tiles' in input_data:
        tiles = input_data['tiles']
    else:
        raise ValueError(f"Unexpected input format. Expected list or dict with 'tiles' key.")
    
    # Apply filepath replacements
    if filepath_replacements:
        for tile in tiles:
            if 'filepath' in tile:
                tile['filepath'] = replace_in_filepath(tile['filepath'], filepath_replacements)
    
    # Build output structure
    output_data = {}
    
    # Add metadata section if any metadata fields are provided
    metadata = {}
    if base_dir is not None:
        metadata['base_dir'] = base_dir
    if mask is not None:
        metadata['mask'] = mask
    if cropx is not None:
        metadata['cropx'] = cropx
    if cropy is not None:
        metadata['cropy'] = cropy
    if scan_resolution is not None:
        metadata['scan_resolution'] = scan_resolution
    
    if metadata:
        output_data['metadata'] = metadata
    
    # Add tiles
    output_data['tiles'] = tiles
    
    # Write output YAML
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)
    
    print(f"Successfully converted {input_path} to {output_path}")
    print(f"  - Processed {len(tiles)} tiles")
    if filepath_replacements:
        print(f"  - Applied {len(filepath_replacements)} filepath replacements")
    if metadata:
        print(f"  - Added {len(metadata)} metadata fields")


@app.default
def main(
    input: str,
    output: str,
    replace: Optional[List[str]] = None,
    base_dir: Optional[str] = None,
    mask: Optional[str] = None,
    cropx: Optional[int] = None,
    cropy: Optional[int] = None,
    scan_resolution: Optional[List[float]] = None,
):
    """
    Convert tile coordinate YAML files to stitching configuration format.
    
    Parameters
    ----------
    input : str
        Path to input YAML file (e.g., Normal.yaml)
    output : str
        Path to output YAML file (e.g., normal_stitching.yaml)
    replace : List[str], optional
        String replacements for filepaths in format "old:new"
        Multiple replacements can be specified (e.g., --replace "aip:processed" --replace "mosaic_001:mosaic_002")
    base_dir : str, optional
        Base directory path to add to metadata
    mask : str, optional
        Mask path to add to metadata
    cropx : int, optional
        Crop X value to add to metadata
    cropy : int, optional
        Crop Y value to add to metadata
    scan_resolution : List[float], optional
        Scan resolution array to add to metadata (e.g., --scan-resolution 0.5 0.5)
    """
    # Parse filepath replacements
    filepath_replacements = {}
    if replace:
        for replacement in replace:
            if ':' not in replacement:
                raise ValueError(f"Replacement must be in format 'old:new', got: {replacement}")
            old_str, new_str = replacement.split(':', 1)
            filepath_replacements[old_str] = new_str
    
    # Convert
    convert_yaml_format(
        input_path=input,
        output_path=output,
        filepath_replacements=filepath_replacements if filepath_replacements else None,
        base_dir=base_dir,
        mask=mask,
        cropx=cropx,
        cropy=cropy,
        scan_resolution=scan_resolution,
    )


if __name__ == "__main__":
    app()

