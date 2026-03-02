#!/usr/bin/env python3
"""
Find the surface of a 3D volume along the z-axis using edge detection.

This script applies edge detection to find the surface position (z-index) for each (x,y) position
in a 3D volume. The algorithm:
1. Optionally applies Gaussian filtering to smooth the volume
2. Computes derivatives along the z-axis
3. Finds the z-index with maximum derivative for each (x,y)
4. Optionally applies median filtering to smooth the surface map
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import nibabel as nib
import numpy as np
import yaml
from cyclopts import App
from scipy.ndimage import gaussian_filter, median_filter

app = App(name="find_volume_surface")


def load_volume(input_path: str) -> Tuple[
    np.ndarray, Optional[nib.Nifti1Image], Optional[np.ndarray]]:
    """
    Load 3D volume from NIfTI file.
    
    Parameters
    ----------
    input_path : str
        Path to input NIfTI file
        
    Returns
    -------
    data : np.ndarray
        3D volume data as numpy array
    nifti_img : Optional[nib.Nifti1Image]
        Nifti image object
    affine : Optional[np.ndarray]
        Affine transformation matrix
    """
    input_path_obj = Path(input_path)
    suffix = input_path_obj.suffix.lower()
    
    if suffix not in ['.nii', '.nii.gz', '.nifti']:
        raise ValueError(f"Unsupported file format: {suffix}. Only NIfTI files (.nii, .nii.gz) are supported.")
    
    # Load nifti file
    nifti_img = nib.load(input_path)
    data = np.asarray(nifti_img.dataobj)
    
    if data.ndim != 3:
        raise ValueError(f"Expected 3D volume, but got {data.ndim}D array with shape {data.shape}")
    
    affine = nifti_img.affine
    return data, nifti_img, affine


def load_focus_file(input_path: str) -> np.ndarray:
    """
    Load 2D focus file from NIfTI file.
    
    Supports 2D arrays or 3D arrays where one dimension has size 1 (will be squeezed).
    
    Parameters
    ----------
    input_path : str
        Path to input NIfTI file containing 2D focus map (or 3D with singleton dimension)
        
    Returns
    -------
    data : np.ndarray
        2D focus map as numpy array, containing z-shift values for each (x, y) position
    """
    input_path_obj = Path(input_path)
    suffix = input_path_obj.suffix.lower()
    
    if suffix not in ['.nii', '.nii.gz', '.nifti']:
        raise ValueError(f"Unsupported file format: {suffix}. Only NIfTI files (.nii, .nii.gz) are supported.")
    
    # Load nifti file
    nifti_img = nib.load(input_path)
    data = np.asarray(nifti_img.dataobj)
    
    # Squeeze out singleton dimensions (e.g., 3D with shape (x, y, 1) -> 2D with shape (x, y))
    data = np.squeeze(data)
    
    if data.ndim != 2:
        raise ValueError(
            f"Expected 2D focus map (or 3D with one dimension of size 1), "
            f"but got {data.ndim}D array with shape {data.shape} after squeezing")
    
    return data


def find_surface(
    volume: np.ndarray,
    sigma: float = 0.0,
    median_size: int = 0,
    crop_threshold: Optional[float] = None,
    crop_n_pixels: int = 0
) -> np.ndarray:
    """
    Find surface z-indices for each (x, y) position using edge detection.
    
    Parameters
    ----------
    volume : np.ndarray
        3D volume array with shape (x, y, z)
    sigma : float, default=0.0
        Standard deviation for Gaussian filter. If 0, no filtering is applied.
    median_size : int, default=0
        Size of median filter kernel. If 0, no filtering is applied.
        Must be an odd integer if > 0.
    crop_threshold : Optional[float], default=None
        Threshold value for detecting cropped surfaces. If None or crop_n_pixels is 0,
        this check is disabled. If the first crop_n_pixels along z have values > threshold,
        the surface index is set to 0 for that z-line.
    crop_n_pixels : int, default=0
        Number of consecutive pixels to check from z=0. If 0, crop detection is disabled.
        
    Returns
    -------
    surface : np.ndarray
        2D array of surface z-indices, shape (x, y), dtype int32
    """
    if volume.ndim != 3:
        raise ValueError(f"Expected 3D volume, but got {volume.ndim}D array")
    
    # Apply Gaussian filter if sigma > 0
    if sigma > 0:
        print(f"Applying Gaussian filter with sigma={sigma}...")
        filtered_volume = gaussian_filter(volume, sigma=sigma)
    else:
        filtered_volume = volume
    
    # Compute gradient along z-axis (axis=2)
    print("Computing derivatives along z-axis...")
    grad_z = np.gradient(filtered_volume, axis=2)
    
    # Find the z-index with maximum derivative for each (x, y)
    print("Finding surface indices (argmax of derivatives)...")
    surface = np.argmax(grad_z, axis=2).astype(np.int32)
    
    # Check for cropped surfaces: if first N pixels have values > threshold, set surface to 0
    if crop_n_pixels > 0 and crop_threshold is not None:
        if crop_n_pixels > volume.shape[2]:
            raise ValueError(
                f"crop_n_pixels ({crop_n_pixels}) exceeds volume z-dimension ({volume.shape[2]})")
        print(f"Checking for cropped surfaces (threshold={crop_threshold}, n_pixels={crop_n_pixels})...")
        
        # For each (x, y), check if all first n_pixels have values > threshold
        # Shape: (x, y, n_pixels)
        first_n_slices = filtered_volume[:, :, :crop_n_pixels]
        
        # Check if all n_pixels are above threshold for each (x, y)
        # Shape: (x, y) - True if all n_pixels are above threshold
        all_above_threshold = np.all(first_n_slices > crop_threshold, axis=2)
        
        # Set surface to 0 for positions where all first n_pixels are above threshold
        num_cropped = np.sum(all_above_threshold)
        if num_cropped > 0:
            print(f"  Found {num_cropped} z-lines with cropped surfaces (set to z=0)")
            surface[all_above_threshold] = 0
    
    # Apply median filter if median_size > 0
    if median_size > 0:
        if median_size % 2 == 0:
            raise ValueError(f"median_size must be an odd integer, got {median_size}")
        print(f"Applying median filter with size={median_size}...")
        surface = median_filter(surface, size=median_size, mode='reflect')
        # Convert back to int32 after median filter (which may return float)
        surface = surface.astype(np.int32)
    
    return surface


def draw_planes_on_volume(
    volume: np.ndarray,
    surface: np.ndarray,
    thickness: int = 1,
    value: float = 0.0
) -> np.ndarray:
    """
    Draw planes in a 3D volume by setting values at z-indices specified in the surface map.
    This creates an overlap image for quality control.
    
    Parameters
    ----------
    volume : np.ndarray
        3D volume array with shape (x, y, z) - will be copied, not modified in place
    surface : np.ndarray
        2D surface z-index map with shape (x, y), where each value is a z-axis index
    thickness : int, default=1
        Thickness of planes (number of z-slices to set)
    value : float, default=0.0
        Value to set at plane locations
        
    Returns
    -------
    volume_overlap : np.ndarray
        Copy of volume with planes drawn (original volume is not modified)
    """
    # Validate shapes
    if surface.shape != volume.shape[:2]:
        raise ValueError(
            f"Shape mismatch: surface shape {surface.shape} does not match volume x,y dimensions {volume.shape[:2]}")
    
    if thickness < 1:
        raise ValueError(f"Thickness must be >= 1, got {thickness}")
    
    # Create a copy of the volume to avoid modifying the original
    volume_overlap = volume.copy()
    
    # Convert value to volume's dtype
    value_dtype = np.array([value]).astype(volume.dtype)[0]
    
    # Convert surface to integer indices, handling non-integer values
    surface_int = np.round(surface).astype(np.int32)
    
    # Clip z-indices to valid range
    surface_int = np.clip(surface_int, 0, volume.shape[2] - 1)
    
    # For each (x, y) position, set the z-range
    for x in range(surface_int.shape[0]):
        for y in range(surface_int.shape[1]):
            z_idx = surface_int[x, y]
            
            # Calculate z-range for plane thickness
            z_start = max(0, z_idx - thickness // 2)
            z_end = min(volume.shape[2], z_idx + thickness // 2 + 1)
            
            # Set values in the plane
            volume_overlap[x, y, z_start:z_end] = value_dtype
    
    return volume_overlap


def save_surface(
    surface: np.ndarray,
    output_path: str,
    nifti_img: Optional[nib.Nifti1Image] = None,
    affine: Optional[np.ndarray] = None
):
    """
    Save 2D surface z-index map to NIfTI file, preserving original affine and header.
    
    Parameters
    ----------
    surface : np.ndarray
        2D surface z-index map array
    output_path : str
        Output file path
    nifti_img : Optional[nib.Nifti1Image]
        Original nifti image object (for preserving header)
    affine : Optional[np.ndarray]
        Affine transformation matrix
    """
    output_path_obj = Path(output_path)
    suffix = output_path_obj.suffix.lower()
    
    if suffix not in ['.nii', '.nii.gz', '.nifti']:
        raise ValueError(f"Unsupported output format: {suffix}. Only NIfTI files (.nii, .nii.gz) are supported.")
    
    if surface.ndim != 2:
        raise ValueError(f"Expected 2D surface map, but got {surface.ndim}D array with shape {surface.shape}")
    
    # Save as nifti
    if nifti_img is not None and affine is not None:
        # Use original nifti header and affine
        # For 2D output, we need to modify the affine to account for the missing z-dimension
        # Extract the x,y part of the affine (first 2x2 block and translation)
        surface_affine = affine.copy()
        # Keep the x,y dimensions, set z to identity
        surface_img = nib.Nifti1Image(surface, surface_affine, nifti_img.header)
    elif affine is not None:
        surface_affine = affine.copy()
        surface_img = nib.Nifti1Image(surface, surface_affine)
    else:
        # Create default affine
        surface_img = nib.Nifti1Image(surface, np.eye(4))
    
    nib.save(surface_img, output_path)


def save_volume(
    volume: np.ndarray,
    output_path: str,
    nifti_img: Optional[nib.Nifti1Image] = None,
    affine: Optional[np.ndarray] = None
):
    """
    Save volume to NIfTI file, preserving original affine and header.
    
    Parameters
    ----------
    volume : np.ndarray
        3D volume array to save
    output_path : str
        Output file path
    nifti_img : Optional[nib.Nifti1Image]
        Original nifti image object (for preserving header)
    affine : Optional[np.ndarray]
        Affine transformation matrix
    """
    output_path_obj = Path(output_path)
    suffix = output_path_obj.suffix.lower()
    
    if suffix not in ['.nii', '.nii.gz', '.nifti']:
        raise ValueError(f"Unsupported output format: {suffix}. Only NIfTI files (.nii, .nii.gz) are supported.")
    
    # Save as nifti
    if nifti_img is not None and affine is not None:
        # Use original nifti header and affine
        volume_img = nib.Nifti1Image(volume, affine, nifti_img.header)
    elif affine is not None:
        volume_img = nib.Nifti1Image(volume, affine)
    else:
        # Create default affine
        volume_img = nib.Nifti1Image(volume, np.eye(4))
    
    nib.save(volume_img, output_path)


def load_yaml_config(yaml_path: str) -> Tuple[Dict, List[Dict]]:
    """Load tile configuration from YAML file."""
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    metadata = data.get('metadata', {})
    tiles = data.get('tiles', [])
    
    return metadata, tiles


def replace_filepath_postfix(filepath: str, postfix_old: str, postfix_new: str) -> str:
    """
    Replace postfix in filepath.
    
    Parameters
    ----------
    filepath : str
        Original filepath (e.g., "mosaic_001_tile_0028_dBI.nii")
    postfix_old : str
        Old postfix to replace (e.g., "_dBI")
    postfix_new : str
        New postfix (e.g., "_surf")
    
    Returns
    -------
    new_filepath : str
        Filepath with replaced postfix (e.g., "mosaic_001_tile_0028_surf.nii")
    """
    path_obj = Path(filepath)
    
    # Handle .nii.gz case - need to get stem before .nii
    if filepath.endswith('.nii.gz'):
        # For .nii.gz, stem is the part before .nii
        stem_with_nii = path_obj.stem  # This gives us the part before .gz
        # stem_with_nii might be "filename.nii" or just "filename" depending on Path behavior
        if stem_with_nii.endswith('.nii'):
            stem = stem_with_nii[:-4]  # Remove .nii
        else:
            stem = stem_with_nii
        suffix = '.nii.gz'
    else:
        stem = path_obj.stem
        suffix = path_obj.suffix
    
    # Replace postfix in stem
    if postfix_old in stem:
        new_stem = stem.replace(postfix_old, postfix_new)
    else:
        # If postfix not found, append new postfix before extension
        new_stem = stem + postfix_new
    
    return new_stem + suffix


@app.default
def main(
    input: str,
    output: str,
    sigma: float = 0.0,
    median_size: int = 0,
    crop_threshold: Optional[float] = None,
    crop_n_pixels: int = 0,
    overlap_output: Optional[str] = None,
    overlap_thickness: int = 1,
    overlap_value: float = 0.0,
    focus_file: Optional[str] = None,
):
    """
    Find the surface of a 3D volume along the z-axis using edge detection.
    
    Parameters
    ----------
    input : str
        Path to input 3D volume NIfTI file
    output : str
        Path to output 2D surface z-index map NIfTI file
    sigma : float, default=0.0
        Standard deviation for Gaussian filter. If 0, no filtering is applied.
    median_size : int, default=0
        Size of median filter kernel. If 0, no filtering is applied.
        Must be an odd integer if > 0.
    crop_threshold : Optional[float], default=None
        Threshold value for detecting cropped surfaces. If None or crop_n_pixels is 0,
        this check is disabled. If the first crop_n_pixels along z have values > threshold,
        the surface index is set to 0 for that z-line.
    crop_n_pixels : int, default=0
        Number of consecutive pixels to check from z=0. If 0, crop detection is disabled.
    overlap_output : Optional[str], default=None
        Optional path to output overlap/quality control image. If provided, creates a 3D volume
        with the detected surface drawn as planes for visualization. Uses draw_planes functionality.
    overlap_thickness : int, default=1
        Thickness of planes in the overlap image (number of z-slices to set).
    overlap_value : float, default=0.0
        Value to set at plane locations in the overlap image (e.g., 0, 255, or any numeric value).
    focus_file : Optional[str], default=None
        Optional path to 2D NIfTI focus file containing z-shift values for each (x,y) position.
        If provided, the focus values are added to the detected surface to account for z-axis shifts
        in the input volume. The focus file represents the shift that was applied to the volume,
        so adding it to the detected surface gives the true surface position.
    """
    # Load volume
    print(f"Loading 3D volume from {input}...")
    vol_data, nifti_img, affine = load_volume(input)
    print(f"  Shape: {vol_data.shape}, dtype: {vol_data.dtype}, range: [{vol_data.min():.2f}, {vol_data.max():.2f}]")
    
    # Load focus file if provided
    focus_data = None
    if focus_file is not None:
        print(f"Loading focus file from {focus_file}...")
        focus_data = load_focus_file(focus_file)
        print(f"  Focus shape: {focus_data.shape}, dtype: {focus_data.dtype}, range: [{focus_data.min():.2f}, {focus_data.max():.2f}]")
        
        # Validate that focus file dimensions match volume (x,y) dimensions
        if focus_data.shape != vol_data.shape[:2]:
            raise ValueError(
                f"Dimension mismatch: focus file shape {focus_data.shape} does not match "
                f"volume x,y dimensions {vol_data.shape[:2]}")
    
    # Find surface
    print("Finding surface using edge detection...")
    surface = find_surface(
        vol_data,
        sigma=sigma,
        median_size=median_size,
        crop_threshold=crop_threshold,
        crop_n_pixels=crop_n_pixels
    )
    print(f"  Surface shape: {surface.shape}, dtype: {surface.dtype}")
    print(f"  Z-index range: [{surface.min()}, {surface.max()}]")
    
    # Apply focus shift if focus file was provided
    if focus_data is not None:
        print("Applying focus shift to surface...")
        # Convert surface to float for addition, then convert back to int32
        surface = (surface.astype(np.float32) + focus_data.astype(np.float32)).astype(np.int32)
        print(f"  Shifted surface z-index range: [{surface.min()}, {surface.max()}]")
    
    # Save surface map
    print(f"Saving surface map to {output}...")
    save_surface(surface, output, nifti_img, affine)
    
    # Create overlap image for quality control if requested
    if overlap_output is not None:
        print(f"Creating overlap image for quality control...")
        print(f"  Drawing planes with thickness={overlap_thickness}, value={overlap_value}...")
        overlap_volume = draw_planes_on_volume(
            vol_data,
            surface,
            thickness=overlap_thickness,
            value=overlap_value
        )
        print(f"Saving overlap image to {overlap_output}...")
        save_volume(overlap_volume, overlap_output, nifti_img, affine)
        num_planes = surface.size
        estimated_voxels = num_planes * overlap_thickness
        print(f"  Estimated {estimated_voxels} voxels modified across {num_planes} plane locations")
    
    print("Done!")


@app.command
def batch(
    yaml_path: str,
    output_dir: str,
    output_yaml: Optional[str] = None,
    postfix_old: str = "_dBI",
    postfix_new: str = "_surf",
    sigma: float = 0.0,
    median_size: int = 0,
    crop_threshold: Optional[float] = None,
    crop_n_pixels: int = 0,
    focus_file: Optional[str] = None,
):
    """
    Batch process multiple tiles from a YAML configuration file to find surfaces.
    
    Parameters
    ----------
    yaml_path : str
        Path to input YAML file with tile configuration
    output_dir : str
        Directory to save all surface files
    output_yaml : Optional[str], default=None
        Path to output YAML file. If None, defaults to {input_yaml}_surf.yaml
    postfix_old : str, default="_dBI"
        Old postfix to replace in filenames (e.g., "_dBI")
    postfix_new : str, default="_surf"
        New postfix for output filenames (e.g., "_surf")
    sigma : float, default=0.0
        Standard deviation for Gaussian filter. If 0, no filtering is applied.
    median_size : int, default=0
        Size of median filter kernel. If 0, no filtering is applied.
        Must be an odd integer if > 0.
    crop_threshold : Optional[float], default=None
        Threshold value for detecting cropped surfaces. If None or crop_n_pixels is 0,
        this check is disabled. If the first crop_n_pixels along z have values > threshold,
        the surface index is set to 0 for that z-line.
    crop_n_pixels : int, default=0
        Number of consecutive pixels to check from z=0. If 0, crop detection is disabled.
    focus_file : Optional[str], default=None
        Optional path to 2D NIfTI focus file containing z-shift values for each (x,y) position.
        If provided, the focus values are added to the detected surface for all tiles.
    """
    # Load YAML configuration
    print(f"Loading tile configuration from {yaml_path}...")
    metadata, tiles = load_yaml_config(yaml_path)
    print(f"  Loaded {len(tiles)} tiles")
    
    # Get base directory
    base_dir = metadata.get('base_dir', '.')
    print(f"  Base directory: {base_dir}")
    
    # Create output directory
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    print(f"  Output directory: {output_dir}")
    
    # Load focus file if provided
    focus_data = None
    if focus_file is not None:
        print(f"Loading focus file from {focus_file}...")
        focus_data = load_focus_file(focus_file)
        print(f"  Focus shape: {focus_data.shape}, dtype: {focus_data.dtype}, range: [{focus_data.min():.2f}, {focus_data.max():.2f}]")
    
    # Process each tile
    processed_tiles = []
    successful_count = 0
    failed_count = 0
    
    print(f"\nProcessing {len(tiles)} tiles...")
    for i, tile in enumerate(tiles, 1):
        filepath = tile.get('filepath')
        if not filepath:
            print(f"  [{i}/{len(tiles)}] Skipping tile {tile.get('tile_number', 'unknown')}: no filepath")
            failed_count += 1
            continue
        
        tile_number = tile.get('tile_number', i)
        print(f"  [{i}/{len(tiles)}] Processing tile {tile_number}: {filepath}")
        
        # Construct input path
        input_path = Path(base_dir) / filepath
        if not input_path.exists():
            print(f"    Error: Input file not found: {input_path}")
            failed_count += 1
            continue
        
        try:
            # Load volume
            vol_data, nifti_img, affine = load_volume(str(input_path))
            
            # Validate focus file dimensions if provided
            if focus_data is not None:
                if focus_data.shape != vol_data.shape[:2]:
                    print(f"    Warning: Focus file dimensions {focus_data.shape} don't match volume {vol_data.shape[:2]}, skipping focus correction")
                    tile_focus_data = None
                else:
                    tile_focus_data = focus_data
            else:
                tile_focus_data = None
            
            # Find surface
            surface = find_surface(
                vol_data,
                sigma=sigma,
                median_size=median_size,
                crop_threshold=crop_threshold,
                crop_n_pixels=crop_n_pixels
            )
            
            # Apply focus shift if provided
            if tile_focus_data is not None:
                surface = (surface.astype(np.float32) + tile_focus_data.astype(np.float32)).astype(np.int32)
            
            # Generate output filename
            output_filename = replace_filepath_postfix(filepath, postfix_old, postfix_new)
            output_path = output_dir_path / output_filename
            
            # Save surface
            save_surface(surface, str(output_path), nifti_img, affine)
            
            # Update tile entry for output YAML
            new_tile = tile.copy()
            new_tile['filepath'] = output_filename
            processed_tiles.append(new_tile)
            
            successful_count += 1
            if successful_count % 10 == 0:
                print(f"    Processed {successful_count} tiles...")
        
        except Exception as e:
            print(f"    Error processing tile {tile_number}: {e}")
            failed_count += 1
            continue
    
    print(f"\nProcessing complete: {successful_count} successful, {failed_count} failed")
    
    # Export updated YAML file
    if output_yaml is None:
        input_yaml_path = Path(yaml_path)
        output_yaml = str(input_yaml_path.parent / f"{input_yaml_path.stem}_surf{input_yaml_path.suffix}")
    
    print(f"\nExporting updated YAML to {output_yaml}...")
    output_data = {
        'metadata': {
            **metadata,
            'base_dir': str(output_dir_path.absolute())
        },
        'tiles': processed_tiles
    }
    
    output_yaml_path = Path(output_yaml)
    output_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_yaml_path, 'w') as f:
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)
    
    print(f"  Exported {len(processed_tiles)} tiles to {output_yaml}")
    print("Done!")


if __name__ == "__main__":
    app()

