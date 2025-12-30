#!/usr/bin/env python3
"""
Draw planes in a 3D volume based on a 2D z-index map.

This script takes a 3D volume and a 2D map where each value represents a z-axis index,
then draws planes by setting values in the 3D volume to a specified value at those z-indices.
Supports configurable plane thickness.
"""

from pathlib import Path
from typing import Optional, Tuple

import nibabel as nib
import numpy as np
from cyclopts import App

app = App(name="draw_planes")


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


def load_zmap(input_path: str) -> np.ndarray:
    """
    Load 2D z-index map from NIfTI file.
    
    Supports 2D arrays or 3D arrays where one dimension has size 1 (will be squeezed).
    
    Parameters
    ----------
    input_path : str
        Path to input NIfTI file containing 2D z-index map (or 3D with singleton dimension)
        
    Returns
    -------
    data : np.ndarray
        2D z-index map as numpy array
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
            f"Expected 2D z-index map (or 3D with one dimension of size 1), "
            f"but got {data.ndim}D array with shape {data.shape} after squeezing")
    
    return data


def draw_planes(
    volume: np.ndarray,
    zmap: np.ndarray,
    thickness: int = 1,
    value: float = 0.0
) -> np.ndarray:
    """
    Draw planes in a 3D volume by setting values at z-indices specified in the zmap.
    
    Parameters
    ----------
    volume : np.ndarray
        3D volume array with shape (x, y, z)
    zmap : np.ndarray
        2D z-index map with shape (x, y), where each value is a z-axis index
    thickness : int, default=1
        Thickness of planes (number of z-slices to set)
    value : float, default=0.0
        Value to set at plane locations
        
    Returns
    -------
    volume : np.ndarray
        Modified volume with planes drawn (same array, modified in place)
    """
    # Validate shapes
    if zmap.shape != volume.shape[:2]:
        raise ValueError(
            f"Shape mismatch: zmap shape {zmap.shape} does not match volume x,y dimensions {volume.shape[:2]}")
    
    if thickness < 1:
        raise ValueError(f"Thickness must be >= 1, got {thickness}")
    
    # Convert value to volume's dtype
    value_dtype = np.array([value]).astype(volume.dtype)[0]
    
    # Convert zmap to integer indices, handling non-integer values
    zmap_int = np.round(zmap).astype(np.int32)
    
    # Clip z-indices to valid range
    zmap_int = np.clip(zmap_int, 0, volume.shape[2] - 1)
    
    # For each (x, y) position, set the z-range
    for x in range(zmap_int.shape[0]):
        for y in range(zmap_int.shape[1]):
            z_idx = zmap_int[x, y]
            
            # Calculate z-range for plane thickness
            z_start = max(0, z_idx - thickness // 2)
            z_end = min(volume.shape[2], z_idx + thickness // 2 + 1)
            
            # Set values in the plane
            volume[x, y, z_start:z_end] = value_dtype
    
    return volume


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


@app.default
def main(
    volume: str,
    zmap: str,
    output: str,
    thickness: int = 1,
    value: float = 0.0,
    normalize_zmap: bool = False,
):
    """
    Draw planes in a 3D volume based on a 2D z-index map.
    
    Parameters
    ----------
    volume : str
        Path to input 3D volume NIfTI file
    zmap : str
        Path to 2D z-index map NIfTI file
    output : str
        Path to output NIfTI file
    thickness : int, default=1
        Thickness of planes (number of z-slices to set)
    value : float, default=0.0
        Value to set at plane locations (e.g., 0, 255, or any numeric value)
    normalize_zmap : bool, default=False
        If True, normalize zmap by subtracting its minimum value (shifts minimum to 0)
    """
    # Load volume and zmap
    print(f"Loading 3D volume from {volume}...")
    vol_data, nifti_img, affine = load_volume(volume)
    print(f"  Shape: {vol_data.shape}, dtype: {vol_data.dtype}, range: [{vol_data.min():.2f}, {vol_data.max():.2f}]")
    
    print(f"Loading z-index map from {zmap}...")
    zmap_data = load_zmap(zmap)
    print(f"  Shape: {zmap_data.shape}, dtype: {zmap_data.dtype}, range: [{zmap_data.min():.2f}, {zmap_data.max():.2f}]")
    
    # Normalize zmap if requested
    if normalize_zmap:
        zmap_min = zmap_data.min()
        print(f"Normalizing zmap by subtracting minimum value ({zmap_min:.2f})...")
        zmap_data = zmap_data - zmap_min
        print(f"  Normalized range: [{zmap_data.min():.2f}, {zmap_data.max():.2f}]")
    
    # Validate dimensions
    if zmap_data.shape != vol_data.shape[:2]:
        raise ValueError(
            f"Dimension mismatch: zmap shape {zmap_data.shape} does not match volume x,y dimensions {vol_data.shape[:2]}")
    
    # Draw planes
    print(f"Drawing planes with thickness={thickness}, value={value}...")
    draw_planes(vol_data, zmap_data, thickness=thickness, value=value)
    
    # Count how many voxels were modified
    # We can't easily count this after the fact, but we can estimate based on thickness
    num_planes = np.sum(~np.isnan(zmap_data)) if zmap_data.dtype == np.float64 else zmap_data.size
    estimated_voxels = num_planes * thickness
    print(f"  Estimated {estimated_voxels} voxels modified across {num_planes} plane locations")
    
    # Save output
    print(f"Saving modified volume to {output}...")
    save_volume(vol_data, output, nifti_img, affine)
    print("Done!")


if __name__ == "__main__":
    app()

