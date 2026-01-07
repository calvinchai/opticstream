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
from typing import Optional, Tuple

import nibabel as nib
import numpy as np
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
    """
    # Load volume
    print(f"Loading 3D volume from {input}...")
    vol_data, nifti_img, affine = load_volume(input)
    print(f"  Shape: {vol_data.shape}, dtype: {vol_data.dtype}, range: [{vol_data.min():.2f}, {vol_data.max():.2f}]")
    
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


if __name__ == "__main__":
    app()

