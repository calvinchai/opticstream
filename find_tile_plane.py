#!/usr/bin/env python3
"""
Find the plane that's being added to each tile's surface values.

When tiles are stitched together, overlapping regions should have similar values.
However, there's a plane being added to the surface value (same plane added to each tile,
the plane has the same size as each tile). This script finds that plane and exports it
as a NIfTI volume.

Important: Tile dimensions interpretation
- tile_size is always (width, height) = (x_size, y_size)
- For example, tile_size = (200, 350) means:
  * width = 200 pixels (added to x coordinate)
  * height = 350 pixels (added to y coordinate)
- When a tile has origin (x, y), its extent is:
  * x: [x, x + width) = [x, x + 200)
  * y: [y, y + height) = [y, y + 350)

The approach:
1. Load surface map NIfTI files for each tile
2. Find overlapping regions between adjacent tiles
3. Compare surface values in overlapping regions
4. Fit a plane that explains the differences: surf1 - surf2 = plane(x2,y2) - plane(x1,y1)
   - Note: This only determines coefficients a, b, and d (x, y gradients, and xy interaction)
   - The constant term c cancels out in differences and must be determined separately
   - Plane equation: signal = a*x + b*y + d*x*y + c
5. Determine constant term c from all surface values
6. Create a plane volume with the same size as each tile
7. Export the plane as a NIfTI volume
8. Verify that subtracting the plane makes overlapping regions similar
"""

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import nibabel as nib
import numpy as np
import yaml
from scipy.optimize import least_squares

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not available, plotting will be disabled")


def load_yaml_config(yaml_path: str) -> Tuple[Dict, List[Dict]]:
    """Load tile configuration from YAML file."""
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    metadata = data.get('metadata', {})
    tiles = data.get('tiles', [])
    
    return metadata, tiles


def load_surface_nifti(filepath: str, base_dir: str, crop_x: int = 0) -> Optional[np.ndarray]:
    """
    Load surface map from NIfTI file.
    
    Parameters
    ----------
    filepath : str
        Path to surface map file (relative to base_dir)
    base_dir : str
        Base directory for surface map files
    crop_x : int
        Number of pixels to crop from the start of x dimension (default: 0)
        If crop_x > 0, surface is cropped as surface[crop_x:, :]
    
    Returns
    -------
    surface : np.ndarray or None
        2D surface map array indexed as [x, y], shape = (width, height)
        If crop_x > 0, the width is reduced by crop_x
    """
    full_path = Path(base_dir) / filepath
    if not full_path.exists():
        print(f"Warning: Surface map not found: {full_path}")
        return None
    
    try:
        img = nib.load(str(full_path))
        surface = np.array(img.dataobj)
        # Surface maps are 2D arrays indexed as [x, y], so shape = (width, height) = (x_size, y_size)
        if surface.ndim == 3:
            surface = surface.squeeze()
        
        # Crop x dimension if requested
        if crop_x > 0:
            if crop_x >= surface.shape[0]:
                print(f"Warning: crop_x ({crop_x}) >= surface width ({surface.shape[0]}), skipping crop")
            else:
                surface = surface[crop_x:, :]
        
        return surface
    except Exception as e:
        print(f"Error loading {full_path}: {e}")
        return None


def find_tile_overlap(
    tile1: Dict, tile2: Dict, tile_size: Tuple[int, int] = (512, 512), crop_x: int = 0
) -> Optional[Tuple]:
    """
    Determine if two tiles overlap and return overlap region.
    
    Parameters
    ----------
    tile_size : tuple
        (width, height) = (x_size, y_size) in pixels
        e.g., (200, 350) means width=200 (added to x), height=350 (added to y)
    crop_x : int
        Number of pixels cropped from x dimension. The tile's x coordinate is adjusted by adding crop_x.
    
    Returns:
        (x1_start, x1_end, y1_start, y1_end, x2_start, x2_end, y2_start, y2_end)
        or None if no overlap
    """
    # Adjust x coordinates to account for cropping
    # When we crop crop_x pixels from the start, the effective x coordinate increases by crop_x
    x1, y1 = tile1['x'] + crop_x, tile1['y']
    x2, y2 = tile2['x'] + crop_x, tile2['y']
    
    # tile_size is (width, height) = (x_size, y_size)
    # width is added to x coordinate, height is added to y coordinate
    w, h = tile_size  # w = width (x dimension), h = height (y dimension)
    
    # Tile bounds in mosaic space
    x1_min, x1_max = x1, x1 + w
    y1_min, y1_max = y1, y1 + h
    x2_min, x2_max = x2, x2 + w
    y2_min, y2_max = y2, y2 + h
    
    # Find overlap
    overlap_x_min = max(x1_min, x2_min)
    overlap_x_max = min(x1_max, x2_max)
    overlap_y_min = max(y1_min, y2_min)
    overlap_y_max = min(y1_max, y2_max)
    
    if overlap_x_min >= overlap_x_max or overlap_y_min >= overlap_y_max:
        return None  # No overlap
    
    # Convert to local tile coordinates
    x1_local_start = overlap_x_min - x1
    x1_local_end = overlap_x_max - x1
    y1_local_start = overlap_y_min - y1
    y1_local_end = overlap_y_max - y1
    
    x2_local_start = overlap_x_min - x2
    x2_local_end = overlap_x_max - x2
    y2_local_start = overlap_y_min - y2
    y2_local_end = overlap_y_max - y2
    
    return (x1_local_start, x1_local_end, y1_local_start, y1_local_end,
            x2_local_start, x2_local_end, y2_local_start, y2_local_end)


def extract_overlap_data(
    surf1: np.ndarray, surf2: np.ndarray,
    overlap: Tuple,
    tile1: Dict, tile2: Dict
) -> Optional[Dict]:
    """
    Extract surface values from overlap region.
    
    Returns:
        Dictionary with overlap surface values and local tile coordinates
    """
    x1_start, x1_end, y1_start, y1_end, x2_start, x2_end, y2_start, y2_end = overlap
    
    # Convert to integer indices
    # Surface arrays are indexed as [x, y], so shape = (width, height) = (x_size, y_size)
    # shape[0] = width (x dimension), shape[1] = height (y dimension)
    x1_start = max(0, int(np.floor(x1_start)))
    x1_end = min(surf1.shape[0], int(np.ceil(x1_end)))  # shape[0] is width (x dimension)
    y1_start = max(0, int(np.floor(y1_start)))
    y1_end = min(surf1.shape[1], int(np.ceil(y1_end)))  # shape[1] is height (y dimension)
    
    x2_start = max(0, int(np.floor(x2_start)))
    x2_end = min(surf2.shape[0], int(np.ceil(x2_end)))  # shape[0] is width (x dimension)
    y2_start = max(0, int(np.floor(y2_start)))
    y2_end = min(surf2.shape[1], int(np.ceil(y2_end)))  # shape[1] is height (y dimension)
    
    # Ensure same size
    x_size = min(x1_end - x1_start, x2_end - x2_start)
    y_size = min(y1_end - y1_start, y2_end - y2_start)
    
    x1_end = x1_start + x_size
    y1_end = y1_start + y_size
    x2_end = x2_start + x_size
    y2_end = y2_start + y_size
    
    # Extract overlap regions (surface arrays are indexed as [x, y])
    surf1_overlap = surf1[x1_start:x1_end, y1_start:y1_end]
    surf2_overlap = surf2[x2_start:x2_end, y2_start:y2_end]
    
    # Create local coordinate arrays (relative to each tile's origin)
    # The plane is a function of local tile coordinates, not mosaic coordinates
    x1_local = np.arange(x1_start, x1_start + x_size)  # local x coordinates in tile1
    y1_local = np.arange(y1_start, y1_start + y_size)  # local y coordinates in tile1
    
    x2_local = np.arange(x2_start, x2_start + x_size)  # local x coordinates in tile2
    y2_local = np.arange(y2_start, y2_start + y_size)  # local y coordinates in tile2
    
    # Create meshgrids for local coordinates
    # With indexing='ij': x1_local_grid[i, j] = x1_local[i], y1_local_grid[i, j] = y1_local[j]
    x1_local_grid, y1_local_grid = np.meshgrid(x1_local, y1_local, indexing='ij')
    x2_local_grid, y2_local_grid = np.meshgrid(x2_local, y2_local, indexing='ij')
    
    # Flatten for easier processing
    surf1_flat = surf1_overlap.flatten()
    surf2_flat = surf2_overlap.flatten()
    x1_flat = x1_local_grid.flatten()
    y1_flat = y1_local_grid.flatten()
    x2_flat = x2_local_grid.flatten()
    y2_flat = y2_local_grid.flatten()
    
    return {
        'surf1': surf1_flat,
        'surf2': surf2_flat,
        'x1': x1_flat,  # local x coordinates in tile1
        'y1': y1_flat,  # local y coordinates in tile1
        'x2': x2_flat,  # local x coordinates in tile2
        'y2': y2_flat   # local y coordinates in tile2
    }


def filter_outliers_iqr(differences: np.ndarray, iqr_factor: float = 1.5) -> np.ndarray:
    """
    Filter outliers using the Interquartile Range (IQR) method.
    
    Parameters
    ----------
    differences : np.ndarray
        Array of differences to filter
    iqr_factor : float
        Factor to multiply IQR by (default: 1.5, typical for outlier detection)
    
    Returns
    -------
    mask : np.ndarray
        Boolean mask where True indicates inlier (non-outlier) values
    """
    q1 = np.percentile(differences, 25)
    q3 = np.percentile(differences, 75)
    iqr = q3 - q1
    
    lower_bound = q1 - iqr_factor * iqr
    upper_bound = q3 + iqr_factor * iqr
    
    mask = (differences >= lower_bound) & (differences <= upper_bound)
    return mask


def filter_outliers_zscore(differences: np.ndarray, z_threshold: float = 3.0) -> np.ndarray:
    """
    Filter outliers using z-score method.
    
    Parameters
    ----------
    differences : np.ndarray
        Array of differences to filter
    z_threshold : float
        Z-score threshold (default: 3.0, values beyond 3 standard deviations are outliers)
    
    Returns
    -------
    mask : np.ndarray
        Boolean mask where True indicates inlier (non-outlier) values
    """
    z_scores = np.abs((differences - np.mean(differences)) / np.std(differences))
    mask = z_scores < z_threshold
    return mask


def filter_overlap_outliers(
    overlap_data_list: List[Dict],
    method: str = 'iqr',
    iqr_factor: float = 1.5,
    z_threshold: float = 3.0
) -> List[Dict]:
    """
    Filter outliers from overlap data based on differences.
    
    Parameters
    ----------
    overlap_data_list : List[Dict]
        List of overlap data dictionaries
    method : str
        Outlier detection method: 'iqr' or 'zscore' (default: 'iqr')
    iqr_factor : float
        IQR factor for IQR method (default: 1.5)
    z_threshold : float
        Z-score threshold for z-score method (default: 3.0)
    
    Returns
    -------
    filtered_overlap_data_list : List[Dict]
        Filtered overlap data with outliers removed
    """
    # First, collect all differences to determine global outlier thresholds
    all_diffs = []
    for overlap_data in overlap_data_list:
        surf1 = overlap_data['surf1']
        surf2 = overlap_data['surf2']
        valid_mask = (surf1 > 0) & (surf2 > 0) & np.isfinite(surf1) & np.isfinite(surf2)
        if np.any(valid_mask):
            diff = surf1[valid_mask] - surf2[valid_mask]
            all_diffs.extend(diff)
    
    if len(all_diffs) == 0:
        return overlap_data_list
    
    all_diffs = np.array(all_diffs)
    
    # Determine outlier thresholds based on all differences
    if method == 'iqr':
        q1 = np.percentile(all_diffs, 25)
        q3 = np.percentile(all_diffs, 75)
        iqr = q3 - q1
        lower_bound = q1 - iqr_factor * iqr
        upper_bound = q3 + iqr_factor * iqr
    elif method == 'zscore':
        mean_diff = np.mean(all_diffs)
        std_diff = np.std(all_diffs)
        lower_bound = mean_diff - z_threshold * std_diff
        upper_bound = mean_diff + z_threshold * std_diff
    else:
        raise ValueError(f"Unknown outlier filtering method: {method}")
    
    # Count outliers
    outlier_mask_global = (all_diffs < lower_bound) | (all_diffs > upper_bound)
    num_outliers = np.sum(outlier_mask_global)
    num_total = len(all_diffs)
    outlier_percent = 100.0 * num_outliers / num_total if num_total > 0 else 0.0
    
    print(f"  Outlier filtering ({method}): {num_outliers}/{num_total} ({outlier_percent:.1f}%) outliers removed")
    print(f"    Threshold: [{lower_bound:.4f}, {upper_bound:.4f}]")
    
    # Now filter each overlap region using the global thresholds
    filtered_list = []
    diff_idx = 0
    
    for overlap_data in overlap_data_list:
        surf1 = overlap_data['surf1']
        surf2 = overlap_data['surf2']
        valid_mask = (surf1 > 0) & (surf2 > 0) & np.isfinite(surf1) & np.isfinite(surf2)
        
        if not np.any(valid_mask):
            continue
        
        # Get differences for this overlap region
        diff = surf1[valid_mask] - surf2[valid_mask]
        n_valid = len(diff)
        
        # Determine which pixels in this region are outliers using global thresholds
        region_inlier_mask = (diff >= lower_bound) & (diff <= upper_bound)
        
        # Create combined mask (valid pixels that are not outliers)
        # Map back to original array indices
        inlier_mask = np.zeros(len(surf1), dtype=bool)
        inlier_mask[valid_mask] = region_inlier_mask
        
        if not np.any(inlier_mask):
            continue  # Skip if all pixels are outliers
        
        # Filter the overlap data
        filtered_data = {
            'surf1': overlap_data['surf1'][inlier_mask],
            'surf2': overlap_data['surf2'][inlier_mask],
            'x1': overlap_data['x1'][inlier_mask],
            'y1': overlap_data['y1'][inlier_mask],
            'x2': overlap_data['x2'][inlier_mask],
            'y2': overlap_data['y2'][inlier_mask]
        }
        
        filtered_list.append(filtered_data)
        diff_idx += n_valid
    
    return filtered_list


def get_plane_params_count(degree: float) -> int:
    """
    Get the number of non-constant parameters for a given degree.
    
    Parameters
    ----------
    degree : float
        Polynomial degree:
        - 1.0: linear (a*x + b*y) -> 2 params
        - 1.5: linear with cross term (a*x + b*y + d*x*y) -> 3 params
        - 2.0: quadratic with xy term (a*x + b*y + d*x^2 + e*y^2 + f*x*y) -> 5 params
        - 2.5: quadratic with xy and x^2y^2 terms (a*x + b*y + d*x^2 + e*y^2 + f*x*y + g*x^2*y^2) -> 6 params
        - 3.0: cubic with x^2y^2 term (a*x + b*y + d*x^2 + e*y^2 + f*x^3 + g*y^3 + h*x^2*y^2) -> 7 params
    
    Returns
    -------
    int
        Number of non-constant parameters (excluding c)
    """
    if degree == 1.0:
        return 2  # a, b
    elif degree == 1.5:
        return 3  # a, b, d
    elif degree == 2.0:
        return 5  # a, b, d, e, f (x, y, x^2, y^2, xy)
    elif degree == 2.5:
        return 6  # a, b, d, e, f, g (x, y, x^2, y^2, xy, x^2y^2)
    elif degree == 3.0:
        return 7  # a, b, d, e, f, g, h (x, y, x^2, y^2, x^3, y^3, x^2y^2)
    else:
        raise ValueError(f"Unsupported degree: {degree}. Supported values: 1.0, 1.5, 2.0, 2.5, 3.0")


def compute_plane_value(params: np.ndarray, x: np.ndarray, y: np.ndarray, degree: float) -> np.ndarray:
    """
    Compute plane value at given coordinates.
    
    Parameters
    ----------
    params : np.ndarray
        Plane parameters (excluding constant c)
    x : np.ndarray
        X coordinates
    y : np.ndarray
        Y coordinates
    degree : float
        Polynomial degree
    
    Returns
    -------
    np.ndarray
        Plane values (without constant term c)
    """
    if degree == 1.0:
        a, b = params
        return a * x + b * y
    elif degree == 1.5:
        a, b, d = params
        return a * x + b * y + d * x * y
    elif degree == 2.0:
        a, b, d, e, f = params
        return a * x + b * y + d * x**2 + e * y**2 + f * x * y
    elif degree == 2.5:
        a, b, d, e, f, g = params
        return a * x + b * y + d * x**2 + e * y**2 + f * x * y + g * x**2 * y**2
    elif degree == 3.0:
        a, b, d, e, f, g, h = params
        return a * x + b * y + d * x**2 + e * y**2 + f * x**3 + g * y**3 + h * x**2 * y**2
    else:
        raise ValueError(f"Unsupported degree: {degree}")


def compute_residuals(params: np.ndarray, overlap_data_list: List[Dict], degree: float = 1.5) -> np.ndarray:
    """
    Compute residuals for plane fitting from overlapping regions.
    
    The plane is a function of local tile coordinates with the specified degree.
    For overlapping pixels at the same physical location:
    - surf1(x1_local, y1_local) - surf2(x2_local, y2_local) = plane(x1_local, y1_local) - plane(x2_local, y2_local)
    
    Note: The constant term c cancels out, so we only fit non-constant parameters from overlaps.
    
    Parameters
    ----------
    params : np.ndarray
        Non-constant plane parameters (excluding c)
    overlap_data_list : List[Dict]
        List of overlap data dictionaries
    degree : float
        Polynomial degree (1.0, 1.5, 2.0, 2.5, 3.0)
    """
    residuals = []
    
    for overlap_data in overlap_data_list:
        surf1 = overlap_data['surf1']
        surf2 = overlap_data['surf2']
        x1 = overlap_data['x1']
        y1 = overlap_data['y1']
        x2 = overlap_data['x2']
        y2 = overlap_data['y2']
        
        # Valid pixels (non-zero, non-NaN)
        valid_mask = (surf1 > 0) & (surf2 > 0) & np.isfinite(surf1) & np.isfinite(surf2)
        
        if not np.any(valid_mask):
            continue
        
        # Extract valid values
        surf1_valid = surf1[valid_mask]
        surf2_valid = surf2[valid_mask]
        x1_valid = x1[valid_mask]
        y1_valid = y1[valid_mask]
        x2_valid = x2[valid_mask]
        y2_valid = y2[valid_mask]
        
        # Compute plane difference: plane(x1_local, y1_local) - plane(x2_local, y2_local)
        # Note: c cancels out, so we only use non-constant parameters
        plane1 = compute_plane_value(params, x1_valid, y1_valid, degree)
        plane2 = compute_plane_value(params, x2_valid, y2_valid, degree)
        plane_diff = plane1 - plane2
        
        # Residual: (surf1 - surf2) - (plane(x1_local,y1_local) - plane(x2_local,y2_local))
        residual = (surf1_valid - surf2_valid) - plane_diff
        residuals.extend(residual)
    
    return np.array(residuals)


def fit_plane_from_overlaps(
    yaml_path: str,
    base_dir: Optional[str] = None,
    tile_size: Optional[Tuple[int, int]] = None,
    subsample: int = 5,
    avg_signal_threshold: Optional[float] = None,
    outlier_method: Optional[str] = 'iqr',
    outlier_iqr_factor: float = 1.5,
    outlier_z_threshold: float = 3.0,
    crop_x: int = 0,
    degree: float = 1.5
) -> Tuple[np.ndarray, Dict]:
    """
    Fit a plane to surface values by comparing overlapping regions.
    
    Returns:
        params: [a, b, d, c] where surface = a*x + b*y + d*x*y + c
        info: Dictionary with fit statistics
    """
    # Load configuration
    metadata, tiles = load_yaml_config(yaml_path)
    
    if base_dir is None:
        base_dir = metadata.get('base_dir', '.')
    
    print(f"Loaded {len(tiles)} tiles from {yaml_path}")
    print(f"Base directory: {base_dir}")
    
    # Filter tiles by avg_signal threshold if provided
    if avg_signal_threshold is not None:
        original_count = len(tiles)
        tiles = [t for t in tiles if 'avg_signal' in t and t['avg_signal'] >= avg_signal_threshold]
        filtered_count = len(tiles)
        print(f"Filtered tiles by avg_signal threshold >= {avg_signal_threshold}: {filtered_count}/{original_count} tiles kept")
        if filtered_count == 0:
            raise ValueError(f"No tiles with avg_signal >= {avg_signal_threshold}")
    
    # Load surface maps
    if crop_x > 0:
        print(f"Loading surface maps with crop_x = {crop_x}...")
    else:
        print("Loading surface maps...")
    tile_data = {}
    for tile in tiles:
        filepath = tile['filepath']
        surface = load_surface_nifti(filepath, base_dir, crop_x=crop_x)
        if surface is not None:
            tile_data[tile['tile_number']] = {
                'surface': surface,
                'tile': tile
            }
            # Determine tile size from first surface map (after cropping)
            # Surface arrays are indexed as [x, y], so shape = (width, height) = (x_size, y_size)
            if tile_size is None:
                w, h = surface.shape[:2]  # w = width (x dimension), h = height (y dimension)
                tile_size = (w, h)  # tile_size = (width, height) = (x_size, y_size)
    
    print(f"Loaded {len(tile_data)} surface maps")
    if tile_size:
        print(f"Tile size (after cropping): {tile_size}")
    
    # Find overlapping regions
    print("Finding overlapping regions...")
    overlap_data_list = []
    tile_list = list(tile_data.values())
    
    for i, tile1_info in enumerate(tile_list):
        for j, tile2_info in enumerate(tile_list[i + 1:], start=i + 1):
            tile1 = tile1_info['tile']
            tile2 = tile2_info['tile']
            
            overlap = find_tile_overlap(tile1, tile2, tile_size, crop_x=crop_x)
            if overlap is None:
                continue
            
            surf1 = tile1_info['surface']
            surf2 = tile2_info['surface']
            
            # Extract overlap data
            overlap_data = extract_overlap_data(surf1, surf2, overlap, tile1, tile2)
            if overlap_data is None:
                continue
            
            # Subsample if requested
            if subsample > 1:
                indices = np.arange(0, len(overlap_data['surf1']), subsample)
                for key in ['surf1', 'surf2', 'x1', 'y1', 'x2', 'y2']:
                    overlap_data[key] = overlap_data[key][indices]
            
            overlap_data_list.append(overlap_data)
    
    print(f"Found {len(overlap_data_list)} overlapping regions")
    
    if len(overlap_data_list) == 0:
        raise ValueError("No overlapping regions found between tiles!")
    
    # Filter outliers if requested
    if outlier_method is not None:
        print(f"\nFiltering outliers using {outlier_method} method...")
        overlap_data_list = filter_overlap_outliers(
            overlap_data_list,
            method=outlier_method,
            iqr_factor=outlier_iqr_factor,
            z_threshold=outlier_z_threshold
        )
        print(f"  Remaining overlap regions after filtering: {len(overlap_data_list)}")
        if len(overlap_data_list) == 0:
            raise ValueError("No overlap data remaining after outlier filtering!")
    
    # Compute and output initial differences before fitting
    print("\nComputing initial differences in overlapping regions...")
    all_diffs_before = []
    for overlap_data in overlap_data_list:
        surf1 = overlap_data['surf1']
        surf2 = overlap_data['surf2']
        valid_mask = (surf1 > 0) & (surf2 > 0) & np.isfinite(surf1) & np.isfinite(surf2)
        if np.any(valid_mask):
            diff = surf1[valid_mask] - surf2[valid_mask]
            all_diffs_before.extend(diff)
    
    if len(all_diffs_before) > 0:
        all_diffs_before = np.array(all_diffs_before)
        print(f"  Mean absolute difference: {np.mean(np.abs(all_diffs_before)):.4f}")
        print(f"  Std of differences: {np.std(all_diffs_before):.4f}")
        print(f"  Max absolute difference: {np.max(np.abs(all_diffs_before)):.4f}")
        print(f"  Min difference: {np.min(all_diffs_before):.4f}")
        print(f"  Max difference: {np.max(all_diffs_before):.4f}")
    
    # Fit plane using least squares
    print(f"Fitting plane (degree {degree}) to overlapping regions...")
    num_params = get_plane_params_count(degree)
    print(f"  Note: Overlapping regions can only determine {num_params} non-constant coefficients.")
    print("  The constant term c cancels out in differences and must be determined separately.")
    
    def residual_func(params):
        return compute_residuals(params, overlap_data_list, degree=degree)
    
    # Initial guess: zeros for all non-constant parameters
    # The constant c cancels out, so we only fit non-constant parameters from overlaps
    initial_params = np.zeros(num_params)
    
    # Use least squares optimization to fit non-constant parameters
    result = least_squares(residual_func, initial_params, method='lm')
    non_const_params = result.x
    
    # Extract parameters based on degree
    if degree == 1.0:
        a, b = non_const_params
        params_dict = {'a': a, 'b': b}
    elif degree == 1.5:
        a, b, d = non_const_params
        params_dict = {'a': a, 'b': b, 'd': d}
    elif degree == 2.0:
        a, b, d, e, f = non_const_params
        params_dict = {'a': a, 'b': b, 'd': d, 'e': e, 'f': f}
    elif degree == 2.5:
        a, b, d, e, f, g = non_const_params
        params_dict = {'a': a, 'b': b, 'd': d, 'e': e, 'f': f, 'g': g}
    elif degree == 3.0:
        a, b, d, e, f, g, h = non_const_params
        params_dict = {'a': a, 'b': b, 'd': d, 'e': e, 'f': f, 'g': g, 'h': h}
    
    print(f"  Fitted coefficients: {', '.join([f'{k} = {v:.6e}' for k, v in params_dict.items()])}")
    
    # Now determine c by using all surface values
    # Since overlaps can't determine c, we use the mean difference between actual
    # surface values and the plane component (a*x + b*y) across all tiles
    print("Determining constant term c from all surface values...")
    
    # Collect all surface values and their coordinates
    all_surfaces = []
    all_x_coords = []
    all_y_coords = []
    
    for tile_info in tile_list:
        tile = tile_info['tile']
        surface = tile_info['surface']
        
        # Get valid surface pixels
        valid_mask = (surface > 0) & np.isfinite(surface)
        if not np.any(valid_mask):
            continue
        
        # Create coordinate arrays
        # Surface arrays are indexed as [x, y], so surface.shape[:2] = (width, height) = (x_size, y_size)
        w, h = surface.shape[:2]  # w = width (x dimension), h = height (y dimension)
        # Create local coordinate grids: x varies from 0 to w-1, y varies from 0 to h-1
        # The plane is a function of local tile coordinates, not mosaic coordinates
        # With indexing='ij': x_local[i, j] = i, y_local[i, j] = j
        x_local, y_local = np.meshgrid(np.arange(w), np.arange(h), indexing='ij')
        
        # Subsample for efficiency
        if subsample > 1:
            mask_subsample = np.zeros_like(valid_mask, dtype=bool)
            mask_subsample[::subsample, ::subsample] = True
            valid_mask = valid_mask & mask_subsample
        
        valid_surface = surface[valid_mask]
        valid_x_local = x_local[valid_mask]  # local x coordinates (0 to w-1)
        valid_y_local = y_local[valid_mask]  # local y coordinates (0 to h-1)
        
        all_surfaces.append(valid_surface)
        all_x_coords.append(valid_x_local)
        all_y_coords.append(valid_y_local)
    
    if len(all_surfaces) > 0:
        all_surfaces = np.concatenate(all_surfaces)
        all_x_coords = np.concatenate(all_x_coords)
        all_y_coords = np.concatenate(all_y_coords)
        
        # Compute plane component (without constant c) for all points
        # Note: This uses local tile coordinates (0 to w-1, 0 to h-1), not mosaic coordinates
        # The plane is a function of local coordinates, identical for each tile
        # Note: This doesn't include c, which we're trying to determine
        plane_component = compute_plane_value(non_const_params, all_x_coords, all_y_coords, degree)
        
        # Determine c such that the mean of (surface - plane_component) is minimized
        # This centers the plane around the mean surface value
        # c = mean(surface - plane_component) ensures that the plane passes through
        # the mean surface value at the mean local coordinates
        residuals_without_c = all_surfaces - plane_component
        c = np.mean(residuals_without_c)
        
        print(f"  Determined c = {c:.6f} from {len(all_surfaces)} surface pixels")
    else:
        # No valid surface pixels found - this should not happen if tiles are loaded correctly
        c = 0.0
        print("  Warning: Could not determine c from surface values, setting to 0")
        print("  This means the plane will pass through the origin in signal space")
    
    # Combine non-constant parameters with constant c
    params = np.concatenate([non_const_params, [c]])
    
    # Compute statistics (use non-constant params for residuals, c doesn't affect overlap differences)
    residuals = compute_residuals(non_const_params, overlap_data_list, degree=degree)
    rmse = np.sqrt(np.mean(residuals ** 2))
    mae = np.mean(np.abs(residuals))
    
    # Output differences after fitting
    print("\nDifferences after plane fitting:")
    print(f"  Mean absolute difference: {mae:.4f}")
    print(f"  Std of differences: {np.std(residuals):.4f}")
    print(f"  Max absolute difference: {np.max(np.abs(residuals)):.4f}")
    print(f"  RMSE: {rmse:.4f}")
    
    info = {
        'rmse': rmse,
        'mae': mae,
        'num_overlaps': len(overlap_data_list),
        'num_pixels': len(residuals),
        'residuals': residuals,
        'tile_size': tile_size,
        'initial_diffs': all_diffs_before if len(all_diffs_before) > 0 else None
    }
    
    return params, info


def create_tile_plane(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    tile_x: float = 0.0,
    tile_y: float = 0.0,
    normalize_min: Optional[float] = None,
    degree: float = 1.5
) -> np.ndarray:
    """
    Create a plane array with the same size as a tile.
    
    Parameters
    ----------
    params : np.ndarray
        Plane parameters (non-constant params + c). Length depends on degree:
        - degree 1.0: [a, b, c]
        - degree 1.5: [a, b, d, c]
        - degree 2.0: [a, b, d, e, c]
        - degree 2.5: [a, b, d, e, f, c]
        - degree 3.0: [a, b, d, e, f, g, c]
    tile_size : tuple
        (width, height) = (x_size, y_size) in pixels
        e.g., (200, 350) means width=200 (added to x), height=350 (added to y)
    tile_x : float
        X coordinate of tile origin in mosaic space (for NIfTI affine, default: 0.0)
        Note: The plane itself is computed using local coordinates (0 to w-1, 0 to h-1)
    tile_y : float
        Y coordinate of tile origin in mosaic space (for NIfTI affine, default: 0.0)
        Note: The plane itself is computed using local coordinates (0 to w-1, 0 to h-1)
    normalize_min : float, optional
        If specified, normalize the plane so its minimum value becomes this value.
        If None, the plane is not normalized (default: None)
    degree : float
        Polynomial degree (1.0, 1.5, 2.0, 2.5, 3.0)
    
    Returns
    -------
    plane : np.ndarray
        2D array with plane values, shape (width, height) = (x_size, y_size) to match surface array indexing [x, y]
    """
    # Extract non-constant parameters and constant c
    num_params = get_plane_params_count(degree)
    non_const_params = params[:num_params]
    c = params[-1]
    
    # tile_size is (width, height) = (x_size, y_size)
    w, h = tile_size  # w = width (x dimension), h = height (y dimension)
    
    # Create local coordinate arrays
    # x_local: [0, 1, 2, ..., w-1] - x coordinates within tile
    # y_local: [0, 1, 2, ..., h-1] - y coordinates within tile
    x_local = np.arange(w)  # width values: 0 to w-1
    y_local = np.arange(h)  # height values: 0 to h-1
    
    # Create meshgrid for local coordinates
    # The plane is a function of local tile coordinates (0 to w-1, 0 to h-1), not mosaic coordinates
    # With indexing='ij': x_local_grid[i, j] = x_local[i], y_local_grid[i, j] = y_local[j]
    # Result shape: (w, h) = (width, height)
    x_local_grid, y_local_grid = np.meshgrid(x_local, y_local, indexing='ij')
    
    # Compute plane values using the appropriate degree
    plane = compute_plane_value(non_const_params, x_local_grid, y_local_grid, degree) + c
    
    # Normalize plane if requested
    if normalize_min is not None:
        plane_min = np.min(plane)
        plane = plane - plane_min + normalize_min
    
    # Plane has shape (w, h) = (width, height) = (x_size, y_size)
    # which matches surface array indexing [x, y]
    return plane


def verify_plane_correction(
    yaml_path: str,
    params: np.ndarray,
    base_dir: Optional[str] = None,
    tile_size: Optional[Tuple[int, int]] = None,
    subsample: int = 1,
    avg_signal_threshold: Optional[float] = None,
    crop_x: int = 0,
    degree: float = 1.5
) -> Dict:
    """
    Verify that after subtracting the plane from each tile, overlapping regions have similar values.
    
    Parameters
    ----------
    yaml_path : str
        Path to YAML file with tile configuration
    params : np.ndarray
        Plane parameters [a, b, d, c] where surface = a*x + b*y + d*x*y + c
    base_dir : str, optional
        Base directory for surface map files. If None, uses metadata['base_dir']
    tile_size : tuple, optional
        Size of each tile (width, height) in pixels. If None, will estimate from surface maps.
    subsample : int
        Subsample factor for overlap regions (use every Nth pixel)
    avg_signal_threshold : float, optional
        Minimum avg_signal value for tiles to be included. If None, all tiles are used.
    
    Returns
    -------
    stats : dict
        Dictionary with verification statistics
    """
    print("\n" + "="*60)
    print("Verifying plane correction...")
    print("="*60)
    
    # Load configuration
    metadata, tiles = load_yaml_config(yaml_path)
    
    if base_dir is None:
        base_dir = metadata.get('base_dir', '.')
    
    # Filter tiles by avg_signal threshold if provided
    if avg_signal_threshold is not None:
        original_count = len(tiles)
        tiles = [t for t in tiles if 'avg_signal' in t and t['avg_signal'] >= avg_signal_threshold]
        filtered_count = len(tiles)
        print(f"Filtered tiles by avg_signal threshold >= {avg_signal_threshold}: {filtered_count}/{original_count} tiles kept")
        if filtered_count == 0:
            raise ValueError(f"No tiles with avg_signal >= {avg_signal_threshold}")
    
    # Load surface maps
    if crop_x > 0:
        print(f"Loading surface maps for verification with crop_x = {crop_x}...")
    else:
        print("Loading surface maps for verification...")
    tile_data = {}
    for tile in tiles:
        filepath = tile['filepath']
        surface = load_surface_nifti(filepath, base_dir, crop_x=crop_x)
        if surface is not None:
            tile_data[tile['tile_number']] = {
                'surface': surface,
                'tile': tile
            }
            # Determine tile size from first surface map (after cropping)
            # Surface arrays are indexed as [x, y], so shape = (width, height) = (x_size, y_size)
            if tile_size is None:
                w, h = surface.shape[:2]  # w = width (x dimension), h = height (y dimension)
                tile_size = (w, h)  # tile_size = (width, height) = (x_size, y_size)
    
    print(f"Loaded {len(tile_data)} surface maps")
    
    # Collect differences in overlapping regions (before and after correction)
    all_differences_before = []
    all_differences_after = []
    overlap_stats = []
    
    tile_list = list(tile_data.values())
    
    for i, tile1_info in enumerate(tile_list):
        for j, tile2_info in enumerate(tile_list[i + 1:], start=i + 1):
            tile1 = tile1_info['tile']
            tile2 = tile2_info['tile']
            
            overlap = find_tile_overlap(tile1, tile2, tile_size, crop_x=crop_x)
            if overlap is None:
                continue
            
            surf1 = tile1_info['surface']
            surf2 = tile2_info['surface']
            
            # Extract overlap data
            overlap_data = extract_overlap_data(surf1, surf2, overlap, tile1, tile2)
            if overlap_data is None:
                continue
            
            # Subsample if requested
            if subsample > 1:
                indices = np.arange(0, len(overlap_data['surf1']), subsample)
                for key in ['surf1', 'surf2', 'x1', 'y1', 'x2', 'y2']:
                    overlap_data[key] = overlap_data[key][indices]
            
            # Get valid pixels
            valid_mask = (overlap_data['surf1'] > 0) & (overlap_data['surf2'] > 0) & \
                        np.isfinite(overlap_data['surf1']) & np.isfinite(overlap_data['surf2'])
            
            if not np.any(valid_mask):
                continue
            
            surf1_valid = overlap_data['surf1'][valid_mask]
            surf2_valid = overlap_data['surf2'][valid_mask]
            x1_valid = overlap_data['x1'][valid_mask]
            y1_valid = overlap_data['y1'][valid_mask]
            x2_valid = overlap_data['x2'][valid_mask]
            y2_valid = overlap_data['y2'][valid_mask]
            
            # Compute plane values at local tile coordinates
            # The plane is a function of local tile coordinates, not mosaic coordinates
            num_params = get_plane_params_count(degree)
            non_const_params = params[:num_params]
            c = params[-1]
            plane1 = compute_plane_value(non_const_params, x1_valid, y1_valid, degree) + c  # plane at tile1 local coords
            plane2 = compute_plane_value(non_const_params, x2_valid, y2_valid, degree) + c  # plane at tile2 local coords
            
            # Corrected surface values (after subtracting plane)
            surf1_corrected = surf1_valid - plane1
            surf2_corrected = surf2_valid - plane2
            
            # Differences before and after correction
            diff_before = surf1_valid - surf2_valid
            diff_after = surf1_corrected - surf2_corrected
            
            all_differences_before.extend(diff_before)
            all_differences_after.extend(diff_after)
            
            # Statistics for this overlap
            overlap_stats.append({
                'tile1': tile1['tile_number'],
                'tile2': tile2['tile_number'],
                'num_pixels': len(diff_before),
                'mean_diff_before': np.mean(np.abs(diff_before)),
                'std_diff_before': np.std(diff_before),
                'max_diff_before': np.max(np.abs(diff_before)),
                'mean_diff_after': np.mean(np.abs(diff_after)),
                'std_diff_after': np.std(diff_after),
                'max_diff_after': np.max(np.abs(diff_after)),
            })
    
    # Convert to numpy arrays for statistics
    all_differences_before = np.array(all_differences_before)
    all_differences_after = np.array(all_differences_after)
    
    # Compute overall statistics
    stats = {
        'num_overlaps': len(overlap_stats),
        'num_pixels': len(all_differences_before),
        'before_correction': {
            'mean_abs_diff': np.mean(np.abs(all_differences_before)),
            'std_diff': np.std(all_differences_before),
            'max_abs_diff': np.max(np.abs(all_differences_before)),
            'rmse': np.sqrt(np.mean(all_differences_before ** 2)),
        },
        'after_correction': {
            'mean_abs_diff': np.mean(np.abs(all_differences_after)),
            'std_diff': np.std(all_differences_after),
            'max_abs_diff': np.max(np.abs(all_differences_after)),
            'rmse': np.sqrt(np.mean(all_differences_after ** 2)),
        },
        'overlap_stats': overlap_stats
    }
    
    # Print results
    print(f"\nVerification Results:")
    print(f"  Number of overlapping regions: {stats['num_overlaps']}")
    print(f"  Total pixels analyzed: {stats['num_pixels']}")
    
    print(f"\n  BEFORE plane correction:")
    print(f"    Mean absolute difference: {stats['before_correction']['mean_abs_diff']:.4f}")
    print(f"    Std of differences:      {stats['before_correction']['std_diff']:.4f}")
    print(f"    Max absolute difference:  {stats['before_correction']['max_abs_diff']:.4f}")
    print(f"    RMSE:                     {stats['before_correction']['rmse']:.4f}")
    
    print(f"\n  AFTER plane correction:")
    print(f"    Mean absolute difference: {stats['after_correction']['mean_abs_diff']:.4f}")
    print(f"    Std of differences:      {stats['after_correction']['std_diff']:.4f}")
    print(f"    Max absolute difference:  {stats['after_correction']['max_abs_diff']:.4f}")
    print(f"    RMSE:                     {stats['after_correction']['rmse']:.4f}")
    
    # Improvement metrics
    improvement_mean = (stats['before_correction']['mean_abs_diff'] - 
                       stats['after_correction']['mean_abs_diff']) / stats['before_correction']['mean_abs_diff'] * 100
    improvement_rmse = (stats['before_correction']['rmse'] - 
                        stats['after_correction']['rmse']) / stats['before_correction']['rmse'] * 100
    
    print(f"\n  Improvement:")
    print(f"    Mean absolute difference: {improvement_mean:.1f}% reduction")
    print(f"    RMSE:                     {improvement_rmse:.1f}% reduction")
    
    # Check if verification passed (threshold-based)
    threshold_mean = 1.0  # Mean absolute difference should be < 1.0 after correction
    threshold_max = 5.0   # Max absolute difference should be < 5.0 after correction
    
    passed = (stats['after_correction']['mean_abs_diff'] < threshold_mean and
              stats['after_correction']['max_abs_diff'] < threshold_max)
    
    stats['verification_passed'] = passed
    stats['thresholds'] = {'mean': threshold_mean, 'max': threshold_max}
    
    if passed:
        print(f"\n  ✓ Verification PASSED: Overlapping regions are similar after correction")
    else:
        print(f"\n  ✗ Verification FAILED: Overlapping regions still differ significantly")
        print(f"    (Thresholds: mean < {threshold_mean}, max < {threshold_max})")
    
    print("="*60)
    
    return stats


def export_plane_to_nifti(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    output_path: str,
    metadata: Dict,
    resolution: Optional[Tuple[float, float]] = None,
    tile_x: float = 0.0,
    tile_y: float = 0.0,
    normalize_min: Optional[float] = None,
    degree: float = 1.5
):
    """
    Export the fitted plane as a NIfTI volume with the same size as a tile.
    
    Parameters
    ----------
    params : np.ndarray
        Plane parameters [a, b, d, c] where surface = a*x + b*y + d*x*y + c
    tile_size : tuple
        Size of tile (width, height) in pixels
    output_path : str
        Path to save the output NIfTI file
    metadata : Dict
        Metadata from YAML file (may contain scan_resolution)
    resolution : tuple, optional
        Resolution (x, y) in mm/pixel. If None, will use metadata or default.
    tile_x : float
        X coordinate of tile in mosaic space (default: 0.0)
    tile_y : float
        Y coordinate of tile in mosaic space (default: 0.0)
    """
    print(f"\nExporting plane to NIfTI file: {output_path}")
    
    # Determine resolution
    if resolution is None:
        if 'scan_resolution' in metadata and len(metadata['scan_resolution']) >= 2:
            resolution = tuple(metadata['scan_resolution'][:2])
            print(f"  Using resolution from metadata: {resolution} mm/pixel")
        else:
            resolution = (0.01, 0.01)  # Default: 0.01 mm/pixel
            print(f"  Using default resolution: {resolution} mm/pixel")
    
    w, h = tile_size
    print(f"  Tile size: {w} x {h} pixels")
    
    # Create plane array
    plane = create_tile_plane(params, tile_size, tile_x, tile_y, normalize_min=normalize_min, degree=degree)
    
    print(f"  Plane shape: {plane.shape}")
    if normalize_min is not None:
        print(f"  Plane normalized: minimum value = {normalize_min:.4f}")
    print(f"  Plane value range: [{np.min(plane):.4f}, {np.max(plane):.4f}]")
    a, b, d, c = params
    print(f"  Plane equation: signal = {a:.6e} * x + {b:.6e} * y + {d:.6e} * x*y + {c:.6f}")
    
    # Set up affine matrix with proper spacing
    affine = np.eye(4)
    affine[0, 0] = resolution[0]  # x spacing in mm
    affine[1, 1] = resolution[1]  # y spacing in mm
    affine[0, 3] = tile_x * resolution[0]  # x origin in mm
    affine[1, 3] = tile_y * resolution[1]  # y origin in mm
    
    # Ensure plane is 3D for NIfTI (add z dimension)
    if plane.ndim == 2:
        plane_3d = plane[:, :, np.newaxis]
    else:
        plane_3d = plane
    
    # Create and save NIfTI image
    plane_img = nib.Nifti1Image(plane_3d.astype(np.float32), affine)
    nib.save(plane_img, output_path)
    
    print(f"  Saved plane to {output_path}")


def save_corrected_surfaces(
    yaml_path: str,
    params: np.ndarray,
    tile_size: Tuple[int, int],
    output_dir: str,
    base_dir: Optional[str] = None,
    avg_signal_threshold: Optional[float] = None,
    crop_x: int = 0,
    normalize_min: Optional[float] = None,
    degree: float = 1.5
):
    """
    Save corrected surface maps (with plane subtracted) to output directory.
    
    Note: ALL tiles are processed and saved, regardless of avg_signal_threshold.
    The threshold is only used for plane fitting, not for outputting corrected surfaces.
    
    Parameters
    ----------
    yaml_path : str
        Path to YAML file with tile configuration
    params : np.ndarray
        Plane parameters [a, b, d, c] where surface = a*x + b*y + d*x*y + c
    tile_size : tuple
        (width, height) = (x_size, y_size) in pixels
    output_dir : str
        Directory to save corrected surface maps
    base_dir : str, optional
        Base directory for surface map files. If None, uses metadata['base_dir']
    avg_signal_threshold : float, optional
        This parameter is kept for API compatibility but is not used.
        All tiles are processed regardless of avg_signal_threshold.
    crop_x : int
        Number of pixels to crop from the start of x dimension (default: 0)
    """
    print(f"\nSaving corrected surface maps to {output_dir}")
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Load configuration
    metadata, tiles = load_yaml_config(yaml_path)
    
    if base_dir is None:
        base_dir = metadata.get('base_dir', '.')
    
    # Note: We process ALL tiles for output, regardless of avg_signal_threshold
    # The threshold is only used for plane fitting, not for outputting corrected surfaces
    print(f"Processing all {len(tiles)} tiles for corrected surface output")
    
    # Create plane array (same for all tiles, using local coordinates)
    plane = create_tile_plane(params, tile_size, normalize_min=normalize_min, degree=degree)
    
    # Process each tile
    saved_count = 0
    skipped_count = 0
    
    for tile in tiles:
        filepath = tile['filepath']
        input_path = Path(base_dir) / filepath
        
        if not input_path.exists():
            print(f"  Warning: Surface map not found: {input_path}, skipping")
            skipped_count += 1
            continue
        
        try:
            # Load original NIfTI file (preserving header/affine)
            img = nib.load(str(input_path))
            surface = np.array(img.dataobj)
            
            # Handle 3D arrays
            if surface.ndim == 3:
                surface = surface.squeeze()
            
            # Crop x dimension if requested
            if crop_x > 0:
                if crop_x >= surface.shape[0]:
                    print(f"  Warning: crop_x ({crop_x}) >= surface width ({surface.shape[0]}) for {filepath}, skipping")
                    skipped_count += 1
                    continue
                surface = surface[crop_x:, :]
            
            # Check if surface size matches tile_size
            w, h = tile_size
            if surface.shape[:2] != (w, h):
                print(f"  Warning: Surface size {surface.shape[:2]} doesn't match tile size {tile_size} for {filepath}, skipping")
                skipped_count += 1
                continue
            
            # Subtract plane from surface
            # The plane is computed in local tile coordinates, so we can directly subtract
            corrected_surface = surface - plane
            
            # Preserve original shape (in case it was 3D)
            if img.shape != corrected_surface.shape:
                # If original was 3D, add z dimension back
                if len(img.shape) == 3 and corrected_surface.ndim == 2:
                    corrected_surface = corrected_surface[:, :, np.newaxis]
            
            # Create new NIfTI image with same header/affine as original
            corrected_img = nib.Nifti1Image(corrected_surface.astype(np.float32), img.affine, img.header)
            
            # Save to output directory with same filename
            output_file = output_path / Path(filepath).name
            nib.save(corrected_img, str(output_file))
            
            saved_count += 1
            if saved_count % 10 == 0:
                print(f"  Processed {saved_count} tiles...")
        
        except Exception as e:
            print(f"  Error processing {filepath}: {e}")
            skipped_count += 1
            continue
    
    print(f"\n  Saved {saved_count} corrected surface maps")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} tiles")


def plot_plane_cross_sections(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    output_path: Optional[str] = None,
    degree: float = 1.5
):
    """
    Plot the fitted plane at middle x and middle y coordinates in tile space.
    
    Parameters
    ----------
    params : np.ndarray
        Plane parameters [a, b, d, c] where surface = a*x + b*y + d*x*y + c
        The plane is a function of local tile coordinates (0 to width-1, 0 to height-1)
    tile_size : tuple
        (width, height) = (x_size, y_size) in pixels
    output_path : str, optional
        Path to save the plot. If None, displays interactively.
    """
    if not HAS_MATPLOTLIB:
        print("Warning: matplotlib not available, skipping plane plots")
        return
    
    a, b, d, c = params
    
    # tile_size is (width, height) = (x_size, y_size)
    w, h = tile_size  # w = width (x dimension), h = height (y dimension)
    
    # Middle coordinates in tile space
    x_mid = (w - 1) / 2.0  # Middle x in local tile coordinates (0 to w-1)
    y_mid = (h - 1) / 2.0  # Middle y in local tile coordinates (0 to h-1)
    
    # Create coordinate arrays for plotting in tile space
    x_local = np.linspace(0, w - 1, 200)  # x coordinates: 0 to w-1
    y_local = np.linspace(0, h - 1, 200)  # y coordinates: 0 to h-1
    
    # Compute plane values using local tile coordinates
    # At fixed x_local = x_mid: plane = a*x_mid + b*y_local + d*x_mid*y_local + c
    plane_at_x_mid = (a * x_mid + c) + (b + d * x_mid) * y_local
    
    # At fixed y_local = y_mid: plane = a*x_local + b*y_mid + d*x_local*y_mid + c
    plane_at_y_mid = (b * y_mid + c) + (a + d * y_mid) * x_local
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Plot 1: Plane as function of y_local at x_local = x_mid
    ax1.plot(y_local, plane_at_x_mid, 'b-', linewidth=2, label=f'Plane at x = {x_mid:.1f}')
    ax1.set_xlabel('Y coordinate (tile space)', fontsize=12)
    ax1.set_ylabel('Plane value', fontsize=12)
    ax1.set_title(f'Plane Cross-Section at x = {x_mid:.1f} (tile space)', fontsize=14)
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # Plot 2: Plane as function of x_local at y_local = y_mid
    ax2.plot(x_local, plane_at_y_mid, 'r-', linewidth=2, label=f'Plane at y = {y_mid:.1f}')
    ax2.set_xlabel('X coordinate (tile space)', fontsize=12)
    ax2.set_ylabel('Plane value', fontsize=12)
    ax2.set_title(f'Plane Cross-Section at y = {y_mid:.1f} (tile space)', fontsize=14)
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\nSaved plane cross-section plots to {output_path}")
    else:
        plt.show()
    
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description='Find plane that describes surface signal variation across mosaic tiles'
    )
    parser.add_argument('yaml_path', type=str, help='Path to tile configuration YAML')
    parser.add_argument('--output', type=str, default='tile_plane.nii.gz',
                       help='Output NIfTI file path (default: tile_plane.nii.gz)')
    parser.add_argument('--base-dir', type=str, default=None,
                       help='Base directory for surface map files (default: from YAML metadata)')
    parser.add_argument('--resolution', type=float, nargs=2, default=None,
                       help='Resolution [x, y] in mm/pixel (default: from YAML metadata)')
    parser.add_argument('--tile-size', type=int, nargs=2, default=None,
                       help='Tile size [width, height] in pixels (default: estimate from surface maps)')
    parser.add_argument('--subsample', type=int, default=1,
                       help='Subsample factor for overlap regions (default: 1)')
    parser.add_argument('--avg-signal-threshold', type=float, default=None,
                       help='Minimum avg_signal value for tiles to be included (default: no threshold)')
    parser.add_argument('--no-verify', action='store_true',
                       help='Skip verification step (default: verification is enabled)')
    parser.add_argument('--plot', type=str, default=None,
                       help='Output path for plane cross-section plots (default: no plot)')
    parser.add_argument('--outlier-method', type=str, default='iqr',
                       help='Outlier detection method (default: iqr)')
    parser.add_argument('--outlier-iqr-factor', type=float, default=1.5,
                       help='IQR factor for outlier detection (default: 1.5)')
    parser.add_argument('--outlier-z-threshold', type=float, default=3.0,
                       help='Z-score threshold for outlier detection (default: 3.0)')
    parser.add_argument('--output-corrected-dir', type=str, default=None,
                       help='Directory to save corrected surface maps (with plane subtracted). If not specified, corrected surfaces are not saved.')
    parser.add_argument('--crop-x', type=int, default=0,
                       help='Number of pixels to crop from the start of x dimension. When set, surface maps are cropped as surface[crop_x:, :] and tile x coordinates are adjusted by adding crop_x (default: 0)')
    parser.add_argument('--normalize-min', type=float, default=None,
                       help='Normalize the plane so its minimum value becomes this value. If not specified, the plane is not normalized (default: None)')
    parser.add_argument('--degree', type=float, default=1.5,
                       help='Polynomial degree for plane fitting: 1.0 (linear), 1.5 (linear with cross term), 2.0 (quadratic), 2.5 (quadratic with cross term), 3.0 (cubic) (default: 1.5)')
    args = parser.parse_args()
    
    # Validate degree
    valid_degrees = [1.0, 1.5, 2.0, 2.5, 3.0]
    if args.degree not in valid_degrees:
        raise ValueError(f"Invalid degree: {args.degree}. Must be one of {valid_degrees}")
    
    # Default verification to True unless --no-verify is set
    args.verify = not args.no_verify
    
    # Load configuration
    print(f"Loading tile configuration from {args.yaml_path}")
    metadata, tiles = load_yaml_config(args.yaml_path)
    
    print(f"Loaded {len(tiles)} tiles")
    
    # Fit plane
    tile_size = tuple(args.tile_size) if args.tile_size else None
    
    print(f"\nFitting plane (degree {args.degree}) from overlapping regions in surface maps...")
    params, info = fit_plane_from_overlaps(
        args.yaml_path,
        base_dir=args.base_dir,
        tile_size=tile_size,
        subsample=args.subsample,
        avg_signal_threshold=args.avg_signal_threshold,
        outlier_method=args.outlier_method,
        outlier_iqr_factor=args.outlier_iqr_factor,
        outlier_z_threshold=args.outlier_z_threshold,
        crop_x=args.crop_x,
        degree=args.degree
    )
    
    degree = info.get('degree', args.degree)
    num_params = get_plane_params_count(degree)
    non_const_params = params[:num_params]
    c = params[-1]
    
    print(f"\nFitted plane parameters (degree {degree}):")
    param_names = ['a', 'b', 'd', 'e', 'f', 'g', 'h'][:num_params]
    for i, name in enumerate(param_names):
        print(f"  {name}: {non_const_params[i]:.6e}  [determined from overlaps]")
    print(f"  c (constant): {c:.6f}  [determined from all surface values]")
    
    # Print plane equation based on degree
    eq_parts = []
    if degree == 1.0:
        eq_parts = [f"{non_const_params[0]:.6e} * x", f"{non_const_params[1]:.6e} * y"]
    elif degree == 1.5:
        eq_parts = [f"{non_const_params[0]:.6e} * x", f"{non_const_params[1]:.6e} * y", f"{non_const_params[2]:.6e} * x*y"]
    elif degree == 2.0:
        eq_parts = [f"{non_const_params[0]:.6e} * x", f"{non_const_params[1]:.6e} * y", 
                   f"{non_const_params[2]:.6e} * x^2", f"{non_const_params[3]:.6e} * y^2", f"{non_const_params[4]:.6e} * x*y"]
    elif degree == 2.5:
        eq_parts = [f"{non_const_params[0]:.6e} * x", f"{non_const_params[1]:.6e} * y",
                   f"{non_const_params[2]:.6e} * x^2", f"{non_const_params[3]:.6e} * y^2", 
                   f"{non_const_params[4]:.6e} * x*y", f"{non_const_params[5]:.6e} * x^2*y^2"]
    elif degree == 3.0:
        eq_parts = [f"{non_const_params[0]:.6e} * x", f"{non_const_params[1]:.6e} * y",
                   f"{non_const_params[2]:.6e} * x^2", f"{non_const_params[3]:.6e} * y^2",
                   f"{non_const_params[4]:.6e} * x^3", f"{non_const_params[5]:.6e} * y^3",
                   f"{non_const_params[6]:.6e} * x^2*y^2"]
    print(f"\nPlane equation: signal = {' + '.join(eq_parts)} + {c:.6f}")
    print(f"\nNote: Non-constant coefficients are determined from overlapping regions.")
    print(f"      Constant c is determined separately from all surface values.")
    print(f"\nFit statistics:")
    print(f"  RMSE: {info['rmse']:.4f}")
    print(f"  MAE:  {info['mae']:.4f}")
    print(f"  Number of overlapping regions: {info['num_overlaps']}")
    print(f"  Number of pixels used: {info['num_pixels']}")
    
    # Export to NIfTI
    resolution = tuple(args.resolution) if args.resolution else None
    tile_size = info['tile_size']
    
    export_plane_to_nifti(
        params,
        tile_size,
        args.output,
        metadata,
        resolution=resolution,
        normalize_min=args.normalize_min,
        degree=degree
    )
    
    # Verify plane correction if requested
    if args.verify:
        verify_stats = verify_plane_correction(
        args.yaml_path,
            params,
        base_dir=args.base_dir,
        tile_size=tile_size,
            subsample=args.subsample,
            avg_signal_threshold=args.avg_signal_threshold,
            crop_x=args.crop_x,
            degree=degree
        )
        
        # Exit with error code if verification failed
        if not verify_stats.get('verification_passed', False):
            print("\nWarning: Verification failed. The plane correction may not be optimal.")
    
    # Plot plane cross-sections if requested
    if args.plot:
        plot_plane_cross_sections(params, tile_size, args.plot, degree=degree)
    
    # Save corrected surfaces if requested
    if args.output_corrected_dir:
        save_corrected_surfaces(
            args.yaml_path,
            params,
            tile_size,
            args.output_corrected_dir,
            base_dir=args.base_dir,
            avg_signal_threshold=args.avg_signal_threshold,
            crop_x=args.crop_x,
            normalize_min=args.normalize_min
    )
    
    print("\nDone!")


if __name__ == "__main__":
    main()
