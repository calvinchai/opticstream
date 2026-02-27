#!/usr/bin/env python3
"""
Two-stage plane fitting: First fit degree 1.5 plane, then fit degree 2 plane on corrected tiles.

This script performs a two-stage plane fitting process:
1. Stage 1: Fit a degree 1.5 plane (linear with x*y cross term) from overlapping regions and subtract it from all tiles
2. Stage 2: Fit a degree 2.0 plane on the corrected tiles to find remaining quadratic terms
3. Output: Extract and export only the quadratic component (x², y², x*y terms)

The approach helps identify quadratic terms that remain after removing linear and cross-term components.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

# Import functions from the original find_tile_plane script
# Add the scripts directory to the path to allow importing
sys.path.insert(0, str(Path(__file__).parent))
import find_tile_plane as ftp

# Re-export commonly used constants and functions for convenience
DEGREE_CONFIG = ftp.DEGREE_CONFIG
VALID_DEGREES = ftp.VALID_DEGREES
DEFAULT_RESOLUTION = ftp.DEFAULT_RESOLUTION
VERIFICATION_THRESHOLDS = ftp.VERIFICATION_THRESHOLDS
HAS_MATPLOTLIB = ftp.HAS_MATPLOTLIB

# Import functions we'll use
load_yaml_config = ftp.load_yaml_config
load_tile_data = ftp.load_tile_data
find_all_overlaps = ftp.find_all_overlaps
fit_plane_from_overlaps = ftp.fit_plane_from_overlaps
compute_residuals = ftp.compute_residuals
determine_constant_term = ftp.determine_constant_term
get_valid_mask = ftp.get_valid_mask
create_coordinate_meshgrid = ftp.create_coordinate_meshgrid
find_tile_overlap = ftp.find_tile_overlap
extract_overlap_data = ftp.extract_overlap_data
filter_overlap_outliers = ftp.filter_overlap_outliers
compute_initial_differences = ftp.compute_initial_differences
create_tile_plane = ftp.create_tile_plane
create_tile_plane_original_size = ftp.create_tile_plane_original_size
extract_plane_params = ftp.extract_plane_params
compute_plane_value = ftp.compute_plane_value
get_plane_params_count = ftp.get_plane_params_count
export_plane_to_nifti = ftp.export_plane_to_nifti
format_plane_equation = ftp.format_plane_equation
PlaneParams = ftp.PlaneParams


def apply_plane_to_tiles(
    tile_data: Dict,
    params: np.ndarray,
    tile_size: Tuple[int, int],
    degree: float = 1.0,
    crop_x: int = 0
) -> Dict:
    """
    Apply fitted plane to all tiles by subtracting it from their surfaces.
    
    Important: Handle crop_x carefully:
    - Surfaces are already cropped (surface[crop_x:, :]) when loaded
    - tile_size is the size AFTER cropping
    - Use create_tile_plane (not create_tile_plane_original_size) because surfaces are already cropped
    - Plane coordinates are local (0 to w-1, 0 to h-1) where w, h are cropped dimensions
    
    Parameters
    ----------
    tile_data : Dict
        Dictionary mapping tile_number to {'surface': np.ndarray, 'tile': Dict}
    params : np.ndarray
        Plane parameters (non-constant params + c)
    tile_size : Tuple[int, int]
        Tile size (width, height) AFTER cropping
    degree : float
        Polynomial degree of the plane
    crop_x : int
        Number of pixels cropped from x dimension (for reference, surfaces are already cropped)
    
    Returns
    -------
    Dict
        Modified tile_data with corrected surfaces (plane subtracted)
    """
    print(f"\nApplying degree {degree} plane to all tiles...")
    
    # Create the plane array (same for all tiles, using local coordinates)
    # Since surfaces are already cropped, we use create_tile_plane with cropped tile_size
    plane = create_tile_plane(params, tile_size, degree=degree)
    
    # Apply plane to each tile
    corrected_tile_data = {}
    for tile_number, tile_info in tile_data.items():
        surface = tile_info['surface'].copy()  # Make a copy to avoid modifying original
        
        # Check that surface size matches tile_size (after cropping)
        if surface.shape[:2] != tile_size:
            print(f"  Warning: Tile {tile_number} size {surface.shape[:2]} doesn't match tile_size {tile_size}, skipping")
            continue
        
        # Subtract plane from surface
        # The plane is computed in local tile coordinates, so we can directly subtract
        corrected_surface = surface - plane
        
        corrected_tile_data[tile_number] = {
            'surface': corrected_surface,
            'tile': tile_info['tile']
        }
    
    print(f"  Applied plane to {len(corrected_tile_data)} tiles")
    return corrected_tile_data


def extract_quadratic_component(params_degree2: np.ndarray, degree: float = 2.0) -> np.ndarray:
    """
    Extract only quadratic terms from degree 2.0 plane parameters.
    
    For degree 2.0: params = [a, b, d, e, f, c] where:
    - a, b are linear terms (x, y)
    - d, e, f are quadratic terms (x², y², x*y)
    - c is constant
    
    Parameters
    ----------
    params_degree2 : np.ndarray
        Full degree 2.0 plane parameters [a, b, d, e, f, c]
    degree : float
        Polynomial degree (should be 2.0)
    
    Returns
    -------
    np.ndarray
        Quadratic-only parameters [d, e, f, c_quad]
        where c_quad is adjusted to represent only the quadratic component
    """
    if degree != 2.0:
        raise ValueError(f"extract_quadratic_component only works with degree 2.0, got {degree}")
    
    plane_params = extract_plane_params(params_degree2, degree)
    
    # For degree 2.0: non_const_params = [a, b, d, e, f]
    # where a, b are linear (x, y), d, e, f are quadratic (x², y², x*y)
    a, b, d, e, f = plane_params.non_const_params
    c_full = plane_params.c
    
    # Extract only quadratic terms: [d, e, f]
    # For the constant term, we keep the original c since it's part of the full plane
    # However, if we want to isolate just the quadratic component's contribution,
    # we might want to set c_quad = 0 or adjust it. For now, we'll keep c.
    # Note: The constant term c is shared between linear and quadratic components,
    # so keeping it is reasonable for visualization purposes.
    
    quadratic_params = np.array([d, e, f, c_full])
    
    return quadratic_params


def compute_quadratic_plane_value(params: np.ndarray, x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Compute plane value using only quadratic terms.
    
    Parameters
    ----------
    params : np.ndarray
        Quadratic parameters [d, e, f, c] where d*x² + e*y² + f*x*y + c
    x : np.ndarray
        X coordinates
    y : np.ndarray
        Y coordinates
    
    Returns
    -------
    np.ndarray
        Plane values using only quadratic terms
    """
    if len(params) != 4:
        raise ValueError(f"quadratic params must have length 4, got {len(params)}")
    
    d, e, f, c = params
    return d * x**2 + e * y**2 + f * x * y + c


def create_quadratic_plane(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    tile_x: float = 0.0,
    tile_y: float = 0.0,
    normalize_min: Optional[float] = None,
    crop_x: int = 0
) -> np.ndarray:
    """
    Create a plane array using only quadratic terms.
    
    Parameters
    ----------
    params : np.ndarray
        Quadratic parameters [d, e, f, c] where d*x² + e*y² + f*x*y + c
    tile_size : Tuple[int, int]
        (width, height) = (x_size, y_size) in pixels (after cropping)
    tile_x : float
        X coordinate of tile origin (for NIfTI affine, default: 0.0)
    tile_y : float
        Y coordinate of tile origin (for NIfTI affine, default: 0.0)
    normalize_min : float, optional
        If specified, normalize the plane so its minimum value becomes this value
    crop_x : int
        Number of pixels cropped (for reference, surfaces are already cropped)
    
    Returns
    -------
    np.ndarray
        2D array with quadratic plane values, shape (width, height)
    """
    w, h = tile_size
    
    # Create coordinate meshgrid for local coordinates (0 to w-1, 0 to h-1)
    x_local_grid, y_local_grid = create_coordinate_meshgrid(w, h)
    
    # Compute quadratic plane values
    plane = compute_quadratic_plane_value(params, x_local_grid, y_local_grid)
    
    # Normalize plane if requested
    if normalize_min is not None:
        plane_min = np.min(plane)
        plane = plane - plane_min + normalize_min
    
    return plane


def create_quadratic_plane_original_size(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    crop_x: int = 0,
    normalize_min: Optional[float] = None
) -> np.ndarray:
    """
    Create a quadratic plane array in the original size (before cropping), including the cropped x region.
    
    Parameters
    ----------
    params : np.ndarray
        Quadratic parameters [d, e, f, c]
    tile_size : Tuple[int, int]
        (width, height) = (x_size, y_size) in pixels (after cropping)
    crop_x : int
        Number of pixels cropped from the start of x dimension
    normalize_min : float, optional
        If specified, normalize the plane so its minimum value becomes this value
    
    Returns
    -------
    np.ndarray
        2D array with quadratic plane values, shape (width + crop_x, height) = (original_width, height)
        The plane includes the cropped region with x coordinates from -crop_x to width-1
    """
    w, h = tile_size
    
    # Original size includes the cropped region
    original_w = w + crop_x
    
    # Create coordinate meshgrid for original size (including cropped region)
    x_local_grid, y_local_grid = create_coordinate_meshgrid(original_w, h, x_start=-crop_x)
    
    # Compute quadratic plane values using the plane equation
    plane = compute_quadratic_plane_value(params, x_local_grid, y_local_grid)
    
    # Normalize plane if requested
    if normalize_min is not None:
        plane_min = np.min(plane)
        plane = plane - plane_min + normalize_min
    
    return plane


def plot_quadratic_plane_cross_sections(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    output_path: Optional[str] = None,
    crop_x: int = 0
):
    """
    Plot the quadratic plane at middle x and middle y coordinates in tile space.
    
    Parameters
    ----------
    params : np.ndarray
        Quadratic parameters [d, e, f, c] where d*x² + e*y² + f*x*y + c
    tile_size : Tuple[int, int]
        (width, height) = (x_size, y_size) in pixels (after cropping)
    output_path : str, optional
        Path to save the plot. If None, displays interactively.
    crop_x : int
        Number of pixels cropped (for reference, coordinates are local to cropped size)
    """
    if not HAS_MATPLOTLIB:
        print("Warning: matplotlib not available, skipping quadratic plane plots")
        return
    
    import matplotlib.pyplot as plt
    
    w, h = tile_size
    
    # Middle coordinates in tile space (local coordinates: 0 to w-1, 0 to h-1)
    x_mid = (w - 1) / 2.0
    y_mid = (h - 1) / 2.0
    
    # Create coordinate arrays for plotting in tile space
    x_local = np.linspace(0, w - 1, 200)  # x coordinates: 0 to w-1
    y_local = np.linspace(0, h - 1, 200)  # y coordinates: 0 to h-1
    
    # Compute quadratic plane values using local tile coordinates
    # At fixed x_local = x_mid
    x_mid_array = np.full_like(y_local, x_mid)
    plane_at_x_mid = compute_quadratic_plane_value(params, x_mid_array, y_local)
    
    # At fixed y_local = y_mid
    y_mid_array = np.full_like(x_local, y_mid)
    plane_at_y_mid = compute_quadratic_plane_value(params, x_local, y_mid_array)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Plot 1: Quadratic plane as function of y_local at x_local = x_mid
    ax1.plot(y_local, plane_at_x_mid, 'b-', linewidth=2, label=f'Quadratic plane at x = {x_mid:.1f}')
    ax1.set_xlabel('Y coordinate (tile space)', fontsize=12)
    ax1.set_ylabel('Plane value', fontsize=12)
    ax1.set_title(f'Quadratic Plane Cross-Section at x = {x_mid:.1f} (tile space)', fontsize=14)
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # Plot 2: Quadratic plane as function of x_local at y_local = y_mid
    ax2.plot(x_local, plane_at_y_mid, 'r-', linewidth=2, label=f'Quadratic plane at y = {y_mid:.1f}')
    ax2.set_xlabel('X coordinate (tile space)', fontsize=12)
    ax2.set_ylabel('Plane value', fontsize=12)
    ax2.set_title(f'Quadratic Plane Cross-Section at y = {y_mid:.1f} (tile space)', fontsize=14)
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\nSaved quadratic plane cross-section plots to {output_path}")
    else:
        plt.show()
    
    plt.close()


def fit_plane_from_tile_data(
    tile_data: Dict,
    tile_size: Tuple[int, int],
    subsample: int = 5,
    outlier_method: Optional[str] = 'iqr',
    outlier_iqr_factor: float = 1.5,
    outlier_z_threshold: float = 3.0,
    crop_x: int = 0,
    degree: float = 2.0
) -> Tuple[np.ndarray, Dict]:
    """
    Fit a plane to surface values from tile_data (already loaded).
    
    This is similar to fit_plane_from_overlaps but works with pre-loaded tile_data.
    
    Parameters
    ----------
    tile_data : Dict
        Dictionary mapping tile_number to {'surface': np.ndarray, 'tile': Dict}
    tile_size : Tuple[int, int]
        Tile size (width, height) after cropping
    subsample : int
        Subsample factor for overlap regions
    outlier_method : str, optional
        Outlier detection method ('iqr' or 'zscore')
    outlier_iqr_factor : float
        IQR factor for outlier detection
    outlier_z_threshold : float
        Z-score threshold for outlier detection
    crop_x : int
        Number of pixels cropped from x dimension
    degree : float
        Polynomial degree (should be 2.0 for stage 2)
    
    Returns
    -------
    Tuple[np.ndarray, Dict]
        params: Plane parameters
        info: Dictionary with fit statistics
    """
    # Find overlapping regions
    overlap_data_list = find_all_overlaps(tile_data, tile_size, crop_x, subsample)
    
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
    all_diffs_before = compute_initial_differences(overlap_data_list)
    
    # Fit plane using least squares
    print(f"Fitting plane (degree {degree}) to overlapping regions...")
    num_params = get_plane_params_count(degree)
    print(f"  Note: Overlapping regions can only determine {num_params} non-constant coefficients.")
    print("  The constant term c cancels out in differences and must be determined separately.")
    
    def residual_func(params):
        return compute_residuals(params, overlap_data_list, degree=degree)
    
    initial_params = np.zeros(num_params)
    
    # Use trimmed mean objective (same as original script)
    def trimmed_mean_objective(params, trim_percent=10):
        """Minimize mean after trimming worst residuals."""
        residuals = residual_func(params)
        squared_residuals = residuals ** 2
        abs_residuals = np.abs(squared_residuals)
        # Remove top trim_percent% of residuals
        threshold = np.percentile(abs_residuals, 100 - trim_percent)
        trimmed = abs_residuals[abs_residuals <= threshold]
        return np.mean(trimmed) if len(trimmed) > 0 else np.mean(abs_residuals)
    
    from scipy.optimize import minimize
    result = minimize(trimmed_mean_objective, initial_params)
    non_const_params = result.x
    
    # Create parameter dictionary for printing
    config = DEGREE_CONFIG[degree]
    params_dict = {name: non_const_params[i] for i, name in enumerate(config['names'])}
    print(f"  Fitted coefficients: {', '.join([f'{k} = {v:.6e}' for k, v in params_dict.items()])}")
    
    # Determine constant term c
    c = determine_constant_term(tile_data, non_const_params, tile_size, degree, subsample)
    
    # Combine non-constant parameters with constant c
    params = np.concatenate([non_const_params, [c]])
    
    # Compute statistics
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
        'initial_diffs': all_diffs_before
    }
    
    return params, info


def export_quadratic_plane_to_nifti(
    params: np.ndarray,
    tile_size: Tuple[int, int],
    output_path: str,
    metadata: Dict,
    resolution: Optional[Tuple[float, float]] = None,
    tile_x: float = 0.0,
    tile_y: float = 0.0,
    normalize_min: Optional[float] = None,
    crop_x: int = 0
):
    """
    Export the quadratic plane as a NIfTI volume.
    
    Parameters
    ----------
    params : np.ndarray
        Quadratic parameters [d, e, f, c]
    tile_size : Tuple[int, int]
        Size of tile (width, height) after cropping
    output_path : str
        Path to save the output NIfTI file
    metadata : Dict
        Metadata from YAML file
    resolution : Tuple[float, float], optional
        Resolution (x, y) in mm/pixel
    tile_x : float
        X coordinate of tile in mosaic space
    tile_y : float
        Y coordinate of tile in mosaic space
    normalize_min : float, optional
        Normalize the plane so its minimum value becomes this value
    crop_x : int
        Number of pixels cropped from x dimension
    """
    import nibabel as nib
    
    print(f"\nExporting quadratic plane to NIfTI file: {output_path}")
    
    # Determine resolution
    if resolution is None:
        if 'scan_resolution' in metadata and len(metadata['scan_resolution']) >= 2:
            resolution = tuple(metadata['scan_resolution'][:2])
            print(f"  Using resolution from metadata: {resolution} mm/pixel")
        else:
            resolution = DEFAULT_RESOLUTION
            print(f"  Using default resolution: {resolution} mm/pixel")
    
    w, h = tile_size
    original_w = w + crop_x
    if crop_x > 0:
        print(f"  Tile size (after cropping): {w} x {h} pixels")
        print(f"  Original tile size (including cropped region): {original_w} x {h} pixels")
        print(f"  Cropped region: {crop_x} pixels from start of x dimension")
    else:
        print(f"  Tile size: {w} x {h} pixels")
    
    # Create quadratic plane array in original size (including cropped region) if crop_x > 0
    if crop_x > 0:
        plane = create_quadratic_plane_original_size(params, tile_size, crop_x=crop_x, normalize_min=normalize_min)
    else:
        plane = create_quadratic_plane(params, tile_size, tile_x, tile_y, normalize_min, crop_x)
    
    print(f"  Quadratic plane shape: {plane.shape}")
    if normalize_min is not None:
        print(f"  Plane normalized: minimum value = {normalize_min:.4f}")
    print(f"  Plane value range: [{np.min(plane):.4f}, {np.max(plane):.4f}]")
    
    # Print quadratic equation
    d, e, f, c = params
    print(f"  Quadratic plane equation: signal = {d:.6e}*x^2 + {e:.6e}*y^2 + {f:.6e}*x*y + {c:.6f}")
    
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
    
    print(f"  Saved quadratic plane to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Two-stage plane fitting: Find linear plane, then quadratic terms on corrected tiles'
    )
    parser.add_argument('yaml_path', type=str, help='Path to tile configuration YAML')
    parser.add_argument('--output', type=str, default='tile_plane_quadratic.nii.gz',
                       help='Output NIfTI file path for quadratic plane (default: tile_plane_quadratic.nii.gz)')
    parser.add_argument('--output-linear', type=str, default=None,
                       help='Optional output NIfTI file path for degree 1.5 plane')
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
    parser.add_argument('--plot', type=str, default=None,
                       help='Output path for quadratic plane cross-section plots (default: no plot)')
    parser.add_argument('--outlier-method', type=str, default='iqr',
                       help='Outlier detection method (default: iqr)')
    parser.add_argument('--outlier-iqr-factor', type=float, default=1.5,
                       help='IQR factor for outlier detection (default: 1.5)')
    parser.add_argument('--outlier-z-threshold', type=float, default=3.0,
                       help='Z-score threshold for outlier detection (default: 3.0)')
    parser.add_argument('--crop-x', type=int, default=0,
                       help='Number of pixels to crop from the start of x dimension (default: 0)')
    parser.add_argument('--normalize-min', type=float, default=None,
                       help='Normalize the quadratic plane so its minimum value becomes this value (default: None)')
    args = parser.parse_args()
    
    # Load configuration
    print("="*60)
    print("Two-Stage Plane Fitting")
    print("="*60)
    print(f"\nLoading tile configuration from {args.yaml_path}")
    metadata, tiles = load_yaml_config(args.yaml_path)
    
    print(f"Loaded {len(tiles)} tiles")
    
    # Load tile data
    tile_size = tuple(args.tile_size) if args.tile_size else None
    tile_data, tile_size = load_tile_data(
        args.yaml_path,
        base_dir=args.base_dir,
        tile_size=tile_size,
        avg_signal_threshold=args.avg_signal_threshold,
        crop_x=args.crop_x
    )
    
    print(f"\nTile size (after cropping): {tile_size}")
    
    # ========================================================================
    # Stage 1: Fit degree 1.5 plane
    # ========================================================================
    print("\n" + "="*60)
    print("STAGE 1: Fitting degree 1.5 (linear with cross term) plane")
    print("="*60)
    
    params_linear, info_linear = fit_plane_from_overlaps(
        args.yaml_path,
        base_dir=args.base_dir,
        tile_size=tile_size,
        subsample=args.subsample,
        avg_signal_threshold=args.avg_signal_threshold,
        outlier_method=args.outlier_method,
        outlier_iqr_factor=args.outlier_iqr_factor,
        outlier_z_threshold=args.outlier_z_threshold,
        crop_x=args.crop_x,
        degree=1.5
    )
    
    # Extract and display linear parameters
    plane_params_linear = extract_plane_params(params_linear, 1.5)
    config_linear = DEGREE_CONFIG[1.5]
    print(f"\nStage 1 - Fitted degree 1.5 plane parameters:")
    for i, name in enumerate(config_linear['names']):
        print(f"  {name} ({config_linear['terms'][i]}): {plane_params_linear.non_const_params[i]:.6e}")
    print(f"  c (constant): {plane_params_linear.c:.6f}")
    print(f"  RMSE: {info_linear['rmse']:.4f}")
    print(f"  MAE:  {info_linear['mae']:.4f}")
    
    # Apply linear plane to tiles
    print("\n" + "-"*60)
    print("Applying degree 1.5 plane to all tiles...")
    corrected_tile_data = apply_plane_to_tiles(
        tile_data, params_linear, tile_size, degree=1.5, crop_x=args.crop_x
    )
    
    # ========================================================================
    # Stage 2: Fit degree 2.0 plane on corrected tiles
    # ========================================================================
    print("\n" + "="*60)
    print("STAGE 2: Fitting degree 2.0 (quadratic) plane on corrected tiles")
    print("="*60)
    
    params_degree2, info_degree2 = fit_plane_from_tile_data(
        corrected_tile_data,
        tile_size,
        subsample=args.subsample,
        outlier_method=args.outlier_method,
        outlier_iqr_factor=args.outlier_iqr_factor,
        outlier_z_threshold=args.outlier_z_threshold,
        crop_x=args.crop_x,
        degree=2.0
    )
    
    # Extract and display degree 2 parameters
    plane_params_degree2 = extract_plane_params(params_degree2, 2.0)
    config_degree2 = DEGREE_CONFIG[2.0]
    print(f"\nStage 2 - Fitted degree 2.0 plane parameters:")
    for i, name in enumerate(config_degree2['names']):
        value = plane_params_degree2.non_const_params[i]
        term_type = "linear" if i < 2 else "quadratic"
        print(f"  {name} ({config_degree2['terms'][i]}, {term_type}): {value:.6e}")
    print(f"  c (constant): {plane_params_degree2.c:.6f}")
    print(f"  RMSE: {info_degree2['rmse']:.4f}")
    print(f"  MAE:  {info_degree2['mae']:.4f}")
    
    # Check if linear and cross terms are near zero (as expected after stage 1)
    # For degree 2.0: [a, b, d, e, f] where a=x, b=y, d=x², e=y², f=x*y
    a, b, d, e, f = plane_params_degree2.non_const_params
    print(f"\n  Note: Linear and cross terms should be near zero after stage 1 correction:")
    print(f"    a (x): {a:.6e} (expected ~0)")
    print(f"    b (y): {b:.6e} (expected ~0)")
    print(f"    f (x*y): {f:.6e} (expected ~0, since x*y was removed in stage 1)")
    
    # ========================================================================
    # Extract quadratic component
    # ========================================================================
    print("\n" + "="*60)
    print("Extracting quadratic component")
    print("="*60)
    
    params_quadratic = extract_quadratic_component(params_degree2, degree=2.0)
    d, e, f, c_quad = params_quadratic
    
    print(f"\nQuadratic component parameters:")
    print(f"  d (x² coefficient): {d:.6e}")
    print(f"  e (y² coefficient): {e:.6e}")
    print(f"  f (x*y coefficient): {f:.6e}")
    print(f"  c (constant): {c_quad:.6f}")
    print(f"\nQuadratic plane equation: signal = {d:.6e}*x² + {e:.6e}*y² + {f:.6e}*x*y + {c_quad:.6f}")
    
    # ========================================================================
    # Export results
    # ========================================================================
    print("\n" + "="*60)
    print("Exporting results")
    print("="*60)
    
    # Export quadratic plane
    resolution = tuple(args.resolution) if args.resolution else None
    export_quadratic_plane_to_nifti(
        params_quadratic,
        tile_size,
        args.output,
        metadata,
        resolution=resolution,
        normalize_min=args.normalize_min,
        crop_x=args.crop_x
    )
    
    # Optionally export linear plane
    if args.output_linear:
        print(f"\nExporting degree 1.5 plane to {args.output_linear}")
        export_plane_to_nifti(
            params_linear,
            tile_size,
            args.output_linear,
            metadata,
            resolution=resolution,
            normalize_min=None,
            degree=1.5,
            crop_x=args.crop_x
        )
    
    # Plot quadratic plane cross-sections if requested
    if args.plot:
        print("\n" + "="*60)
        print("Plotting quadratic plane cross-sections")
        print("="*60)
        plot_quadratic_plane_cross_sections(
            params_quadratic,
            tile_size,
            args.plot,
            crop_x=args.crop_x
        )
    
    print("\n" + "="*60)
    print("Done!")
    print("="*60)


if __name__ == "__main__":
    main()

