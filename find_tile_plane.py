"""
Find the plane that describes how tiles are tilted in z-direction.

The problem: Each tile volume is tilted by a plane, meaning the z-direction
is shifted based on a plane. For overlapping positions between tiles, the
surface indices should match after accounting for this plane tilt.

We solve for a plane: z = ax + by + c
where (x, y) are coordinates in the mosaic space.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
import yaml
from scipy.optimize import least_squares


def load_tile_config(yaml_path: str) -> Tuple[Dict, List[Dict]]:
    """Load tile configuration from YAML file."""
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)

    metadata = data.get('metadata', {})
    tiles = data.get('tiles', [])

    return metadata, tiles


def load_surface_map(filepath: str, base_dir: str) -> Optional[np.ndarray]:
    """Load surface map for a tile."""
    full_path = Path(base_dir) / filepath
    if not full_path.exists():
        print(f"Warning: Surface map not found: {full_path}")
        return None

    try:
        img = nib.load(str(full_path))
        surface = np.array(img.dataobj)
        # Surface maps are typically 2D (x, y) -> z_index
        if surface.ndim == 3:
            # If 3D, take first slice or squeeze
            surface = surface.squeeze()
        return surface
    except Exception as e:
        print(f"Error loading {full_path}: {e}")
        return None


def get_tile_overlap(
    tile1: Dict, tile2: Dict, tile_size: Tuple[int, int] = (512, 512)
    ) -> Optional[Tuple]:
    """
    Determine if two tiles overlap and return overlap region.
    
    Note: The tile positions (x, y) in the YAML are in the mosaic coordinate system.
    The tile_size should match the actual dimensions of the surface maps.
    
    Returns:
        (x1_start, x1_end, y1_start, y1_end, x2_start, x2_end, y2_start, y2_end)
        or None if no overlap
    """
    x1, y1 = tile1['x'], tile1['y']
    x2, y2 = tile2['x'], tile2['y']

    # Tile size in pixels (width, height)
    # This should match the dimensions of the surface maps
    w, h = tile_size

    # Tile 1 bounds in mosaic space
    x1_min, x1_max = x1, x1 + w
    y1_min, y1_max = y1, y1 + h

    # Tile 2 bounds in mosaic space
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
    # For tile 1
    x1_local_start = overlap_x_min - x1
    x1_local_end = overlap_x_max - x1
    y1_local_start = overlap_y_min - y1
    y1_local_end = overlap_y_max - y1

    # For tile 2
    x2_local_start = overlap_x_min - x2
    x2_local_end = overlap_x_max - x2
    y2_local_start = overlap_y_min - y2
    y2_local_end = overlap_y_max - y2

    return (x1_local_start, x1_local_end, y1_local_start, y1_local_end,
            x2_local_start, x2_local_end, y2_local_start, y2_local_end)


def extract_overlap_surfaces(
    tile1_surf: np.ndarray, tile2_surf: np.ndarray,
    overlap: Tuple
    ) -> Tuple[np.ndarray, np.ndarray]:
    """
    Extract surface values from overlap region.
    
    Note: Surface maps are typically indexed as [y, x] (row, column) in numpy,
    which corresponds to [height, width].
    """
    x1_start, x1_end, y1_start, y1_end, x2_start, x2_end, y2_start, y2_end = overlap

    # Convert to integer indices and ensure they're within bounds
    x1_start = max(0, int(np.floor(x1_start)))
    x1_end = min(tile1_surf.shape[1], int(np.ceil(x1_end)))
    y1_start = max(0, int(np.floor(y1_start)))
    y1_end = min(tile1_surf.shape[0], int(np.ceil(y1_end)))

    x2_start = max(0, int(np.floor(x2_start)))
    x2_end = min(tile2_surf.shape[1], int(np.ceil(x2_end)))
    y2_start = max(0, int(np.floor(y2_start)))
    y2_end = min(tile2_surf.shape[0], int(np.ceil(y2_end)))

    # Ensure overlap regions have the same size (take minimum)
    x_size = min(x1_end - x1_start, x2_end - x2_start)
    y_size = min(y1_end - y1_start, y2_end - y2_start)

    x1_end = x1_start + x_size
    y1_end = y1_start + y_size
    x2_end = x2_start + x_size
    y2_end = y2_start + y_size

    # Extract overlap regions (note: numpy arrays are indexed as [y, x])
    surf1_overlap = tile1_surf[y1_start:y1_end, x1_start:x1_end]
    surf2_overlap = tile2_surf[y2_start:y2_end, x2_start:x2_end]

    return surf1_overlap, surf2_overlap


def plane_z(x: np.ndarray, y: np.ndarray, params: np.ndarray) -> np.ndarray:
    """Compute z = ax + by + c for given x, y coordinates."""
    a, b, c = params
    return a * x + b * y + c


def compute_residuals(params: np.ndarray, overlap_data: List[Dict]) -> np.ndarray:
    """
    Compute residuals for plane fitting.
    
    For each overlapping region between two tiles:
    - Surface from tile 1 at (x1, y1) in mosaic space: z1_local
    - Surface from tile 2 at (x2, y2) in mosaic space: z2_local
    - After plane correction:
      z1_global = z1_local + plane(x1, y1)
      z2_global = z2_local + plane(x2, y2)
    - These should match: z1_global = z2_global
    - Residual: z1_local + plane(x1, y1) - z2_local - plane(x2, y2)
    """
    residuals = []

    for overlap_info in overlap_data:
        tile1_x = overlap_info['tile1_x']
        tile1_y = overlap_info['tile1_y']
        tile2_x = overlap_info['tile2_x']
        tile2_y = overlap_info['tile2_y']

        # Mosaic coordinates for each pixel in overlap
        x1_coords = overlap_info['x1_coords']
        y1_coords = overlap_info['y1_coords']
        x2_coords = overlap_info['x2_coords']
        y2_coords = overlap_info['y2_coords']

        # Surface values
        surf1 = overlap_info['surf1']
        surf2 = overlap_info['surf2']

        # Valid pixels (non-zero, non-NaN)
        valid_mask = (surf1 > 0) & (surf2 > 0) & np.isfinite(surf1) & np.isfinite(surf2)

        if not np.any(valid_mask):
            continue

        # Extract valid values
        x1_valid = x1_coords[valid_mask]
        y1_valid = y1_coords[valid_mask]
        x2_valid = x2_coords[valid_mask]
        y2_valid = y2_coords[valid_mask]
        surf1_valid = surf1[valid_mask]
        surf2_valid = surf2[valid_mask]

        # Compute plane corrections
        z1_plane = plane_z(x1_valid, y1_valid, params)
        z2_plane = plane_z(x2_valid, y2_valid, params)

        # Residual: difference in global z after plane correction
        residual = (surf1_valid + z1_plane) - (surf2_valid + z2_plane)
        residuals.extend(residual)

    return np.array(residuals)


def find_tile_plane(
    yaml_path: str,
    base_dir: Optional[str] = None,
    tile_size: Tuple[int, int] = (512, 512),
    max_tiles: Optional[int] = None,
    subsample: int = 10
    ) -> Tuple[np.ndarray, Dict]:
    """
    Find the plane that describes tile tilt.
    
    Parameters
    ----------
    yaml_path : str
        Path to YAML file with tile configuration
    base_dir : str, optional
        Base directory for surface map files. If None, uses metadata['base_dir']
    tile_size : tuple
        Size of each tile (width, height) in pixels
    max_tiles : int, optional
        Maximum number of tiles to process (for testing)
    subsample : int
        Subsample factor for overlap regions (use every Nth pixel)
    
    Returns
    -------
    params : np.ndarray
        Plane parameters [a, b, c] where z = ax + by + c
    info : dict
        Additional information about the fitting
    """
    # Load configuration
    metadata, tiles = load_tile_config(yaml_path)

    if base_dir is None:
        base_dir = metadata.get('base_dir', '.')

    print(f"Loaded {len(tiles)} tiles from {yaml_path}")
    print(f"Base directory: {base_dir}")

    # Limit tiles if requested
    if max_tiles is not None:
        tiles = tiles[:max_tiles]
        print(f"Processing first {len(tiles)} tiles")

    # Load surface maps
    print("Loading surface maps...")
    tile_surfaces = {}
    for tile in tiles:
        filepath = tile['filepath']
        surface = load_surface_map(filepath, base_dir)
        if surface is not None:
            tile_surfaces[tile['tile_number']] = {
                'surface': surface,
                'tile': tile,
                'shape': surface.shape
            }
            # Update tile_size from actual surface map if available
            if tile_size == (512, 512):  # Default, try to get actual size
                h, w = surface.shape[:2]
                tile_size = (w, h)

    print(f"Loaded {len(tile_surfaces)} surface maps")
    print(f"Tile size: {tile_size}")

    # Find overlapping regions
    print("Finding overlapping regions...")
    overlap_data = []
    tile_list = list(tile_surfaces.values())

    for i, tile1_info in enumerate(tile_list):
        for j, tile2_info in enumerate(tile_list[i + 1:], start=i + 1):
            tile1 = tile1_info['tile']
            tile2 = tile2_info['tile']

            overlap = get_tile_overlap(tile1, tile2, tile_size)
            if overlap is None:
                continue

            surf1 = tile1_info['surface']
            surf2 = tile2_info['surface']

            # Extract overlap surfaces (this may adjust the bounds)
            surf1_overlap, surf2_overlap = extract_overlap_surfaces(surf1, surf2,
                                                                    overlap)

            # Skip if overlap is too small
            if surf1_overlap.size < 100:
                continue

            # Get the actual overlap bounds after extraction
            # The extract function ensures both surfaces have the same size
            h_overlap, w_overlap = surf1_overlap.shape

            # Get original overlap bounds
            x1_start, x1_end, y1_start, y1_end, x2_start, x2_end, y2_start, y2_end = (
            overlap)

            # Compute actual local coordinates used (after bounds adjustment in extract function)
            x1_local_start = max(0, int(np.floor(x1_start)))
            y1_local_start = max(0, int(np.floor(y1_start)))
            x2_local_start = max(0, int(np.floor(x2_start)))
            y2_local_start = max(0, int(np.floor(y2_start)))

            # Mosaic coordinates for tile 1 overlap region
            # Create coordinate arrays matching the extracted surface shape
            x1_local_coords = np.arange(x1_local_start, x1_local_start + w_overlap)
            y1_local_coords = np.arange(y1_local_start, y1_local_start + h_overlap)
            x1_mesh, y1_mesh = np.meshgrid(x1_local_coords + tile1['x'],
                                           y1_local_coords + tile1['y'])

            # Mosaic coordinates for tile 2 overlap region
            x2_local_coords = np.arange(x2_local_start, x2_local_start + w_overlap)
            y2_local_coords = np.arange(y2_local_start, y2_local_start + h_overlap)
            x2_mesh, y2_mesh = np.meshgrid(x2_local_coords + tile2['x'],
                                           y2_local_coords + tile2['y'])

            # Subsample if requested
            if subsample > 1:
                mask = np.zeros_like(surf1_overlap, dtype=bool)
                mask[::subsample, ::subsample] = True
                surf1_overlap = surf1_overlap[mask]
                surf2_overlap = surf2_overlap[mask]
                x1_mesh = x1_mesh[mask]
                y1_mesh = y1_mesh[mask]
                x2_mesh = x2_mesh[mask]
                y2_mesh = y2_mesh[mask]

            overlap_data.append({
                'tile1_x': tile1['x'],
                'tile1_y': tile1['y'],
                'tile2_x': tile2['x'],
                'tile2_y': tile2['y'],
                'x1_coords': x1_mesh,
                'y1_coords': y1_mesh,
                'x2_coords': x2_mesh,
                'y2_coords': y2_mesh,
                'surf1': surf1_overlap,
                'surf2': surf2_overlap,
            })

    print(f"Found {len(overlap_data)} overlapping regions")

    if len(overlap_data) == 0:
        raise ValueError("No overlapping regions found. Cannot fit plane.")

    # Fit plane using least squares
    print("Fitting plane...")
    initial_params = np.array([0.0, 0.0, 0.0])  # [a, b, c]

    result = least_squares(
        compute_residuals,
        initial_params,
        args=(overlap_data,),
        method='lm',  # Levenberg-Marquardt
        verbose=2
    )

    params = result.x
    print(f"\nFitted plane parameters:")
    print(f"  a (x coefficient): {params[0]:.6e}")
    print(f"  b (y coefficient): {params[1]:.6e}")
    print(f"  c (constant):     {params[2]:.6f}")
    print(
        f"\nPlane equation: z = {params[0]:.6e} * x + {params[1]:.6e} * y + {params[2]:.6f}")

    # Compute statistics
    residuals = compute_residuals(params, overlap_data)
    rmse = np.sqrt(np.mean(residuals ** 2))
    print(f"\nRMSE: {rmse:.4f} pixels")
    print(f"Mean absolute error: {np.mean(np.abs(residuals)):.4f} pixels")
    print(f"Max absolute error: {np.max(np.abs(residuals)):.4f} pixels")

    info = {
        'rmse': rmse,
        'residuals': residuals,
        'num_overlaps': len(overlap_data),
        'num_residuals': len(residuals),
        'tile_size': tile_size,
    }

    return params, info


def visualize_plane(
    params: np.ndarray, tiles: List[Dict], output_path: Optional[str] = None
    ):
    """Visualize the fitted plane over the tile layout."""
    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111, projection='3d')

    # Extract tile positions
    x_positions = [t['x'] for t in tiles]
    y_positions = [t['y'] for t in tiles]

    # Create grid for plane visualization
    x_min, x_max = min(x_positions), max(x_positions)
    y_min, y_max = min(y_positions), max(y_positions)

    x_range = x_max - x_min
    y_range = y_max - y_min

    # Extend range a bit
    x_min -= x_range * 0.1
    x_max += x_range * 0.1
    y_min -= y_range * 0.1
    y_max += y_range * 0.1

    # Create meshgrid
    x_grid = np.linspace(x_min, x_max, 50)
    y_grid = np.linspace(y_min, y_max, 50)
    X, Y = np.meshgrid(x_grid, y_grid)

    # Compute plane
    Z = plane_z(X, Y, params)

    # Plot plane
    ax.plot_surface(X, Y, Z, alpha=0.3, color='blue', label='Fitted plane')

    # Plot tile positions
    z_tiles = plane_z(np.array(x_positions), np.array(y_positions), params)
    ax.scatter(x_positions, y_positions, z_tiles,
               c='red', s=50, label='Tile positions', alpha=0.6)

    ax.set_xlabel('X (mosaic space)')
    ax.set_ylabel('Y (mosaic space)')
    ax.set_zlabel('Z (plane correction)')
    ax.set_title('Fitted Plane for Tile Tilt Correction')
    ax.legend()

    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"Saved visualization to {output_path}")
    else:
        plt.show()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Find plane that describes tile tilt')
    parser.add_argument('yaml_path', type=str, help='Path to tile configuration YAML')
    parser.add_argument('--base-dir', type=str, default=None,
                        help='Base directory for surface maps (default: from YAML metadata)')
    parser.add_argument('--tile-size', type=int, nargs=2, default=[512, 512],
                        help='Tile size [width, height] in pixels')
    parser.add_argument('--max-tiles', type=int, default=None,
                        help='Maximum number of tiles to process (for testing)')
    parser.add_argument('--subsample', type=int, default=10,
                        help='Subsample factor for overlap regions')
    parser.add_argument('--visualize', action='store_true',
                        help='Create visualization of fitted plane')
    parser.add_argument('--output', type=str, default=None,
                        help='Output file for plane parameters (JSON)')

    args = parser.parse_args()

    # Find plane
    params, info = find_tile_plane(
        args.yaml_path,
        base_dir=args.base_dir,
        tile_size=tuple(args.tile_size),
        max_tiles=args.max_tiles,
        subsample=args.subsample
    )

    # Save results
    if args.output:
        import json

        result = {
            'plane_parameters': {
                'a': float(params[0]),
                'b': float(params[1]),
                'c': float(params[2])
            },
            'plane_equation': f"z = {params[0]:.6e} * x + {params[1]:.6e} * y + {params[2]:.6f}",
            'statistics': {
                'rmse': float(info['rmse']),
                'mean_abs_error': float(np.mean(np.abs(info['residuals']))),
                'max_abs_error': float(np.max(np.abs(info['residuals']))),
                'num_overlaps': info['num_overlaps'],
                'num_residuals': info['num_residuals'],
            }
        }
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved results to {args.output}")

    # Visualize
    if args.visualize:
        metadata, tiles = load_tile_config(args.yaml_path)
        vis_path = args.output.replace('.json',
                                       '_plane.png') if args.output else 'plane_visualization.png'
        visualize_plane(params, tiles, vis_path)

