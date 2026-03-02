#!/usr/bin/env python3
"""
Find a consecutive tile region in a mosaic YAML file.

This script finds a rectangular region (e.g., 3x3) of consecutive tiles where:
1. All tiles are consecutive (form a rectangular grid with no gaps)
2. Each tile has avg_signal above a specified threshold
3. The region is located in the center-most area of the mosaic

The script outputs a filtered YAML file containing only the selected tiles.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys
import yaml
import numpy as np
from cyclopts import App

app = App(name="find_tile_region")


def load_yaml_config(yaml_path: str) -> Tuple[Dict, List[Dict]]:
    """Load tile configuration from YAML file."""
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    metadata = data.get('metadata', {})
    tiles = data.get('tiles', [])
    
    return metadata, tiles


def find_unique_coordinates(tiles: List[Dict], tolerance: float = 1.0) -> Tuple[List[float], List[float], Dict[float, int], Dict[float, int]]:
    """
    Find unique x and y coordinates from tiles, handling floating-point matching with tolerance.
    
    Returns:
        unique_x: Sorted list of unique x coordinates
        unique_y: Sorted list of unique y coordinates
        x_to_col: Mapping from x coordinate to column index
        y_to_row: Mapping from y coordinate to row index
    """
    # Collect all x and y coordinates
    x_coords = []
    y_coords = []
    
    for tile in tiles:
        x_coords.append(tile['x'])
        y_coords.append(tile['y'])
    
    # Cluster coordinates within tolerance (handles overlapping tiles)
    def cluster_coordinates(all_coords: List[float], tolerance: float) -> Tuple[List[float], Dict[float, int]]:
        """
        Cluster coordinates within tolerance, computing cluster centers as means.
        This handles the case where tiles have slight coordinate variations due to overlap.
        Similar approach to process_tile_coord.py but with tolerance for alignment.
        """
        if not all_coords:
            return [], {}
        
        # Get unique coordinates
        unique_coords = sorted(set(all_coords))
        clusters = []  # List of lists, each containing coordinates in a cluster
        
        # Assign each unique coordinate to a cluster
        for coord in unique_coords:
            # Find closest existing cluster (by checking distance to any cluster member)
            # This ensures only truly adjacent coordinates are grouped together
            best_cluster_idx = None
            min_distance = tolerance + 1.0
            
            for i, cluster_members in enumerate(clusters):
                # Check distance to each member of the cluster, not the center
                # This prevents chaining non-adjacent coordinates together
                for member in cluster_members:
                    distance = abs(coord - member)
                    if distance <= tolerance and distance < min_distance:
                        min_distance = distance
                        best_cluster_idx = i
                        break  # Found a match, no need to check other members
            
            if best_cluster_idx is not None:
                # Add to existing cluster
                clusters[best_cluster_idx].append(coord)
            else:
                # Create new cluster
                clusters.append([coord])
        
        # Compute cluster centers as means
        cluster_centers = [np.mean(members) for members in clusters]
        
        # Sort clusters by center value
        sorted_indices = sorted(range(len(cluster_centers)), key=lambda i: cluster_centers[i])
        sorted_centers = [cluster_centers[i] for i in sorted_indices]
        
        # Create mapping from old cluster index to new sorted index
        old_to_new = {old_idx: new_idx for new_idx, old_idx in enumerate(sorted_indices)}
        
        # Build mapping from coordinates to cluster indices
        coord_to_cluster = {}
        for old_idx, members in enumerate(clusters):
            new_idx = old_to_new[old_idx]
            for coord in members:
                coord_to_cluster[coord] = new_idx
        
        # Map all original coordinates (including duplicates) to clusters
        final_mapping = {}
        for coord in all_coords:
            if coord in coord_to_cluster:
                final_mapping[coord] = coord_to_cluster[coord]
            else:
                # Find closest cluster center (shouldn't happen for unique coords, but handle duplicates)
                closest_idx = min(range(len(sorted_centers)), 
                                 key=lambda i: abs(coord - sorted_centers[i]))
                final_mapping[coord] = closest_idx
        
        return sorted_centers, final_mapping
    
    unique_x, x_to_col = cluster_coordinates(x_coords, tolerance)
    unique_y, y_to_row = cluster_coordinates(y_coords, tolerance)
    
    return unique_x, unique_y, x_to_col, y_to_row


def build_grid(tiles: List[Dict], unique_x: List[float], unique_y: List[float], 
               x_to_col: Dict[float, int], y_to_row: Dict[float, int]) -> List[List[Optional[Dict]]]:
    """
    Build a 2D grid of tiles.
    
    Returns:
        grid: 2D list where grid[row][col] = tile or None
    """
    num_rows = len(unique_y)
    num_cols = len(unique_x)
    
    # Initialize grid with None
    grid = [[None for _ in range(num_cols)] for _ in range(num_rows)]
    
    # Place tiles in grid
    for tile in tiles:
        x = tile['x']
        y = tile['y']
        
        # Get column and row indices (coordinate mapping handles all cases)
        col = x_to_col[x]
        row = y_to_row[y]
        
        # Warn if multiple tiles map to same position (shouldn't happen with proper tolerance)
        if grid[row][col] is not None:
            print(f"Warning: Multiple tiles map to grid position ({row}, {col}): "
                  f"{grid[row][col].get('filepath', 'unknown')} and {tile.get('filepath', 'unknown')}")
        
        grid[row][col] = tile
    
    return grid


def find_valid_regions(grid: List[List[Optional[Dict]]], num_rows: int, num_cols: int, 
                      signal_threshold: float, unique_x: List[float], unique_y: List[float]) -> List[Dict]:
    """
    Find all valid N×M regions where all tiles exist and have avg_signal >= threshold.
    
    Returns:
        List of region dictionaries with keys: start_row, start_col, tiles, center_x, center_y
    """
    grid_height = len(grid)
    grid_width = len(grid[0]) if grid else 0
    
    valid_regions = []
    
    # Try all possible starting positions
    for start_row in range(grid_height - num_rows + 1):
        for start_col in range(grid_width - num_cols + 1):
            # Check if region is valid
            region_tiles = []
            all_valid = True
            
            for r in range(start_row, start_row + num_rows):
                for c in range(start_col, start_col + num_cols):
                    tile = grid[r][c]
                    
                    # Check if tile exists
                    if tile is None:
                        all_valid = False
                        break
                    
                    # Check if signal is above threshold
                    if tile.get('avg_signal', 0) < signal_threshold:
                        all_valid = False
                        break
                    
                    region_tiles.append(tile)
                
                if not all_valid:
                    break
            
            if all_valid:
                # Calculate region center
                # Get coordinates of corners
                top_left_x = unique_x[start_col]
                top_left_y = unique_y[start_row]
                bottom_right_x = unique_x[start_col + num_cols - 1]
                bottom_right_y = unique_y[start_row + num_rows - 1]
                
                center_x = (top_left_x + bottom_right_x) / 2.0
                center_y = (top_left_y + bottom_right_y) / 2.0
                
                valid_regions.append({
                    'start_row': start_row,
                    'start_col': start_col,
                    'tiles': region_tiles,
                    'center_x': center_x,
                    'center_y': center_y
                })
    
    return valid_regions


def find_center_most_region(valid_regions: List[Dict], mosaic_center_x: float, mosaic_center_y: float) -> Optional[Dict]:
    """
    Find the region whose center is closest to the mosaic center.
    
    Returns:
        The region dictionary closest to center, or None if no regions found
    """
    if not valid_regions:
        return None
    
    min_distance = float('inf')
    best_region = None
    
    for region in valid_regions:
        dx = region['center_x'] - mosaic_center_x
        dy = region['center_y'] - mosaic_center_y
        distance = np.sqrt(dx**2 + dy**2)
        
        if distance < min_distance:
            min_distance = distance
            best_region = region
    
    return best_region


def write_filtered_yaml(output_path: str, metadata: Dict, selected_tiles: List[Dict]):
    """Write filtered YAML with metadata and selected tiles."""
    output_data = {
        'metadata': metadata,
        'tiles': selected_tiles
    }
    
    with open(output_path, 'w') as f:
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)


@app.default
def find_tile_region(
    input_yaml: str,
    output_yaml: str,
    signal_threshold: float,
    grid_size: str = "3x3",
    coordinate_tolerance: float = 100,
):
    """
    Find a consecutive tile region in a mosaic YAML file.
    
    Finds a rectangular region (e.g., 3x3) of consecutive tiles where all tiles
    have avg_signal above the threshold, and outputs a filtered YAML with only
    the selected tiles.
    
    Parameters
    ----------
    input_yaml : str
        Path to input YAML file containing tile configuration
    output_yaml : str
        Path to output YAML file with filtered tiles
    grid_size : str, default="3x3"
        Grid size as NxM (e.g., 3x3 for 3 columns by 3 rows)
    signal_threshold : float
        Minimum avg_signal threshold that each tile must meet
    coordinate_tolerance : float, default=1.0
        Tolerance for coordinate matching when building the grid
    """
    # Parse grid size
    try:
        parts = grid_size.lower().split('x')
        if len(parts) != 2:
            raise ValueError("Grid size must be in format NxM (e.g., 3x3)")
        num_cols = int(parts[0])
        num_rows = int(parts[1])
        if num_cols <= 0 or num_rows <= 0:
            raise ValueError("Grid dimensions must be positive")
    except ValueError as e:
        print(f"Error parsing grid size: {e}", file=sys.stderr)
        return
    
    print(f"Loading YAML from {input_yaml}")
    metadata, tiles = load_yaml_config(input_yaml)
    print(f"Loaded {len(tiles)} tiles")
    
    if not tiles:
        print("No tiles found in YAML file", file=sys.stderr)
        return
    
    # Find unique coordinates
    print("Building coordinate grid...")
    unique_x, unique_y, x_to_col, y_to_row = find_unique_coordinates(tiles, coordinate_tolerance)
    print(f"Found {len(unique_x)} unique x coordinates, {len(unique_y)} unique y coordinates")
    
    # Build grid
    grid = build_grid(tiles, unique_x, unique_y, x_to_col, y_to_row)
    
    # Calculate mosaic center
    min_x = min(unique_x)
    max_x = max(unique_x)
    min_y = min(unique_y)
    max_y = max(unique_y)
    mosaic_center_x = (min_x + max_x) / 2.0
    mosaic_center_y = (min_y + max_y) / 2.0
    print(f"Mosaic center: ({mosaic_center_x:.2f}, {mosaic_center_y:.2f})")
    
    # Find valid regions
    print(f"Searching for {num_cols}x{num_rows} regions with avg_signal >= {signal_threshold}...")
    valid_regions = find_valid_regions(grid, num_rows, num_cols, signal_threshold, unique_x, unique_y)
    print(f"Found {len(valid_regions)} valid regions")
    
    if not valid_regions:
        print(f"No valid regions found matching criteria", file=sys.stderr)
        return
    
    # Find center-most region
    best_region = find_center_most_region(valid_regions, mosaic_center_x, mosaic_center_y)
    
    if best_region is None:
        print("Error: Could not select best region", file=sys.stderr)
        return
    
    print(f"Selected region at grid position ({best_region['start_row']}, {best_region['start_col']})")
    print(f"Region center: ({best_region['center_x']:.2f}, {best_region['center_y']:.2f})")
    print(f"Selected {len(best_region['tiles'])} tiles")
    
    # Write output YAML
    print(f"Writing filtered YAML to {output_yaml}")
    write_filtered_yaml(output_yaml, metadata, best_region['tiles'])
    print("Done!")


if __name__ == '__main__':
    app()

