#!/usr/bin/env python3
"""
Filter tiles in a mosaic YAML file based on signal threshold.

This script filters tiles from a YAML file where each tile has avg_signal
above a specified threshold. The script outputs a filtered YAML file
containing only the tiles that meet the threshold criteria.
"""

from typing import Dict, List, Tuple
import sys
import yaml
from cyclopts import App

app = App(name="filter_tiles_by_signal")


def load_yaml_config(yaml_path: str) -> Tuple[Dict, List[Dict]]:
    """Load tile configuration from YAML file."""
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    metadata = data.get('metadata', {})
    tiles = data.get('tiles', [])
    
    return metadata, tiles


def write_filtered_yaml(output_path: str, metadata: Dict, selected_tiles: List[Dict]):
    """Write filtered YAML with metadata and selected tiles."""
    output_data = {
        'metadata': metadata,
        'tiles': selected_tiles
    }
    
    with open(output_path, 'w') as f:
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)


@app.default
def filter_tiles_by_signal(
    input_yaml: str,
    output_yaml: str,
    signal_threshold: float,
):
    """
    Filter tiles in a mosaic YAML file based on signal threshold.
    
    Filters tiles where avg_signal is above the specified threshold and
    outputs a filtered YAML with only the selected tiles.
    
    Parameters
    ----------
    input_yaml : str
        Path to input YAML file containing tile configuration
    output_yaml : str
        Path to output YAML file with filtered tiles
    signal_threshold : float
        Minimum avg_signal threshold that each tile must meet
    """
    print(f"Loading YAML from {input_yaml}")
    metadata, tiles = load_yaml_config(input_yaml)
    print(f"Loaded {len(tiles)} tiles")
    
    if not tiles:
        print("No tiles found in YAML file", file=sys.stderr)
        return
    
    # Filter tiles by signal threshold
    print(f"Filtering tiles with avg_signal >= {signal_threshold}...")
    filtered_tiles = [
        tile for tile in tiles
        if tile.get('avg_signal', 0) >= signal_threshold
    ]
    
    print(f"Filtered {len(filtered_tiles)} tiles from {len(tiles)} total tiles")
    
    if not filtered_tiles:
        print(f"No tiles found with avg_signal >= {signal_threshold}", file=sys.stderr)
        return
    
    # Write output YAML
    print(f"Writing filtered YAML to {output_yaml}")
    write_filtered_yaml(output_yaml, metadata, filtered_tiles)
    print("Done!")


if __name__ == '__main__':
    app()

