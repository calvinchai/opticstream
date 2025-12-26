# Finding the Tile Tilt Plane

This document explains how to find the plane that describes how tiles are tilted in the z-direction.

## Problem Description

Each tile in the mosaic has a surface map that indicates where the surface is located for each (x, y) position within that tile. However, each tile volume is tilted by a plane, meaning the z-direction is shifted based on the plane.

For overlapping positions between tiles, the surface indices should match after accounting for this plane tilt. We use this constraint to solve for the plane parameters.

## Plane Equation

The plane is described by:
```
z = a*x + b*y + c
```

where:
- `(x, y)` are coordinates in the mosaic space
- `z` is the z-shift/correction at that position
- `a`, `b`, `c` are the plane parameters we want to find

## Usage

### Basic Usage

```bash
python find_tile_plane.py mosaic_001_surf.yaml
```

### With Options

```bash
python find_tile_plane.py mosaic_001_surf.yaml \
    --base-dir /path/to/tiles \
    --tile-size 512 512 \
    --subsample 10 \
    --max-tiles 50 \
    --output plane_params.json \
    --visualize
```

### Parameters

- `yaml_path`: Path to the YAML file containing tile configuration (required)
- `--base-dir`: Base directory for surface map files. If not specified, uses `metadata.base_dir` from YAML
- `--tile-size`: Tile size in pixels as `[width, height]` (default: `512 512`)
- `--subsample`: Subsample factor for overlap regions to speed up computation (default: `10`)
- `--max-tiles`: Maximum number of tiles to process (useful for testing, default: all tiles)
- `--output`: Output JSON file for plane parameters
- `--visualize`: Create a 3D visualization of the fitted plane

## Output

The script outputs:
1. Plane parameters: `a`, `b`, `c`
2. Plane equation: `z = a*x + b*y + c`
3. Statistics: RMSE, mean absolute error, max absolute error
4. (Optional) JSON file with all results
5. (Optional) 3D visualization plot

## How It Works

1. **Load Configuration**: Reads tile positions and metadata from YAML
2. **Load Surface Maps**: Loads surface map NIfTI files for each tile
3. **Find Overlaps**: Identifies overlapping regions between pairs of tiles
4. **Extract Overlap Data**: Extracts surface values from overlapping regions
5. **Fit Plane**: Uses least squares optimization to find plane parameters that minimize the difference in surface values at overlapping positions

## Example Output

```
Loaded 560 tiles from mosaic_001_surf.yaml
Base directory: /autofs/space/zircon_007/users/psoct-pipeline/sub-I80
Loaded 560 surface maps
Tile size: (512, 512)
Found 1234 overlapping regions
Fitting plane...

Fitted plane parameters:
  a (x coefficient): 1.234567e-05
  b (y coefficient): -2.345678e-06
  c (constant):     42.123456

Plane equation: z = 1.234567e-05 * x + -2.345678e-06 * y + 42.123456

RMSE: 0.1234 pixels
Mean absolute error: 0.0987 pixels
Max absolute error: 0.5678 pixels
```

## Notes

- The tile positions in the YAML are in the mosaic coordinate system
- Surface maps are 2D arrays where each pixel (x, y) contains the z-index of the surface
- The plane correction is applied at each pixel's global position: `z_global = z_local + plane(x_global, y_global)`
- For overlapping regions, `z_global` should match between tiles after plane correction

## Troubleshooting

1. **No overlapping regions found**: 
   - Check that tile positions are correct
   - Verify tile_size matches actual surface map dimensions
   - Tiles may not actually overlap

2. **Large RMSE**:
   - The plane model may not be sufficient (consider higher-order terms)
   - There may be systematic errors in surface detection
   - Tiles may have different tilts (consider per-tile planes)

3. **Surface maps not found**:
   - Check that `base_dir` is correct
   - Verify file paths in YAML match actual file locations
   - Check file permissions



