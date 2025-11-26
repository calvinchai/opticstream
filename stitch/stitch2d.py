from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union

import dask.array as da
from nibabel.nifti1 import Nifti1Image
import numpy as np
from numpy.typing import ArrayLike
from helper import _to_dask, _normalize_overlap, _blending_ramp
from helper import *

@dataclass
class TileInfo:
    """Tile information with coordinates and image data."""
    x: int
    y: int
    image: ArrayLike


def stitch_2d(
    map: Sequence[Union[Dict[str, Union[int, ArrayLike]], TileInfo]],
    overlap: Union[int, Tuple[int, int]] = 0,
    circular_mean: bool = False,
    chunk: Union[str, Tuple[int, ...], None] = "auto",
) -> da.Array:
    """
    Blend 2D tiles into a single mosaic using coordinate-based placement.

    Parameters
    ----------
    map
        Sequence of tiles, where each tile is a dict or dataclass with fields:
        - x: int - row coordinate (top-left corner)
        - y: int - column coordinate (top-left corner)
        - image: ArrayLike - the tile image data (numpy or dask array)
    overlap
        Overlap between adjacent tiles in pixels. Provide an int for both axes or a
        (row_overlap, col_overlap) tuple.
    circular_mean
        If True, treat inputs as angles (degrees) and blend using a circular mean.
    chunk
        Chunk size for output dask arrays. Can be "auto", None, or a tuple.

    Returns
    -------
    da.Array
        The stitched mosaic as a dask array.
    """
    if not map:
        raise ValueError("map must contain at least one tile.")

    # Normalize tile entries to TileInfo-like objects
    tiles = []
    for entry in map:
        if isinstance(entry, dict):
            tiles.append(TileInfo(x=entry["x"], y=entry["y"], image=entry["image"]))
        elif isinstance(entry, TileInfo):
            tiles.append(entry)
        else:
            raise ValueError("Each tile must be a dict or TileInfo with 'x', 'y', 'image' fields.")

    # Get first tile to determine shape
    first_tile_img = _to_dask(tiles[0].image)
    if first_tile_img.ndim < 2:
        raise ValueError("Tiles must be at least 2D.")
    
    tile_shape = first_tile_img.shape
    tile_height, tile_width = tile_shape[:2]
    row_overlap, col_overlap = _normalize_overlap(overlap)

    # Calculate canvas size from coordinates
    x_coords = [t.x for t in tiles]
    y_coords = [t.y for t in tiles]
    x_min, x_max = min(x_coords), max(x_coords)
    y_min, y_max = min(y_coords), max(y_coords)
    
    # Canvas size: from min coord to max coord + tile size
    full_height = x_max + tile_height - x_min 
    full_width = y_max + tile_width - y_min 
    print(full_height, full_width)
    # Create blending ramp
    blend = _blending_ramp((tile_height, tile_width), (row_overlap, col_overlap))

    # Extract coords and images
    coords = [(t.x - x_min, t.y - y_min) for t in tiles]
    tile_images = [t.image for t in tiles]

    # Use unified helper function
    result = stitch_tiles(
        tiles,
        (full_height,full_width),
        blend,
        None,
        circular_mean
    )
    return result



import os
import dask.array as da
import nibabel as nib

from collections import namedtuple

TileInfo = namedtuple("TileInfo", ["x", "y", "image"])


# txt_path = '/homes/5/kc1708/localhome/project/psoct-analysis/I55_1119/processed/Mosaic_003.txt'
txt_path = '/space/zircon/5/users/kchai/I55_slice6/analysis/processed/Mosaic_depth001_slice001.txt'
base_dir = '/space/zircon/5/users/kchai/I55_slice6/analysis/processed/'
yaml_path = '/autofs/homes/005/kc1708/localhome/code/psoct-renew/tile_coords_export_004.yaml'
import yaml
tiles_y = yaml.load(open(yaml_path),Loader=yaml.FullLoader)
tiles = []
for tile in tiles_y:
    fname_ori = tile['filepath']
    fname_ori = fname_ori.replace('002','004')
    # fname_ori = fname_ori.replace('aip','ori')
    img_path = os.path.join(base_dir, fname_ori)
    if not os.path.exists(img_path):
        print(f"Missing file: {img_path}")
        continue
    x = round(tile ['x'])
    y= round(tile['y'])
    try:
        img = nib.load(img_path)
        arr = img.dataobj
        darr = da.from_array(arr, chunks="auto")
        tile = TileInfo(x=x, y=y, image=darr)
        tiles.append(tile)
    
    except Exception as e:
        print(f"Error loading {img_path}: {e}")
stitched = stitch_2d(tiles, 50, False).compute()
nib.save(Nifti1Image(stitched,np.eye(4)),"mosaic_004_aip.nii")
exit()

tiles = []
with open(txt_path, 'r') as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Expecting: filename; ; (x, y)
        parts = line.split(";")
        
        if len(parts) < 3:
            continue
        fname = parts[0].strip()
        coord_part = parts[2].strip()
        # coords like (123.4, 567.8)
        coord_part = coord_part.strip("()")
        try:
            x_str, y_str = coord_part.split(",")
            x = int(float(x_str))
            y = int(float(y_str))
        except Exception as e:
            continue  # skip malformed lines

        # Replace '_aip' with '_ori' in filename
        fname_ori = fname.replace('aip', 'ori')
        
        img_path = os.path.join(base_dir, fname_ori)
        if not os.path.exists(img_path):
            print(f"Missing file: {img_path}")
            continue

        try:
            img = nib.load(img_path)
            arr = img.dataobj
            darr = da.from_array(arr, chunks="auto")
            tile = TileInfo(x=x, y=y, image=darr)
            tiles.append(tile)
        except Exception as e:
            print(f"Error loading {img_path}: {e}")
stitched = stitch_2d(tiles, 40, True).compute()
nib.save(Nifti1Image(stitched,np.eye(4)),"test2.nii")
from tifffile import imwrite
imwrite('test2.tiff', np.transpose(stitched))