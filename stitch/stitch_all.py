from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union

import dask.array as da
import numpy as np
from numpy.typing import ArrayLike


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

    # Create blending ramp
    blend = _blending_ramp(tile_height, tile_width, row_overlap, col_overlap)

    # Extract coords and images
    coords = [(t.x - x_min, t.y - y_min) for t in tiles]
    tile_images = [t.image for t in tiles]

    # Use unified helper function
    result = stitch_tiles(
        coords=coords,
        tiles=tile_images,
        full_width=full_width,
        full_height=full_height,
        blend_ramp=blend,
        tile_size=(tile_width, tile_height),
        chunk=chunk,
        depth=None,
        circular_mean=circular_mean,
    )
    
    return result


def _normalize_overlap(
    overlap: Union[int, Tuple[int, int]],
) -> Tuple[int, int]:
    """Normalize overlap parameter to (row_overlap, col_overlap) tuple."""
    if isinstance(overlap, tuple):
        if len(overlap) != 2:
            raise ValueError("overlap tuple must be (row_overlap, col_overlap).")
        row_overlap, col_overlap = overlap
    else:
        row_overlap = col_overlap = int(overlap)

    if row_overlap < 0 or col_overlap < 0:
        raise ValueError("Overlap must be non-negative.")
    return row_overlap, col_overlap


def _blending_ramp(
    shape: tuple(int),
    overlap: tuple(int),
) -> np.ndarray:
    """
    Create a separable 2D blending ramp similar to build_slice().
    """
    wx = np.ones(shape[0], dtype=np.float32)
    wy = np.ones(shape[1], dtype=np.float32)
    if overlap[0] > 0:
        wx[:overlap[0]] = np.linspace(0, 1, overlap[0], dtype=np.float32)
        wx[-overlap[0]:] = np.linspace(1, 0, overlap[0], dtype=np.float32)
    if overlap[1] > 0:
        wy[:overlap[1]] = np.linspace(0, 1, overlap[1], dtype=np.float32)
        wy[-overlap[1]:] = np.linspace(1, 0, overlap[1], dtype=np.float32)
    return np.outer(wx, wy)


def _to_dask(arr: ArrayLike) -> da.Array:
    """Convert array-like to dask array."""
    if isinstance(arr, da.Array):
        return arr
    return da.from_array(np.asarray(arr), chunks="auto")

def stitch_tiles(
    tile_info: Sequence[TileInfo],
    full_shape: tuple(int),# None for auto, tuple of int for chunk size
    blend_ramp: ArrayLike,
    circular_mean: Sequence[int]| None = None, # None for linear, tuple of int for last dimension index for circular mean
    chunk_size: tuple(int)| None = None,
) -> Union[da.Array, List[da.Array]]:
    """
    Stitch tiles with coordinate-based placement.
    """
    tile_shape = tile_info[0].image.shape[:2]
    blend_ramp = da.array(blend_ramp)
    canvas = da.zeros(out_shape, chunks=chunk, dtype=np.float32)
    chunk_width,chunk_height = chunk_size[:2]
    weight = da.zeros((full_width, full_height), chunks=chunk[:2], dtype=np.float32)
    
    # Map blocks (chunks) to lists of tiles and associated blending ramps/weights
    from collections import defaultdict

    # Unpack or infer output shape and chunking
    if chunk_size is not None:
        chunk = chunk_size
    else:
        chunk = "auto"
    if full_shape is not None:
        out_shape = full_shape
    else:
        # fallback: get max placement + tile shapes
        max_y = max(y for y, x in [tile.position for tile in tile_info])
        max_x = max(x for y, x in [tile.position for tile in tile_info])
        h, w = tile_info[0].image.shape[:2]
        out_shape = (max_y + h, max_x + w)

    # Prepare block (chunk) mapping for accumulation
    block_tiles = defaultdict(list)
    block_weights = defaultdict(list)

    # For each tile, place its weighted image into all affected chunks
    for tile in tile_info:
        y, x = tile.position
        img = _to_dask(tile.image)
        h, w = img.shape[:2]
        y_end = y + h
        x_end = x + w
        blend = blend_ramp
        img = img * blend[..., None] if img.ndim == 3 else img * blend

        # Calculate chunk indices affected by this tile
        # This requires knowing the chunk/grid structure
        rows = range(y // chunk[0], (y_end - 1) // chunk[0] + 1)
        cols = range(x // chunk[1], (x_end - 1) // chunk[1] + 1)

        for row in rows:
            for col in cols:
                key = (row, col)
                chunk_y0 = row * chunk[0]
                chunk_x0 = col * chunk[1]
                chunk_y1 = min(out_shape[0], chunk_y0 + chunk[0])
                chunk_x1 = min(out_shape[1], chunk_x0 + chunk[1])

                # Compute overlap region between tile and chunk
                yy0 = max(y, chunk_y0)
                yy1 = min(y_end, chunk_y1)
                xx0 = max(x, chunk_x0)
                xx1 = min(x_end, chunk_x1)

                if yy1 <= yy0 or xx1 <= xx0:
                    continue  # No overlap in this chunk

                # Slices within tile and within chunk
                tile_slice_y = slice(yy0 - y, yy1 - y)
                tile_slice_x = slice(xx0 - x, xx1 - x)
                chunk_slice_y = slice(yy0 - chunk_y0, yy1 - chunk_y0)
                chunk_slice_x = slice(xx0 - chunk_x0, xx1 - chunk_x0)

                # Extract relevant patch from tile and blend
                tile_img_patch = img[tile_slice_y, tile_slice_x]
                blend_patch = blend[tile_slice_y, tile_slice_x]
                block_tiles[key].append(tile_img_patch)
                block_weights[key].append(blend_patch)

    return out_shape, chunk, block_tiles, block_weights
def stitch_tiles(
    coords: Sequence[Tuple[int, int]],
    tiles: Sequence[ArrayLike],
    full_width: int,
    full_height: int,
    blend_ramp: ArrayLike,
    tile_size: Tuple[int, int],
    chunk: Union[str, Tuple[int, ...], None] = "auto",
    depth: Optional[int] = None,
    circular_mean: bool = False,
) -> Union[da.Array, List[da.Array]]:
    """
    Unified helper function to stitch tiles with coordinate-based placement.
    
    Supports both 2D and 3D data with chunk-aligned processing for efficiency.
    Can be used by both 2D and 3D stitching pipelines.

    Parameters
    ----------
    coords
        Sequence of (x, y) coordinate tuples for each tile (top-left corner).
    tiles
        Sequence of tile images (numpy or dask arrays). All tiles must have the same shape.
    full_width
        Full width of the output canvas.
    full_height
        Full height of the output canvas.
    blend_ramp
        Blending weight ramp array (2D) to apply to each tile.
    tile_size
        (tile_width, tile_height) tuple.
    chunk
        Chunk size for output dask arrays. Can be "auto", None, or a tuple.
    depth
        Optional depth dimension for 3D data. If None, tiles are treated as 2D.
    circular_mean
        If True, treat inputs as angles (degrees) and blend using circular mean.
        Only valid for 2D data (depth=None).

    Returns
    -------
    da.Array or List[da.Array]
        For 2D data: single dask array.
        For 3D data with multiple channels: list of dask arrays (one per channel).
    """
    if not coords or not tiles:
        raise ValueError("coords and tiles must be non-empty.")
    if len(coords) != len(tiles):
        raise ValueError("coords and tiles must have the same length.")

    # Convert tiles to dask arrays
    dask_tiles = [_to_dask(t) for t in tiles]
    
    # Get tile shape
    first_tile = dask_tiles[0]
    tile_shape = first_tile.shape
    tile_height, tile_width = tile_size
    
    if tile_shape[:2] != (tile_height, tile_width):
        raise ValueError(f"Tile shape {tile_shape[:2]} does not match tile_size {tile_size}")

    # Determine output shape
    if depth is not None:
        # 3D data: tiles should have shape (tile_width, tile_height, depth, ...)
        if len(tile_shape) == 4:
            # Shape: (height, width, depth, channels)
            n_channels = tile_shape[3]
            out_shape = (full_width, full_height, depth, n_channels)
            chunks_shape = (tile_width, tile_height, depth, n_channels)
        elif len(tile_shape) == 3:
            # Shape: (height, width, depth) - single channel 3D
            out_shape = (full_width, full_height, depth)
            chunks_shape = (tile_width, tile_height, depth)
        else:
            raise ValueError(f"Unexpected tile shape for 3D: {tile_shape}")
    else:
        # 2D data
        if len(tile_shape) == 2:
            out_shape = (full_width, full_height)
            chunks_shape = (tile_width, tile_height)
        else:
            # 2D with extra dimensions (e.g., channels)
            out_shape = (full_width, full_height) + tile_shape[2:]
            chunks_shape = (tile_width, tile_height) + tile_shape[2:]

    # Normalize chunk parameter
    if chunk == "auto" or chunk is None:
        chunk_tuple = chunks_shape
    else:
        chunk_tuple = chunk

    # Convert blend_ramp to dask array
    blend_ramp_da = _to_dask(blend_ramp)
    if blend_ramp_da.ndim != 2:
        raise ValueError("blend_ramp must be 2D.")

    if circular_mean:
        if depth is not None:
            raise ValueError("circular_mean is only supported for 2D data (depth=None).")
        return _stitch_tiles_circular_mean(
            coords, dask_tiles, full_width, full_height, blend_ramp_da,
            tile_size, chunk_tuple, out_shape
        )
    else:
        return _stitch_tiles_linear(
            coords, dask_tiles, full_width, full_height, blend_ramp_da,
            tile_size, chunk_tuple, out_shape, depth
        )


def _stitch_tiles_linear(
    coords: Sequence[Tuple[int, int]],
    tiles: List[da.Array],
    full_width: int,
    full_height: int,
    blend_ramp: da.Array,
    tile_size: Tuple[int, int],
    chunk: Tuple[int, ...],
    out_shape: Tuple[int, ...],
    depth: Optional[int],
) -> Union[da.Array, List[da.Array]]:
    """Stitch tiles using linear blending with chunk-aligned processing."""
    from collections import defaultdict
    
    pw, ph = tile_size
    
    # Determine if we need to return multiple channels
    first_tile = tiles[0]
    if depth is not None and first_tile.ndim == 4:
        # 3D with channels: return list of arrays
        n_channels = first_tile.shape[3]
        canvas = da.zeros(out_shape, chunks=chunk, dtype=np.float32)
        weight = da.zeros((full_width, full_height), chunks=chunk[:2], dtype=np.float32)
        
        # Collect per-chunk pieces
        block_tiles = defaultdict(list)
        block_weights = defaultdict(list)
        
        for (x0, y0), t in zip(coords, tiles):
            # Determine which chunks this tile falls into
            x0c = x0 // pw
            y0c = y0 // ph
            x1c = (x0 + pw - 1) // pw
            y1c = (y0 + ph - 1) // ph
            
            # Pad region covering those chunks
            x_start = x0c * pw
            y_start = y0c * ph
            x_end = (x1c + 1) * pw
            y_end = (y1c + 1) * ph
            
            block_canvas = da.zeros(
                (x_end - x_start, y_end - y_start, depth, n_channels),
                chunks=(pw, ph, depth, n_channels),
                dtype=np.float32
            )
            block_weight = da.zeros(
                (x_end - x_start, y_end - y_start),
                chunks=(pw, ph),
                dtype=np.float32
            )
            
            # Place tile into block
            xs = slice(x0 - x_start, x0 - x_start + pw)
            ys = slice(y0 - y_start, y0 - y_start + ph)
            block_canvas[xs, ys, ...] = t * blend_ramp[:, :, None, None]
            block_weight[xs, ys] = blend_ramp
            
            # Chop into per-chunk pieces
            for cx in range(x0c, x1c + 1):
                for cy in range(y0c, y1c + 1):
                    bid = (cx, cy)
                    sub_x = slice((cx - x0c) * pw, (cx - x0c + 1) * pw)
                    sub_y = slice((cy - y0c) * ph, (cy - y0c + 1) * ph)
                    block_tiles[bid].append(block_canvas[sub_x, sub_y, ...])
                    block_weights[bid].append(block_weight[sub_x, sub_y])
        
        canvas = da.map_blocks(
            _combine_block,
            canvas,
            block_tiles,
            block_weights,
            dtype=np.float32,
            chunks=chunk
        )
        
        return [canvas[..., i] for i in range(n_channels)]
    else:
        # Single output array (2D or 3D single channel)
        canvas = da.zeros(out_shape, chunks=chunk, dtype=np.float32)
        weight = da.zeros((full_width, full_height), chunks=chunk[:2], dtype=np.float32)
        
        # Collect per-chunk pieces
        block_tiles = defaultdict(list)
        block_weights = defaultdict(list)
        
        for (x0, y0), t in zip(coords, tiles):
            x0c = x0 // pw
            y0c = y0 // ph
            x1c = (x0 + pw - 1) // pw
            y1c = (y0 + ph - 1) // ph
            
            x_start = x0c * pw
            y_start = y0c * ph
            x_end = (x1c + 1) * pw
            y_end = (y1c + 1) * ph
            
            if depth is not None:
                block_shape = (x_end - x_start, y_end - y_start, depth)
                block_chunks = (pw, ph, depth)
                weight_block = blend_ramp[:, :, None]
            else:
                block_shape = (x_end - x_start, y_end - y_start) + t.shape[2:]
                block_chunks = (pw, ph) + t.shape[2:]
                if t.ndim == 2:
                    weight_block = blend_ramp
                else:
                    weight_block = blend_ramp[:, :, None]
            
            block_canvas = da.zeros(block_shape, chunks=block_chunks, dtype=np.float32)
            block_weight = da.zeros(
                (x_end - x_start, y_end - y_start),
                chunks=(pw, ph),
                dtype=np.float32
            )
            
            xs = slice(x0 - x_start, x0 - x_start + pw)
            ys = slice(y0 - y_start, y0 - y_start + ph)
            block_canvas[xs, ys, ...] = t.astype(np.float32) * weight_block
            block_weight[xs, ys] = blend_ramp
            
            for cx in range(x0c, x1c + 1):
                for cy in range(y0c, y1c + 1):
                    bid = (cx, cy)
                    sub_x = slice((cx - x0c) * pw, (cx - x0c + 1) * pw)
                    sub_y = slice((cy - y0c) * ph, (cy - y0c + 1) * ph)
                    block_tiles[bid].append(block_canvas[sub_x, sub_y, ...])
                    block_weights[bid].append(block_weight[sub_x, sub_y])
        
        if depth is not None:
            canvas = da.map_blocks(
                _combine_block_3d,
                canvas,
                block_tiles,
                block_weights,
                dtype=np.float32,
                chunks=chunk
            )
        else:
            canvas = da.map_blocks(
                _combine_block_2d,
                canvas,
                block_tiles,
                block_weights,
                dtype=np.float32,
                chunks=chunk
            )
        
        return canvas


def _stitch_tiles_circular_mean(
    coords: Sequence[Tuple[int, int]],
    tiles: List[da.Array],
    full_width: int,
    full_height: int,
    blend_ramp: da.Array,
    tile_size: Tuple[int, int],
    chunk: Tuple[int, ...],
    out_shape: Tuple[int, ...],
) -> da.Array:
    """Stitch tiles using circular mean for orientation data."""
    from collections import defaultdict
    
    pw, ph = tile_size
    
    sin_canvas = da.zeros(out_shape, chunks=chunk, dtype=np.float32)
    cos_canvas = da.zeros(out_shape, chunks=chunk, dtype=np.float32)
    weight = da.zeros((full_width, full_height), chunks=chunk[:2], dtype=np.float32)
    
    # Collect per-chunk pieces
    block_sin = defaultdict(list)
    block_cos = defaultdict(list)
    block_weights = defaultdict(list)
    
    for (x0, y0), t in zip(coords, tiles):
        x0c = x0 // pw
        y0c = y0 // ph
        x1c = (x0 + pw - 1) // pw
        y1c = (y0 + ph - 1) // ph
        
        x_start = x0c * pw
        y_start = y0c * ph
        x_end = (x1c + 1) * pw
        y_end = (y1c + 1) * ph
        
        block_shape = (x_end - x_start, y_end - y_start) + t.shape[2:]
        block_chunks = (pw, ph) + t.shape[2:]
        
        block_sin_tile = da.zeros(block_shape, chunks=block_chunks, dtype=np.float32)
        block_cos_tile = da.zeros(block_shape, chunks=block_chunks, dtype=np.float32)
        block_weight = da.zeros(
            (x_end - x_start, y_end - y_start),
            chunks=(pw, ph),
            dtype=np.float32
        )
        
        # Convert to radians and compute sin/cos
        radians = np.deg2rad(t.astype(np.float32))
        if t.ndim == 2:
            weight_block = blend_ramp
        else:
            weight_block = blend_ramp[:, :, None]
        
        sin_tile = np.sin(radians) * weight_block
        cos_tile = np.cos(radians) * weight_block
        
        xs = slice(x0 - x_start, x0 - x_start + pw)
        ys = slice(y0 - y_start, y0 - y_start + ph)
        block_sin_tile[xs, ys, ...] = sin_tile
        block_cos_tile[xs, ys, ...] = cos_tile
        block_weight[xs, ys] = blend_ramp
        
        for cx in range(x0c, x1c + 1):
            for cy in range(y0c, y1c + 1):
                bid = (cx, cy)
                sub_x = slice((cx - x0c) * pw, (cx - x0c + 1) * pw)
                sub_y = slice((cy - y0c) * ph, (cy - y0c + 1) * ph)
                block_sin[bid].append(block_sin_tile[sub_x, sub_y, ...])
                block_cos[bid].append(block_cos_tile[sub_x, sub_y, ...])
                block_weights[bid].append(block_weight[sub_x, sub_y])
    
    sin_canvas = da.map_blocks(
        _combine_block_2d,
        sin_canvas,
        block_sin,
        block_weights,
        dtype=np.float32,
        chunks=chunk
    )
    cos_canvas = da.map_blocks(
        _combine_block_2d,
        cos_canvas,
        block_cos,
        block_weights,
        dtype=np.float32,
        chunks=chunk
    )
    
    # Compute weight canvas
    weight = da.map_blocks(
        _combine_weight_block,
        weight,
        block_weights,
        dtype=np.float32,
        chunks=chunk[:2]
    )
    
    # Compute circular mean
    angle = np.rad2deg(np.arctan2(sin_canvas, cos_canvas))
    mask = weight > 0
    
    if angle.ndim == 2:
        result = da.where(mask, angle, 0.0)
    else:
        result = da.where(mask[..., None], angle, 0.0)
    
    return result


def _combine_block_2d(
    canvas: da.Array,
    block_tiles: Dict[Tuple[int, int], List[da.Array]],
    block_weights: Dict[Tuple[int, int], List[da.Array]],
    *args,
    block_info=None,
    **kwargs,
) -> np.ndarray:
    """Combine tiles within a block for 2D data."""
    if block_info is None:
        return np.zeros((), dtype=np.float32)
    
    chunk_id = tuple(block_info[None]['chunk-location'][:2])
    shape = block_info[None]['chunk-shape']
    
    paints = block_tiles.get(chunk_id, [])
    weights = block_weights.get(chunk_id, [])
    
    if not paints:
        return np.broadcast_to(np.zeros((), dtype=np.float32), shape)
    
    total_paint = da.sum(da.stack(paints, axis=0), axis=0)
    total_weight = da.sum(da.stack(weights, axis=0), axis=0)
    
    # Normalize by weight
    if total_paint.ndim > 2:
        result = total_paint / da.where(total_weight > 0, total_weight[..., None], 1.0)
    else:
        result = total_paint / da.where(total_weight > 0, total_weight, 1.0)
    
    return result.compute()


def _combine_block_3d(
    canvas: da.Array,
    block_tiles: Dict[Tuple[int, int], List[da.Array]],
    block_weights: Dict[Tuple[int, int], List[da.Array]],
    *args,
    block_info=None,
    **kwargs,
) -> np.ndarray:
    """Combine tiles within a block for 3D data."""
    if block_info is None:
        return np.zeros((), dtype=np.float32)
    
    chunk_id = tuple(block_info[None]['chunk-location'][:2])
    shape = block_info[None]['chunk-shape']
    
    paints = block_tiles.get(chunk_id, [])
    weights = block_weights.get(chunk_id, [])
    
    if not paints:
        return np.broadcast_to(np.zeros((), dtype=np.float32), shape)
    
    total_paint = da.sum(da.stack(paints, axis=0), axis=0)
    total_weight = da.sum(da.stack(weights, axis=0), axis=0)
    
    # For 3D, weight needs to be expanded to match depth dimension
    result = total_paint / da.where(total_weight > 0, total_weight[..., None], 1.0)
    
    return result.compute()


def _combine_weight_block(
    weight: da.Array,
    block_weights: Dict[Tuple[int, int], List[da.Array]],
    *args,
    block_info=None,
    **kwargs,
) -> np.ndarray:
    """Combine weight blocks."""
    if block_info is None:
        return np.zeros((), dtype=np.float32)
    
    chunk_id = tuple(block_info[None]['chunk-location'][:2])
    shape = block_info[None]['chunk-shape']
    
    weights = block_weights.get(chunk_id, [])
    
    if not weights:
        return np.broadcast_to(np.zeros((), dtype=np.float32), shape)
    
    total_weight = da.sum(da.stack(weights, axis=0), axis=0)
    return total_weight.compute()


def _combine_block(
    canvas: da.Array,
    block_tiles: Dict[Tuple[int, int], List[da.Array]],
    block_weights: Dict[Tuple[int, int], List[da.Array]],
    *args,
    block_info=None,
    **kwargs,
) -> np.ndarray:
    """Combine tiles within a block (3D with channels)."""
    if block_info is None:
        return np.zeros((), dtype=np.float32)
    
    chunk_id = tuple(block_info[None]['chunk-location'][:2])
    shape = block_info[None]['chunk-shape']
    
    paints = block_tiles.get(chunk_id, [])
    weights = block_weights.get(chunk_id, [])
    
    if not paints:
        return np.broadcast_to(np.zeros((), dtype=np.float32), shape)
    
    total_paint = da.sum(da.stack(paints, axis=0), axis=0)
    total_weight = da.sum(da.stack(weights, axis=0), axis=0)
    
    result = total_paint / da.where(total_weight > 0, total_weight[..., None, None], 1.0)
    
    return result.compute()
