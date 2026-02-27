#!/usr/bin/env python3
"""
Edit tile_info Jinja2 YAML files by removing rows or columns from the tile grid.

Reads a tile_info_*.j2 file, builds the 2D grid from the given row and column
counts (tiles are in row-major order: index i -> row i // n_cols, col i % n_cols),
applies remove-row/remove-column options, and writes the result preserving the
header and YAML style.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml
from cyclopts import App

app = App(name="edit_tile_info_grid")


def parse_tile_info_file(input_path: str) -> tuple:
    """Split file into header (including 'tiles:') and parsed tile list.

    Preserves the exact header so Jinja2 (e.g. {{ base_dir }}) is unchanged.
    Returns (header_str, list of tile dicts).
    """
    path = Path(input_path)
    text = path.read_text()
    lines = text.splitlines(keepends=True)

    # Find the line that is exactly "tiles:" (possibly with leading spaces)
    header_end = -1
    for i, line in enumerate(lines):
        if line.strip() == "tiles:":
            header_end = i
            break
    if header_end < 0:
        raise ValueError(f"No 'tiles:' key found in {input_path}")

    header = "".join(lines[: header_end + 1])
    body_lines = lines[header_end + 1 :]
    body = "".join(body_lines).strip()
    if not body:
        return header, []

    # Body is a YAML list of tile dicts
    tiles = yaml.safe_load(body)
    if not isinstance(tiles, list):
        raise ValueError(f"Expected tiles to be a YAML list, got {type(tiles)}")
    return header, tiles


def build_grid(
    tiles: List[Dict[str, Any]], n_rows: int, n_cols: int
) -> List[List[Dict[str, Any]]]:
    """Build grid from flat tile list using row-major order.

    Tile at list index i maps to row i // n_cols, column i % n_cols.
    Requires len(tiles) == n_rows * n_cols.
    """
    expected = n_rows * n_cols
    if len(tiles) != expected:
        raise ValueError(
            f"Tile count {len(tiles)} does not match rows×cols {n_rows}×{n_cols} = {expected}"
        )
    grid: List[List[Dict[str, Any]]] = []
    for r in range(n_rows):
        row = tiles[r * n_cols : (r + 1) * n_cols]
        grid.append(row)
    return grid


def apply_removals(
    grid: List[List[Dict[str, Any]]],
    remove_rows_top: int,
    remove_rows_bottom: int,
    remove_cols_left: int,
    remove_cols_right: int,
) -> List[Dict[str, Any]]:
    """Drop rows/columns from grid and return flat row-major tile list."""
    if not grid:
        return []

    n_rows = len(grid)
    n_cols = len(grid[0]) if grid else 0

    # Bounds for slicing
    r0 = remove_rows_top
    r1 = n_rows - remove_rows_bottom
    c0 = remove_cols_left
    c1 = n_cols - remove_cols_right

    if r0 >= r1 or c0 >= c1:
        return []

    out: List[Dict[str, Any]] = []
    for r in range(r0, r1):
        row = grid[r]
        for c in range(c0, min(c1, len(row))):
            out.append(row[c])
    return out


def renumber_tiles(tiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Renumber tiles to 1..N by sorted original tile_number; update filepaths.

    Gathers all tile_number values, sorts them, maps old -> 1, 2, ..., N.
    Updates each tile's tile_number and filepath (replaces zero-padded old
    number with new in the filepath string).
    """
    if not tiles:
        return tiles
    # Deep copy so we don't mutate originals
    tiles = [dict(t) for t in tiles]
    old_numbers = sorted(t.get("tile_number") for t in tiles)
    old_to_new = {old: new for new, old in enumerate(old_numbers, start=1)}
    n_digits = 4  # match common image_0001 style
    for t in tiles:
        old_num = t.get("tile_number")
        new_num = old_to_new[old_num]
        t["tile_number"] = new_num
        fp = t.get("filepath")
        if fp is not None and isinstance(fp, str):
            old_str = str(old_num).zfill(n_digits)
            new_str = str(new_num).zfill(n_digits)
            t["filepath"] = fp.replace(old_str, new_str)
    return tiles


def write_tile_info_file(
    output_path: str, header: str, tiles: List[Dict[str, Any]]
) -> None:
    """Write header unchanged and tiles as YAML list (same style)."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        f.write(header)
        yaml.dump(
            tiles,
            f,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
            allow_unicode=True,
        )


@app.default
def main(
    input_path: str,
    output_path: str,
    rows: int,
    cols: int,
    remove_rows_top: int = 0,
    remove_rows_bottom: int = 0,
    remove_cols_left: int = 0,
    remove_cols_right: int = 0,
    dry_run: bool = False,
) -> None:
    """Edit tile_info grid: remove rows or columns.

    Reads the input tile_info .j2 file and builds the grid from the given
    row and column counts (tiles are in row-major order), then removes the
    requested rows/columns and writes the result. Only removal is supported.

    Parameters
    ----------
    input_path : str
        Path to input tile_info file (e.g. tile_info_normal.j2)
    output_path : str
        Path to output file
    rows : int
        Number of rows in the existing tile grid
    cols : int
        Number of columns in the existing tile grid
    remove_rows_top : int
        Number of rows to remove from the top (default 0)
    remove_rows_bottom : int
        Number of rows to remove from the bottom (default 0)
    remove_cols_left : int
        Number of columns to remove from the left (default 0)
    remove_cols_right : int
        Number of columns to remove from the right (default 0)
    dry_run : bool
        If true, print grid size and removals without writing
    """
    header, tiles = parse_tile_info_file(input_path)
    if not tiles:
        print("No tiles found in input file.", file=sys.stderr)
        sys.exit(1)

    try:
        grid = build_grid(tiles, rows, cols)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    n_rows, n_cols = rows, cols

    if dry_run:
        print(f"Grid: {n_rows} rows × {n_cols} columns ({len(tiles)} tiles)")
        print(
            f"Would remove: rows top={remove_rows_top}, bottom={remove_rows_bottom}; "
            f"cols left={remove_cols_left}, right={remove_cols_right}"
        )
        after_rows = n_rows - remove_rows_top - remove_rows_bottom
        after_cols = n_cols - remove_cols_left - remove_cols_right
        if after_rows <= 0 or after_cols <= 0:
            print("Result would be empty.", file=sys.stderr)
        else:
            print(f"Result would have {after_rows} rows × {after_cols} columns")
        return

    result = apply_removals(
        grid,
        remove_rows_top,
        remove_rows_bottom,
        remove_cols_left,
        remove_cols_right,
    )
    if not result:
        print(
            "Removals would leave no tiles. Refusing to write empty file.",
            file=sys.stderr,
        )
        sys.exit(1)

    result = renumber_tiles(result)
    write_tile_info_file(output_path, header, result)
    print(f"Wrote {len(result)} tiles to {output_path}")


if __name__ == "__main__":
    app()
