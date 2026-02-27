"""
Grid and Tile representation for PSOCT/mosaic tile configurations.

The grid is constructed first (grid_type, rows, columns, direction); the instance
then has a fixed layout: each index already maps to a (row, col). Tiles can be
filled by generate_tiles (x, y, filepath from offsets and template), or by
loading from tile config (.j2/.yaml) or Fiji result (.registered.txt). Use
set_file_path to set filepath for all tiles (template or list).
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml


VALID_GRID_TYPES = [
    "row-by-row",
    "column-by-column",
    "snake-by-rows",
    "snake-by-columns",
]

ROW_BASED_ORDERS = ["right-down", "left-down", "right-up", "left-up"]
COLUMN_BASED_ORDERS = ["down-right", "down-left", "up-right", "up-left"]


# -----------------------------------------------------------------------------
# Tile
# -----------------------------------------------------------------------------


class Tile:
    """Single tile with coordinate, filepath, and grid position."""

    __slots__ = ("x", "y", "filepath", "index", "row", "col", "normalized_coord", "avg_signal")

    def __init__(
        self,
        x: float,
        y: float,
        filepath: str,
        index: int,
        row: Optional[int] = None,
        col: Optional[int] = None,
        avg_signal: Optional[float] = None,
    ) -> None:
        self.x = float(x)
        self.y = float(y)
        self.filepath = filepath
        self.index = index
        self.row = row
        self.col = col
        self.normalized_coord: Optional[Tuple[float, float]] = None
        self.avg_signal = avg_signal


# -----------------------------------------------------------------------------
# Grid
# -----------------------------------------------------------------------------


class Grid:
    """
    Grid of tiles defined by grid_type, rows, columns, and direction.

    Construction builds the full layout: each tile has index and (row, col)
    immediately. Then fill x, y, filepath via generate_tiles(), load_tile_config(),
    or load_fiji_result(). set_file_path() sets filepath for all tiles.
    Lookup by index (int) or cell (row, col) via __getitem__.
    """

    def __init__(
        self,
        grid_type: str,
        rows: int,
        columns: int,
        direction: Optional[str] = None,
        index_base: int = 1,
    ) -> None:
        if grid_type not in VALID_GRID_TYPES:
            raise ValueError(
                f"Invalid grid_type: {grid_type}. Must be one of {VALID_GRID_TYPES}"
            )
        if rows <= 0 or columns <= 0:
            raise ValueError("rows and columns must be positive")
        if index_base not in (0, 1):
            raise ValueError("index_base must be 0 or 1")

        self.grid_type = grid_type
        self.rows = rows
        self.columns = columns
        self.index_base = index_base
        self.direction = direction if direction is not None else self._default_direction()
        self._validate_direction()

        # Build tiles with index and (row, col) from layout; x, y, filepath empty
        self._tiles = []
        linear = index_base
        for r, c in self._iter_cells():
            self._tiles.append(
                Tile(x=0.0, y=0.0, filepath="", index=linear, row=r, col=c)
            )
            linear += 1
        self._cell_map = {(t.row, t.col): t for t in self._tiles}

    def _default_direction(self) -> str:
        if self.grid_type in ("row-by-row", "snake-by-rows"):
            return "right-down"
        return "down-right"

    def _validate_direction(self) -> None:
        row_based = self.grid_type in ("row-by-row", "snake-by-rows")
        valid = ROW_BASED_ORDERS if row_based else COLUMN_BASED_ORDERS
        if self.direction not in valid:
            raise ValueError(
                f"Invalid direction for {self.grid_type}: {self.direction}. "
                f"Must be one of {valid}"
            )

    def _iter_cells(self):
        """Yield (row, col) in grid traversal order (same order as tile index)."""
        order = self.direction
        if self.grid_type == "row-by-row":
            if order == "right-down":
                row_range, col_range = range(self.rows), range(self.columns)
            elif order == "left-down":
                row_range, col_range = range(self.rows), range(self.columns - 1, -1, -1)
            elif order == "right-up":
                row_range, col_range = range(self.rows - 1, -1, -1), range(self.columns)
            else:  # left-up
                row_range, col_range = range(self.rows - 1, -1, -1), range(self.columns - 1, -1, -1)
            for r in row_range:
                for c in col_range:
                    yield r, c
        elif self.grid_type == "column-by-column":
            if order == "down-right":
                col_range, row_range = range(self.columns), range(self.rows)
            elif order == "down-left":
                col_range, row_range = range(self.columns - 1, -1, -1), range(self.rows)
            elif order == "up-right":
                col_range, row_range = range(self.columns), range(self.rows - 1, -1, -1)
            else:  # up-left
                col_range, row_range = range(self.columns - 1, -1, -1), range(self.rows - 1, -1, -1)
            for c in col_range:
                for r in row_range:
                    yield r, c
        elif self.grid_type == "snake-by-rows":
            if order == "right-down":
                row_range, first_left = range(self.rows), True
            elif order == "left-down":
                row_range, first_left = range(self.rows), False
            elif order == "right-up":
                row_range, first_left = range(self.rows - 1, -1, -1), True
            else:
                row_range, first_left = range(self.rows - 1, -1, -1), False
            for row_pos, r in enumerate(row_range):
                col_range = range(self.columns) if (row_pos % 2 == 0) == first_left else range(self.columns - 1, -1, -1)
                for c in col_range:
                    yield r, c
        else:  # snake-by-columns
            if order == "down-right":
                col_range, first_down = range(self.columns), True
            elif order == "down-left":
                col_range, first_down = range(self.columns - 1, -1, -1), True
            elif order == "up-right":
                col_range, first_down = range(self.columns), False
            else:
                col_range, first_down = range(self.columns - 1, -1, -1), False
            for col_pos, c in enumerate(col_range):
                row_range = range(self.rows) if (col_pos % 2 == 0) == first_down else range(self.rows - 1, -1, -1)
                for r in row_range:
                    yield r, c

    def _format_filepath(
        self,
        path_template: str,
        index: int,
        row: int,
        col: int,
    ) -> str:
        """Format path_template with tile_number, index, row, col."""
        # User-facing index (e.g. 1-based)
        tile_number = index if self.index_base == 1 else index + 1
        try:
            return path_template.format(
                tile_number=tile_number,
                index=index,
                row=row,
                col=col,
            )
        except (KeyError, ValueError):
            return path_template.replace("{tile_number}", str(tile_number)).replace(
                "{index}", str(index)
            ).replace("{row}", str(row)).replace("{col}", str(col))

    def generate_tiles(
        self,
        row_offset: float,
        column_offset: float,
        path_template: str,
    ) -> List[Tile]:
        """
        Fill x, y, and filepath for all tiles from row/column offset and path template.

        Path template supports {tile_number}, {index}, {row}, {col}.
        Index follows the grid's index_base (default 1-based).
        """
        for i, (r, c) in enumerate(self._iter_cells()):
            t = self._tiles[i]
            t.x = c * column_offset
            t.y = r * row_offset
            t.filepath = self._format_filepath(path_template, t.index, r, c)
        return self._tiles

    def __len__(self) -> int:
        return len(self._tiles)

    def __getitem__(self, key: Union[int, tuple]) -> Tile:
        if isinstance(key, int):
            # Lookup by index (user-facing: 0- or 1-based)
            if self.index_base == 1:
                if key < 1 or key > len(self._tiles):
                    raise IndexError(f"Index {key} out of range (1-based, {len(self._tiles)} tiles)")
                return self._tiles[key - 1]
            if key < 0 or key >= len(self._tiles):
                raise IndexError(f"Index {key} out of range (0-based, {len(self._tiles)} tiles)")
            return self._tiles[key]
        if isinstance(key, tuple) and len(key) == 2:
            r, c = key
            if not self._cell_map:
                raise KeyError("Cell lookup not available (no row/col mapping)")
            if (r, c) not in self._cell_map:
                raise KeyError(f"No tile at cell ({r}, {c})")
            return self._cell_map[(r, c)]
        raise TypeError(f"key must be int (index) or tuple (row, col), got {type(key)}")

    # -------------------------------------------------------------------------
    # Set file path for all tiles
    # -------------------------------------------------------------------------

    def set_file_path(self, path_spec: Union[str, List[str]]) -> None:
        """
        Set filepath for all tiles.

        path_spec: either a template string (with {tile_number}, {index}, {row}, {col})
        or a list of paths (one per tile in index order). List length must match
        the number of tiles.
        """
        if isinstance(path_spec, list):
            if len(path_spec) != len(self._tiles):
                raise ValueError(
                    f"path_spec list length ({len(path_spec)}) must equal "
                    f"number of tiles ({len(self._tiles)})"
                )
            for i, path in enumerate(path_spec):
                self._tiles[i].filepath = str(path)
        else:
            for i, (r, c) in enumerate(self._iter_cells()):
                t = self._tiles[i]
                t.filepath = self._format_filepath(path_spec, t.index, r, c)

    # -------------------------------------------------------------------------
    # Tile map and coordinate normalization (aligned with process_tile_coord)
    # -------------------------------------------------------------------------

    def print_tile_map(self) -> None:
        """Print a visual grid map of tile indices (based on grid layout)."""
        if not self._tiles:
            print("No tiles to create a map.")
            return
        total_width = self.columns * 6 + 1
        print("\n--- Tile Grid Map (Based on Normal Coordinates) ---")
        print("-" * total_width)
        for row in range(self.rows):
            line = "|"
            for col in range(self.columns):
                t = self._cell_map.get((row, col))
                if t is None:
                    tile_id = "----"
                else:
                    tile_id = f"{t.index:04d}" if t.index is not None else "XXXX"
                line += f" {tile_id} |"
            print(line)
            print("-" * total_width)

    def normalize_coordinates(self) -> Tuple[float, float]:
        """
        Set normalized_coord on each tile so the minimum (x, y) is (0, 0).
        Uses current tile x, y. Returns (min_x, min_y).
        """
        if not self._tiles:
            raise ValueError("No tiles to normalize.")
        xs = [t.x for t in self._tiles]
        ys = [t.y for t in self._tiles]
        min_x = min(xs)
        min_y = min(ys)
        for t in self._tiles:
            norm_x = round(t.x - min_x, 3)
            norm_y = round(t.y - min_y, 3)
            t.normalized_coord = (norm_x, norm_y)
        print(
            f"\nNormalized coordinates set on tiles. "
            f"Minimum offsets found: X={min_x:.3f}, Y={min_y:.3f}"
        )
        return min_x, min_y

    def export_to_yaml(
        self,
        output_filename: str = "tile_coords_export.yaml",
        use_normalized: bool = True,
    ) -> None:
        """
        Export tiles to a YAML file (filepath, tile_number, x, y, avg_signal).
        If use_normalized is True, uses normalized_coord (and runs normalize_coordinates
        if any tile is missing it). Otherwise uses (x, y) directly.
        """
        if not self._tiles:
            print("No tiles to export.")
            return
        if use_normalized:
            if any(t.normalized_coord is None for t in self._tiles):
                self.normalize_coordinates()
        yaml_list = []
        for t in self._tiles:
            if t.index is None:
                continue
            if use_normalized:
                coord = t.normalized_coord
                if coord is None:
                    continue
                x_val, y_val = coord
            else:
                x_val = round(t.x, 3)
                y_val = round(t.y, 3)
            avg_sig_int = int(t.avg_signal) if t.avg_signal is not None else None
            tile_data = {
                "filepath": os.path.basename(t.filepath) if t.filepath else "",
                "tile_number": t.index,
                "x": x_val,
                "y": y_val,
                "avg_signal": avg_sig_int,
            }
            yaml_list.append(tile_data)
        try:
            with open(output_filename, "w") as f:
                try:
                    yaml.dump(yaml_list, f, sort_keys=False, default_flow_style=False)
                except TypeError:
                    yaml.dump(yaml_list, f, default_flow_style=False)
            print(f"Successfully exported {len(yaml_list)} tiles to {output_filename}")
        except Exception as e:
            print(f"Error saving YAML file: {e}")

    # -------------------------------------------------------------------------
    # Load from tile config (.j2 or .yaml)
    # -------------------------------------------------------------------------

    def load_tile_config(
        self,
        path: Union[str, Path],
        **render_context: Any,
    ) -> None:
        """
        Fill x, y, and filepath for all tiles from a tile config file (.j2 or .yaml).

        Call on an already-constructed grid. File must contain the same number of
        tiles (in order). Each entry has filepath, x, y. For .j2, render_context
        is passed to Jinja2 before parsing.
        """
        path = Path(path)
        text = path.read_text()

        if path.suffix.lower() in (".j2", ".jinja2"):
            try:
                import jinja2
            except ImportError:
                raise ImportError("Loading .j2 tile config requires jinja2")
            template = jinja2.Template(text)
            text = template.render(**render_context)

        data = yaml.safe_load(text)
        if not isinstance(data, dict) or "tiles" not in data:
            raise ValueError(f"Tile config must have top-level 'tiles' list: {path}")

        raw_tiles = data["tiles"]
        if not isinstance(raw_tiles, list):
            raise ValueError(f"'tiles' must be a list: {path}")

        entries = []
        for entry in raw_tiles:
            if not isinstance(entry, dict):
                continue
            avg = entry.get("avg_signal")
            if avg is not None:
                avg = float(avg)
            entries.append({
                "filepath": entry.get("filepath", ""),
                "x": float(entry.get("x", 0)),
                "y": float(entry.get("y", 0)),
                "avg_signal": avg,
            })

        if len(entries) != len(self._tiles):
            raise ValueError(
                f"Tile config has {len(entries)} entries but grid has {len(self._tiles)} tiles"
            )

        for i, e in enumerate(entries):
            t = self._tiles[i]
            t.x = e["x"]
            t.y = e["y"]
            t.filepath = e["filepath"]
            t.avg_signal = e.get("avg_signal")

    # -------------------------------------------------------------------------
    # Load from Fiji result (.registered.txt)
    # -------------------------------------------------------------------------

    def load_fiji_result(self, path: Union[str, Path]) -> None:
        """
        Fill x, y, and filepath for all tiles from a Fiji TileConfiguration.registered.txt file.

        Call on an already-constructed grid. File lines must be in the same order
        as grid traversal. Data lines: `filename; ; (x, y)`.
        """
        path = Path(path)
        pattern = re.compile(
            r"^(.+?)\s*;\s*;\s*\(\s*([-\d.eE+]+)\s*,\s*([-\d.eE+]+)\s*\)\s*$"
        )
        entries = []
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = pattern.match(line)
            if not m:
                continue
            entries.append({
                "filepath": m.group(1).strip(),
                "x": float(m.group(2)),
                "y": float(m.group(3)),
            })

        if len(entries) != len(self._tiles):
            raise ValueError(
                f"Fiji result has {len(entries)} entries but grid has {len(self._tiles)} tiles"
            )

        for i, e in enumerate(entries):
            t = self._tiles[i]
            t.x = e["x"]
            t.y = e["y"]
            t.filepath = e["filepath"]


__all__ = ["Grid", "Tile", "VALID_GRID_TYPES", "ROW_BASED_ORDERS", "COLUMN_BASED_ORDERS"]
