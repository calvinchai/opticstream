from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from opticstream.config.psoct_scan_config import PSOCTScanConfigModel, TileSavingType
from opticstream.flows.psoct.utils import MosaicContext
from opticstream.utils.filename_utils import (
    extract_processed_index_from_filename,
    extract_spectral_index_from_filename,
    extract_tile_number_from_filename,
)


class TileFileReference(BaseModel):
    """Per input file: path and tile index used for indexed processed output names."""

    model_config = ConfigDict(frozen=True)
    file_path: Path
    tile_number: int = Field(
        ...,
        description=(
            "1-based tile index in processed stems mosaic_*_image_{tile:04d}. "
            "For mosaics_per_slice==2, matches image_<n> in the filename. "
            "For mosaics_per_slice==3 (processed_* layout), logical grid index from "
            "processed index and grid_size_x (may differ from any image_ token)."
        ),
    )


def build_tile_file_reference_list(
    file_list: list[Path],
    *,
    config: PSOCTScanConfigModel,
    mosaic_context: MosaicContext,
) -> list[TileFileReference]:
    mosaics_per_slice = config.mosaics_per_slice
    grid_size_x = mosaic_context.grid_size_x(config)
    refs: list[TileFileReference] = []
    for p in file_list:
        if mosaics_per_slice == 2:
            tile_number = extract_tile_number_from_filename(str(p))
        else:
            if config.acquisition.tile_saving_type in (TileSavingType.SPECTRAL, TileSavingType.SPECTRAL_12bit, TileSavingType.COMPLEX_WITH_SPECTRAL):
                i = extract_spectral_index_from_filename(str(p))
            else:
                i = extract_processed_index_from_filename(str(p))
            j = grid_size_x
            if j < 1:
                raise ValueError(f"grid_size_x must be >= 1, got {j}")
            tile_number = (i - 1) % j + 1
        refs.append(TileFileReference(file_path=p, tile_number=tile_number))
    return refs
