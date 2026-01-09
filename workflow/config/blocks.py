"""
Prefect Blocks for project configuration.

This module defines custom Prefect Blocks for managing project-level configuration.
Blocks provide typed configuration schemas with validation and UI management.

See: https://docs.prefect.io/v3/concepts/blocks
"""

from typing import List, Optional, Tuple

from prefect.blocks.core import Block

from workflow.config.constants import TileSavingType
from linc_convert.utils.zarr_config import ZarrConfig


class PSOCTScanConfig(Block):
    """
    Project-level configuration block.

    Stores all project-specific parameters needed for processing workflows.
    Block instances should be saved with name: "{project_name}-config"

    Attributes
    ----------
    project_base_path : str
        Base filesystem path for project data (required)
    grid_size_x_normal : int
        Number of batches (columns) per mosaic for normal illumination (required)
    grid_size_x_tilted : int
        Number of batches (columns) per mosaic for tilted illumination (required)
    grid_size_y : int
        Number of tiles per batch (rows) - determines batch size (required)
    tile_overlap : float, optional
        Overlap between tiles in pixels (default: 20.0)
    mask_threshold_normal : float, optional
        Threshold for mask generation and coordinate processing for normal illumination (default: 60.0)
    mask_threshold_tilted : float, optional
        Threshold for mask generation and coordinate processing for tilted illumination (default: 55.0)
    scan_resolution_3d : List[float], optional
        Scan resolution for 3D volumes [x, y, z] in millimeters
        (default: [0.01, 0.01, 0.0025])
    """

    # model_config = ConfigDict(frozen=False)

    zarr_config: ZarrConfig

    project_base_path: str
    grid_size_x_normal: int
    grid_size_x_tilted: int
    grid_size_y: int
    tile_size_x_normal: int = 350
    tile_size_x_tilted: int = 200
    tile_size_y: int = 350

    tile_overlap: float = 20.0
    mask_threshold_normal: float = 60.0
    mask_threshold_tilted: float = 55.0
    scan_resolution_3d: Tuple[float, float, float] = (0.01, 0.01, 0.0025)
    tile_saving_type: TileSavingType = TileSavingType.SPECTRAL
    dandiset_path: Optional[str] = None
    archive_path: Optional[str] = None

    archive_tile_name_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii.gz"
    mosaic_volume_format: str = "{project_name}_sample-slice{slice_id:02d}_acq-{acq}_proc-{modality}_OCT.ome.zarr"
    mosaic_enface_format: str = (
        "{project_name}_sample-slice{slice_id:02d}_acq-{acq}_proc-{modality}_OCT.nii.gz"
    )
    mosaic_mask_format: str = (
        "{project_name}_sample-slice{slice_id:03d}_acq-{acq}_OCT_mask.nii.gz"
    )
    slice_registered_format: str = (
        "{project_name}_sample-slice{slice_id:03d}_proc-3daxis_OCT.nii.gz"
    )

    enface_modalities: List[str] = ["ret", "ori", "biref", "mip", "surf"]
    volume_modalities: List[str] = ["dBI", "R3D", "O3D"]

    stitch_3d_volumes: bool = True


# PSOCTScanConfig.register_type_and_schema()
