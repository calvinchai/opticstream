"""
OCT setup: create or update the PSOCTScanConfig Prefect block for a project.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from niizarr import ZarrConfig

from opticstream.cli.oct import oct_cli
from opticstream.config.psoct_scan_config import PSOCTScanConfig

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)

_DEFAULT_ZARR_SHARD = (1024,)
_DEFAULT_MASK_NORMAL = 60.0
_DEFAULT_MASK_TILTED = 55.0


def configure_project_block(
    project_name: str,
    project_base_path: str = "./",
    grid_size_x_normal: int = 1,
    grid_size_x_tilted: int = 1,
    grid_size_y: int = 1,
) -> PSOCTScanConfig:
    block_name = f"{project_name.lower().replace('_', '-')}-config"
    zarr_config = ZarrConfig(shard=_DEFAULT_ZARR_SHARD)
    config_dict: Dict[str, Any] = {
        "project_name": project_name,
        "zarr_config": zarr_config,
        "project_base_path": project_base_path,
        "acquisition": {
            "grid_size_x_normal": grid_size_x_normal,
            "grid_size_x_tilted": grid_size_x_tilted,
            "grid_size_y": grid_size_y,
        },
        "mask_threshold_normal": _DEFAULT_MASK_NORMAL,
        "mask_threshold_tilted": _DEFAULT_MASK_TILTED,
    }
    project_config = PSOCTScanConfig(**config_dict)
    project_config.save(block_name, overwrite=True)
    logger.info("Saved PSOCTScanConfig block as '%s'", block_name)
    return project_config


@oct_cli.command
def setup(
    project_name: str,
    project_base_path: str = "./",
    grid_size_x_normal: int = 1,
    grid_size_x_tilted: int = 1,
    grid_size_y: int = 1,
) -> None:
    """
    Create or update the PSOCTScanConfig block for a project.

    Run this before ``opticstream oct watch run``. The block name is derived from
    ``project_name`` (e.g. ``myproject`` -> ``myproject-config``).

    Defaults keep CLI input minimal: ``project_base_path`` is ``./``, grid sizes are ``1``.
    """
    configure_project_block(
        project_name=project_name,
        project_base_path=project_base_path,
        grid_size_x_normal=grid_size_x_normal,
        grid_size_x_tilted=grid_size_x_tilted,
        grid_size_y=grid_size_y,
    )
    print(f"PSOCTScanConfig block for project '{project_name}' saved successfully.")
