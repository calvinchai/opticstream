"""
OCT setup: create or update the PSOCTScanConfig Prefect block for a project.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

from opticstream.cli.oct import oct_cli
from opticstream.cli.setup_common import default_zarr_config
from opticstream.config.psoct_scan_config import get_psoct_scan_config_block_name
from opticstream.config.psoct_scan_config import PSOCTScanConfig
from opticstream.state.oct_project_state import ensure_lock

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)
warnings.filterwarnings(
    "ignore",
    message=".*PydanticSerializationUnexpectedValue.*",
)

_DEFAULT_MASK_NORMAL = 60.0
_DEFAULT_MASK_TILTED = 55.0


@oct_cli.command
def update_block() -> None:
    """
    Update the PSOCTScanConfig block.
    """
    PSOCTScanConfig.register_type_and_schema()


@oct_cli.command
def create_lock(
    project_name: str,
) -> None:
    """
    Create the OCT project state lock.
    """
    ensure_lock(project_name)


@oct_cli.command
def setup(
    project_name: str,
    *,
    project_base_path: Path | None = None,
    grid_size_x_normal: int = 1,
    grid_size_x_tilted: int = 1,
    grid_size_y: int = 1,
) -> None:
    """
    Create or update the PSOCTScanConfig block for a project.

    Run this before ``opticstream oct watch``. The block name is derived from
    ``project_name`` (e.g. ``myproject`` -> ``myproject-psoct-config``).

    Defaults keep CLI input minimal: grid sizes are ``1``.
    """
    update_block()
    ensure_lock(project_name)

    block_name = get_psoct_scan_config_block_name(project_name)

    if project_base_path is None:
        logger.warning("project_base_path is not set, please set it using prefect UI")

    scan_config = PSOCTScanConfig(
        project_name=project_name if project_name else Path("."),
        project_base_path=project_base_path if project_base_path else Path("."),
        acquisition={
            "grid_size_x_normal": grid_size_x_normal,
            "grid_size_x_tilted": grid_size_x_tilted,
            "grid_size_y": grid_size_y,
        },
        mask_threshold_normal=_DEFAULT_MASK_NORMAL,
        mask_threshold_tilted=_DEFAULT_MASK_TILTED,
        zarr_config=default_zarr_config(),
    )
    scan_config.save(block_name, overwrite=True)
    logger.info("Saved PSOCTScanConfig block as '%s'", block_name)

    created: list[Path] = []
    verified: list[Path] = []

    def _ensure_dir(path: Path | str | None) -> None:
        if not path:
            return
        p = Path(path)
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(p)
        else:
            verified.append(p)

    _ensure_dir(scan_config.project_base_path)
    _ensure_dir(scan_config.archive_path)
    _ensure_dir(scan_config.dandiset_path)

    if created:
        print("Created directories:")
        for p in created:
            print(f"  - {p}")
    if verified:
        print("Verified existing directories:")
        for p in verified:
            print(f"  - {p}")
    if not created and not verified:
        print("No directories to create or verify from PSOCTScanConfig.")
