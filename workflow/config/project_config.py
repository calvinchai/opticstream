"""
Project-level parameter management using Prefect Blocks.

See: https://docs.prefect.io/v3/concepts/blocks
"""

from typing import Optional

from workflow.config.blocks import PSOCTScanConfig


def get_project_config_block(project_name: str) -> Optional[PSOCTScanConfig]:
    """
    Load a project configuration block.

    Block instances are saved with name: "{project_name}-config"

    Parameters
    ----------
    project_name : str
        Project identifier

    Returns
    -------
    ProjectConfig, optional
        Project configuration block, or None if not found
    """
    block_name = f"{project_name.lower().replace('_', '-')}-config"
    try:
        return PSOCTScanConfig.load(block_name)
    except Exception:
        # Block not found or error loading - return None
        return None


def get_grid_size_x(project_name: str, mosaic_id: int) -> int:
    """
    Get grid size x for a given project and mosaic id.

    Parameters
    ----------
    project_name: str
    mosaic_id: int

    Returns
    -------
    int
        Grid size x (number of columns/batches) for the mosaic

    Raises
    ------
    ValueError
        If project config block is not found
    """
    project_config = get_project_config_block(project_name)
    if project_config is None:
        raise ValueError(
            f"Project config block for '{project_name}' not found. "
            f"Cannot determine grid_size_x. Please create config block '{project_name}-config' or provide grid_size_x explicitly."
        )
    return (
        project_config.grid_size_x_normal
        if mosaic_id % 2 == 1
        else project_config.grid_size_x_tilted
    )
