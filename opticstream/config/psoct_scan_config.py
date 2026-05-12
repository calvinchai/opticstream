from typing import Optional
from pathlib import Path

import psoct_toolbox
from prefect.blocks.core import Block
from pydantic import Field

from opticapi.config.psoct_scan_config import PSOCTScanConfigModel
from opticapi.config.psoct_scan_config import get_psoct_scan_config_block_name


class PSOCTScanConfig(PSOCTScanConfigModel, Block):
    """
    Project-level configuration block.

    Stores all project-specific parameters needed for processing workflows.
    Block instances should be saved with name: "{project_name}-config"
    """

    matlab_root: Path | None = Field(
        default_factory=psoct_toolbox.get_matlab_root,
        description="MATLAB installation root used by psoct_toolbox execution",
    )


def get_psoct_scan_config(
    project_name: str, override_config_name: Optional[str] = None
) -> PSOCTScanConfig:
    """
    Get the scan configuration for a project.
    """
    return PSOCTScanConfig.load(
        override_config_name or get_psoct_scan_config_block_name(project_name)
    )
