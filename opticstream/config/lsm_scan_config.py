from typing import Optional

from prefect.blocks.core import Block

from opticapi.config.lsm_scan_config import LSMScanConfigModel
from opticapi.config.lsm_scan_config import get_lsm_scan_config_block_name


class LSMScanConfig(LSMScanConfigModel, Block):
    """
    Project-level configuration block for LSM processing.
    Block instances should be saved with name: "{project_name}-lsm-config"
    """


def get_lsm_scan_config(
    project_name: str, override_config_name: Optional[str] = None
) -> LSMScanConfig:
    """
    Get the scan configuration for a project.
    """
    return LSMScanConfig.load(
        override_config_name or get_lsm_scan_config_block_name(project_name)
    )
