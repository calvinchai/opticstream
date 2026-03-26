import logging

from pathlib import Path

from opticstream.config import LSMScanConfig
from opticstream.cli.lsm.cli import lsm_cli
from opticstream.cli.setup_common import default_zarr_config
from opticstream.config.lsm_scan_config import get_lsm_scan_config_block_name
from opticstream.state.lsm_project_state import ensure_lock

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)
import warnings
warnings.filterwarnings(
    "ignore",
    message=".*PydanticSerializationUnexpectedValue.*",
)

@lsm_cli.command
def update_block() -> None:
    """
    Update the LSMScanConfig block.
    """
    LSMScanConfig.register_type_and_schema()


@lsm_cli.command
def create_lock(
    project_name: str,
) -> None:
    """
    Create the LSM project state lock.
    """
    ensure_lock(project_name)


@lsm_cli.command
def setup(
    project_name: str,
    *,
    project_base_path: Path | None = None,
    info_file: Path | None = None,
    output_path: Path | None = None,
) -> None:
    update_block()
    ensure_lock(project_name)

    block_name = get_lsm_scan_config_block_name(project_name)

    if project_base_path is None:
        logger.warning("project_base_path is not set, please set it using prefect UI")
    if info_file is None:
        logger.warning("info_file is not set, please set it using prefect UI")
    if output_path is None:
        logger.warning("output_path is not set, please set it using prefect UI")
    scan_config = LSMScanConfig(
        project_name=project_name if project_name else Path('.'),
        project_base_path=project_base_path if project_base_path else Path('.'),
        info_file=info_file if info_file else Path('./info.mat'),
        output_path=output_path if output_path else Path('.'),
        zarr_config=default_zarr_config(),
    )
    scan_config.save(block_name, overwrite=True)

    created: list[Path] = []
    verified: list[Path] = []

    def _ensure_dir(path: str | None) -> None:
        if not path:
            return
        p = Path(path)
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(p)
        else:
            verified.append(p)

    _ensure_dir(scan_config.project_base_path)
    _ensure_dir(scan_config.output_path)
    _ensure_dir(scan_config.archive_path)

    if scan_config.info_file:
        info_parent = Path(scan_config.info_file).parent
        _ensure_dir(str(info_parent))

    if created:
        print("Created directories:")
        for p in created:
            print(f"  - {p}")
    if verified:
        print("Verified existing directories:")
        for p in verified:
            print(f"  - {p}")
    if not created and not verified:
        print("No directories to create or verify from LSMScanConfig.")
