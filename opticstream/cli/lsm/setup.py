

from opticstream.config import LSMScanConfig
from opticstream.cli.lsm.cli import lsm_cli
from opticstream.state.lsm_project_state import ensure_lock

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
    config_block_name: str | None = None,
    project_base_path: str,
    info_file: str,
    output_path: str,
) -> None:
    from pathlib import Path


    block_name = config_block_name or f"{project_name}-lsm-config"
    LSMScanConfig.register_type_and_schema()
    config = {
        "project_base_path": project_base_path,
        "info_file": info_file,
        "output_path": output_path,
    }

    for key in [k for k, v in list(config.items()) if v is None]:
        del config[key]

    scan_config = LSMScanConfig(**config)  # type: ignore[arg-type]
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

