

from opticstream.config import LSMScanConfig
from opticstream.cli.lsm.cli import lsm_cli

@lsm_cli.command
def setup(
    project_name: str,
    *,
    config_block_name: str | None = None,
    project_base_path: str,
    info_file: str,
    output_path: str,
    output_mip: bool | None = None,
    output_format: str | None = None,
    output_mip_format: str | None = None,
    archive_path: str | None = None,
    delete_strip: bool | None = None,
    dandi_bin: str | None = None,
    dandi_instance: str | None = None,
) -> None:
    from pathlib import Path


    block_name = config_block_name or f"{project_name}-lsm-config"
    LSMScanConfig.register_type_and_schema()
    config = {
        "project_base_path": project_base_path,
        "info_file": info_file,
        "output_path": output_path,
        "output_mip": output_mip,
        "output_format": output_format,
        "output_mip_format": output_mip_format,
        "archive_path": archive_path,
        "delete_strip": delete_strip,
        "dandi_bin": dandi_bin,
        "dandi_instance": dandi_instance,
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

