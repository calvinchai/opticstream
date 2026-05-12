"""Project setup helpers for the OpticHub dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from opticapi.project_state.lsm_state_service import LSMProjectStateService
from opticapi.project_state.oct_state_service import OCTProjectStateService
from opticapi.project_state.redis_backend import RedisStateBackend
from opticapi.config.utils import default_zarr_config
from opticstream.config import LSMScanConfig
from opticapi.config.lsm_scan_config import get_lsm_scan_config_block_name
from opticstream.config.psoct_scan_config import PSOCTScanConfig
from opticapi.config.psoct_scan_config import get_psoct_scan_config_block_name

_DEFAULT_OCT_MASK_NORMAL = 60.0
_DEFAULT_OCT_MASK_TILTED = 55.0


@dataclass(frozen=True)
class ProjectSetupResult:
    """Summary of a project setup action."""

    block_name: str
    created: list[Path]
    verified: list[Path]
    redis_project_initialized: bool


def _ensure_dir(path: Path | str | None, created: list[Path], verified: list[Path]) -> None:
    if not path:
        return
    p = Path(path)
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)
        created.append(p)
    else:
        verified.append(p)


def _require_project_name(project_name: str) -> str:
    name = project_name.strip()
    if not name:
        raise ValueError("Project name is required.")
    return name


def create_oct_project(
    project_name: str,
    *,
    state_backend: RedisStateBackend | None = None,
    project_base_path: Path | None = None,
    grid_size_x_normal: int = 1,
    grid_size_x_tilted: int = 1,
    grid_size_y: int = 1,
) -> ProjectSetupResult:
    """Create or update an OCT project config block."""
    name = _require_project_name(project_name)

    PSOCTScanConfig.register_type_and_schema()
    block_name = get_psoct_scan_config_block_name(name)
    scan_config = PSOCTScanConfig(
        project_name=name,
        project_base_path=project_base_path if project_base_path else Path("."),
        acquisition={
            "grid_size_x_normal": grid_size_x_normal,
            "grid_size_x_tilted": grid_size_x_tilted,
            "grid_size_y": grid_size_y,
        },
        mask_threshold_normal=_DEFAULT_OCT_MASK_NORMAL,
        mask_threshold_tilted=_DEFAULT_OCT_MASK_TILTED,
        zarr_config=default_zarr_config(),
    )
    scan_config.save(block_name, overwrite=True)

    created: list[Path] = []
    verified: list[Path] = []
    _ensure_dir(scan_config.project_base_path, created, verified)
    _ensure_dir(scan_config.archive_path, created, verified)
    _ensure_dir(scan_config.dandiset_path, created, verified)

    redis_project_initialized = False
    if state_backend is not None:
        with OCTProjectStateService(state_backend).open_project_by_parts(name):
            redis_project_initialized = True

    return ProjectSetupResult(
        block_name=block_name,
        created=created,
        verified=verified,
        redis_project_initialized=redis_project_initialized,
    )


def create_lsm_project(
    project_name: str,
    *,
    state_backend: RedisStateBackend | None = None,
    project_base_path: Path | None = None,
    info_file: Path | None = None,
    output_path: Path | None = None,
) -> ProjectSetupResult:
    """Create or update an LSM project config block."""
    name = _require_project_name(project_name)

    LSMScanConfig.register_type_and_schema()
    block_name = get_lsm_scan_config_block_name(name)
    scan_config = LSMScanConfig(
        project_name=name,
        project_base_path=project_base_path if project_base_path else Path("."),
        info_file=info_file if info_file else Path("./info.mat"),
        output_path=output_path if output_path else Path("."),
        zarr_config=default_zarr_config(),
    )
    scan_config.save(block_name, overwrite=True)

    created: list[Path] = []
    verified: list[Path] = []
    _ensure_dir(scan_config.project_base_path, created, verified)
    _ensure_dir(scan_config.output_path, created, verified)
    _ensure_dir(scan_config.archive_path, created, verified)
    if scan_config.info_file:
        _ensure_dir(Path(scan_config.info_file).parent, created, verified)

    redis_project_initialized = False
    if state_backend is not None:
        with LSMProjectStateService(state_backend).open_project_by_parts(name):
            redis_project_initialized = True

    return ProjectSetupResult(
        block_name=block_name,
        created=created,
        verified=verified,
        redis_project_initialized=redis_project_initialized,
    )
