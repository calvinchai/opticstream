from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from opticstream.config.project_config import get_project_config_block
from opticstream.config.psoct_scan_config import (
    PSOCTScanConfigModel,
    get_psoct_scan_config,
)
from opticstream.state.oct_project_state import (
    OCTBatchId,
    OCTMosaicId,
    OCTSliceId,
)


def slice_id_for_mosaic_id(mosaic_id: int) -> int:
    """Match ``OCT_STATE_SERVICE`` derivation (mosaic_id // 2)."""
    return mosaic_id // 2


def mosaic_ident_from_project_and_mosaic_id(project_name: str, mosaic_id: int) -> OCTMosaicId:
    return OCTMosaicId(
        project_name=project_name,
        slice_id=slice_id_for_mosaic_id(mosaic_id),
        mosaic_id=mosaic_id,
    )


def grid_size_x_for_mosaic(cfg: PSOCTScanConfigModel, mosaic_id: int) -> int:
    return (
        cfg.acquisition.grid_size_x_tilted
        if mosaic_id % 2 == 0
        else cfg.acquisition.grid_size_x_normal
    )


def mask_threshold_for_mosaic(cfg: PSOCTScanConfigModel, mosaic_id: int) -> float:
    return (
        cfg.mask_threshold_tilted
        if mosaic_id % 2 == 0
        else cfg.mask_threshold_normal
    )


PROCESS_MOSAIC_FLOW_KWARGS_KEYS = (
    "project_base_path",
    "grid_size_x",
    "grid_size_y",
    "tile_overlap",
    "mask_threshold",
    "scan_resolution_3d",
    "enface_modalities",
    "dandiset_path",
    "mosaic_enface_format",
)


def _process_mosaic_flow_defaults_from_config(
    mosaic_ident: OCTMosaicId,
    cfg: PSOCTScanConfigModel,
) -> Dict[str, Any]:
    """Base keyword args for :func:`process_mosaic_flow` from the project config block."""
    return {
        "project_base_path": str(cfg.project_base_path),
        "grid_size_x": grid_size_x_for_mosaic(cfg, mosaic_ident.mosaic_id),
        "grid_size_y": cfg.acquisition.grid_size_y,
        "tile_overlap": cfg.acquisition.tile_overlap,
        "mask_threshold": mask_threshold_for_mosaic(cfg, mosaic_ident.mosaic_id),
        "scan_resolution_3d": cfg.acquisition.scan_resolution_3d,
        "enface_modalities": [m.value for m in cfg.enface_modalities],
        "dandiset_path": str(cfg.dandiset_path) if cfg.dandiset_path else None,
        "mosaic_enface_format": cfg.mosaic_enface_format,
    }


def resolve_process_mosaic_flow_kwargs(
    payload: Mapping[str, Any],
    mosaic_ident: OCTMosaicId,
    cfg: PSOCTScanConfigModel,
) -> Dict[str, Any]:
    """
    Resolve kwargs for :func:`process_mosaic_flow` like LSM strip flows: values come
    from the config block unless the event payload overrides a key.
    """
    defaults = _process_mosaic_flow_defaults_from_config(mosaic_ident, cfg)
    resolved: Dict[str, Any] = {}
    for key in PROCESS_MOSAIC_FLOW_KWARGS_KEYS:
        if key in payload:
            resolved[key] = payload[key]
        else:
            resolved[key] = defaults[key]
    return resolved


def nifti_paths_from_enface_outputs(
    enface_outputs: Mapping[str, Any],
) -> list[str]:
    return [
        outputs["nifti"]
        for outputs in enface_outputs.values()
        if isinstance(outputs, dict) and "nifti" in outputs
    ]


def non_empty_paths_from_mapping(paths_by_key: Mapping[str, Any]) -> list[str]:
    return [str(p) for p in paths_by_key.values() if p]


def normalize_float_sequence(
    value: Any,
    *,
    default: Sequence[float] | None = None,
) -> list[float]:
    if value is None:
        return list(default or [0.01, 0.01, 0.0025])
    if isinstance(value, tuple):
        return [float(x) for x in value]
    if isinstance(value, list):
        return [float(x) for x in value]
    raise TypeError(f"expected list or tuple, got {type(value)}")


def get_project_base_path(project_name: str) -> Path:
    return get_psoct_scan_config(project_name).project_base_path


def get_item_path(
    item_indent: OCTBatchId | OCTMosaicId | OCTSliceId,
    project_base_path: Optional[Path] = None,
) -> Path:
    if project_base_path is None:
        project_base_path = get_project_base_path(item_indent.project_name)
    if not isinstance(project_base_path, Path):
        if isinstance(project_base_path, str):
            project_base_path = Path(project_base_path)
        else:
            raise ValueError(f"project_base_path must be a Path, got {type(project_base_path)}")
    if isinstance(item_indent, OCTMosaicId):
        return project_base_path / f"mosaic-{item_indent.mosaic_id:03d}"
    if isinstance(item_indent, OCTSliceId):
        return project_base_path / f"slice-{item_indent.slice_id:02d}"
    raise ValueError(f"Invalid item indent: {item_indent}")

def _model_from_payload(payload: Mapping[str, Any], key: str, model_type: Any) -> Any:
    if key not in payload:
        raise KeyError(f"payload must include {key}")
    value = payload[key]
    if isinstance(value, model_type):
        return value
    if isinstance(value, Mapping):
        return model_type(**value)
    raise TypeError(f"payload[{key}] must be a mapping or {model_type.__name__}")


def batch_ident_from_payload(payload: Mapping[str, Any]) -> OCTBatchId:
    return _model_from_payload(payload, "batch_ident", OCTBatchId)


def mosaic_ident_from_payload(payload: Mapping[str, Any]) -> OCTMosaicId:
    return _model_from_payload(payload, "mosaic_ident", OCTMosaicId)


def slice_ident_from_payload(payload: Mapping[str, Any]) -> OCTSliceId:
    return _model_from_payload(payload, "slice_ident", OCTSliceId)


def load_scan_config_for_payload(payload: Mapping[str, Any]) -> PSOCTScanConfigModel:
    project_name = payload.get("project_name")
    if project_name is None:
        raw_ident = payload.get("mosaic_ident")
        if isinstance(raw_ident, OCTMosaicId):
            project_name = raw_ident.project_name
        elif isinstance(raw_ident, Mapping):
            project_name = raw_ident.get("project_name")
    if project_name is None:
        raise KeyError("payload must include project_name or mosaic_ident with project_name")
    override = payload.get("override_config")
    if override is not None:
        cfg = get_psoct_scan_config(project_name, override_config_name=override)
    else:
        cfg = get_project_config_block(project_name)
    if cfg is None:
        raise ValueError(
            f"project config block '{project_name.lower().replace('_', '-')}-config' not found"
        )
    return PSOCTScanConfigModel.model_validate(cfg.model_dump())


def path_list_from_payload(payload: Mapping[str, Any], key: str = "file_list") -> list[Path]:
    values = payload.get(key, [])
    if not isinstance(values, list):
        raise TypeError(f"payload[{key}] must be a list")
    return [Path(v) for v in values]

