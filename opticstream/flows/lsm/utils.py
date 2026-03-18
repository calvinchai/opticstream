"""LSM event payload helpers: idents must be present as dicts."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Type, TypeVar

from opticstream.config.lsm_scan_config import LSMScanConfig, get_lsm_scan_config
from opticstream.state.lsm_project_state import LSMChannelId, LSMStripId

T = TypeVar("T")


def _model_from_payload(payload: Dict[str, Any], key: str, model_cls: Type[T]) -> T:
    raw = payload.get(key)
    if isinstance(raw, model_cls):
        return raw
    if isinstance(raw, dict):
        return model_cls(**raw)  # type: ignore[arg-type, call-arg]
    raise KeyError(f"payload must include {key} as dict or {model_cls.__name__}")


def strip_ident_from_payload(payload: Dict[str, Any]) -> LSMStripId:
    """Build LSMStripId from payload; requires ``strip_ident`` (dict or model)."""
    try:
        return _model_from_payload(payload, "strip_ident", LSMStripId)
    except KeyError:
        raise KeyError(
            "payload must include strip_ident (dict with project_name, slice_id, strip_id, channel_id)"
        ) from None


def channel_ident_from_payload(payload: Dict[str, Any]) -> LSMChannelId:
    """Build LSMChannelId from payload; requires ``channel_ident`` (dict or model)."""
    try:
        return _model_from_payload(payload, "channel_ident", LSMChannelId)
    except KeyError:
        raise KeyError(
            "payload must include channel_ident (dict with project_name, slice_id, channel_id)"
        ) from None


def channel_ident_from_strip(strip: LSMStripId) -> LSMChannelId:
    """Channel identity for the slice/channel of this strip."""
    return LSMChannelId(
        project_name=strip.project_name,
        slice_id=strip.slice_id,
        channel_id=strip.channel_id,
    )


def load_scan_config_for_payload(
    project_name: str, payload: Mapping[str, Any]
) -> LSMScanConfig:
    """Load LSM scan config using optional ``override_config`` from event payload."""
    return get_lsm_scan_config(
        project_name, override_config_name=payload.get("override_config")
    )
