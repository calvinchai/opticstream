"""LSM event payload helpers: idents must be present as dicts."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Type, TypeVar

from opticstream.config.lsm_scan_config import (
    LSMScanConfig,
    LSMScanConfigModel,
    get_lsm_scan_config,
)
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


def host_lsm_fs_path(path: Path | str) -> Path:
    """
    Resolve paths from Windows acquisition hosts when flows run on Linux.

    Prefect config often stores ``D:/...``; the same share is mounted at
    ``/mnt/willow-d/`` on Linux workers.
    """
    s = os.fspath(path)
    if not sys.platform.startswith("linux"):
        return Path(s)

    # Replace Windows drive-letter paths (e.g. D:/ or d:\) with corresponding /mnt/willow-x/ path
    def _replace_windows_drive_prefix(path_str: str) -> str:
        """
        Replace Windows drive-letter prefixes at the start of a path string
        (e.g. 'D:/...' or 'd:\\...') with their Linux mount equivalent (e.g. '/mnt/willow-d/').
        Only matches at the start of the string.
        Returns the modified string or the original string if no match was found.

        This function is safe under expected usage: it only rewrites
        absolute Windows paths starting with a drive-letter and a slash or backslash.
        """
        def _mnt_repl(match):
            letter = match.group(1).lower()
            return f"/mnt/willow-{letter}/"
        # Regex explanation:
        # ^          : start of string
        # [A-Za-z]   : single letter (drive)
        # :          : colon
        # [\\/]{1}   : either slash or backslash (only one character)
        return re.sub(r'^([A-Za-z]):[\\/]', _mnt_repl, path_str, count=1)

    s = _replace_windows_drive_prefix(s)
    s = s.replace("\\", "/")
    return Path(s)


def lsm_output_root(scan_config: LSMScanConfigModel) -> Path:
    """Normalized output root: ``output_path`` if set, else ``project_base_path``."""
    root = scan_config.output_path or scan_config.project_base_path
    return host_lsm_fs_path(root)


def _strip_formatted_output_path(
    strip_ident: LSMStripId,
    scan_config: LSMScanConfigModel,
    format_template: str,
) -> Path:
    acq = f"camera-{strip_ident.channel_id:02d}"
    output_path = lsm_output_root(scan_config) / format_template.format(
        project_name=strip_ident.project_name,
        slice_id=strip_ident.slice_id,
        strip_id=strip_ident.strip_id,
        acq=acq,
    )
    return host_lsm_fs_path(output_path)


def strip_zarr_output_path(
    strip_ident: LSMStripId, scan_config: LSMScanConfigModel
) -> Path:
    """Per-strip compressed zarr path (same convention as process_strip)."""
    fmt = scan_config.output_format or ""
    return _strip_formatted_output_path(strip_ident, scan_config, fmt)


def strip_mip_output_path(
    strip_ident: LSMStripId, scan_config: LSMScanConfigModel
) -> Path:
    """Per-strip MIP output path (same convention as process_strip)."""
    fmt = scan_config.output_mip_format or ""
    return _strip_formatted_output_path(strip_ident, scan_config, fmt)


def channel_zarr_volume_path(
    channel_ident: LSMChannelId, scan_config: LSMScanConfigModel
) -> Path:
    """Stitched channel volume zarr path (same convention as channel_volume_flow)."""
    vol_name = (
        f"{channel_ident.project_name}_slice-{channel_ident.slice_id:02d}_"
        f"channel-{channel_ident.channel_id:02d}_volume.zarr"
    )
    return lsm_output_root(scan_config) / vol_name
