"""Filesystem paths for LSM outputs derived from idents and scan config."""

from __future__ import annotations

import os.path as op

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.state.lsm_project_state import LSMChannelId, LSMStripId


def _output_root(scan_config: LSMScanConfigModel) -> str:
    return scan_config.output_path or scan_config.project_base_path


def _strip_formatted_output_path(
    strip_ident: LSMStripId,
    scan_config: LSMScanConfigModel,
    format_template: str,
) -> str:
    acq = f"camera-{strip_ident.channel_id:02d}"
    return op.join(
        _output_root(scan_config),
        format_template.format(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            acq=acq,
        ),
    )


def strip_zarr_output_path(
    strip_ident: LSMStripId, scan_config: LSMScanConfigModel
) -> str:
    """Per-strip compressed zarr path (same convention as process_strip)."""
    fmt = scan_config.output_format or ""
    return _strip_formatted_output_path(strip_ident, scan_config, fmt)


def strip_mip_output_path(
    strip_ident: LSMStripId, scan_config: LSMScanConfigModel
) -> str:
    """Per-strip MIP output path (same convention as process_strip)."""
    fmt = scan_config.output_mip_format or ""
    return _strip_formatted_output_path(strip_ident, scan_config, fmt)


def channel_zarr_volume_path(
    channel_ident: LSMChannelId, scan_config: LSMScanConfigModel
) -> str:
    """Stitched channel volume zarr path (same convention as channel_volume_flow)."""
    vol_name = (
        f"{channel_ident.project_name}_slice-{channel_ident.slice_id:02d}_"
        f"channel-{channel_ident.channel_id:02d}_volume.zarr"
    )
    return op.join(_output_root(scan_config), vol_name)
