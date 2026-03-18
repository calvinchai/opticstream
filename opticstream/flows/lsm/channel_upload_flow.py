"""
Upload stitched channel volume (e.g. DANDI). Triggered on CHANNEL_VOLUME_STITCHED.
"""

from __future__ import annotations

from typing import Any, Dict

from prefect import flow, get_run_logger, task

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.flows.lsm.event import CHANNEL_VOLUME_UPLOADED
from opticstream.flows.lsm.prefect_events import emit_channel_lsm_event
from opticstream.flows.lsm.paths import channel_zarr_volume_path
from opticstream.flows.lsm.state_guards import (
    force_rerun_from_payload,
    prepare_idempotent_channel_milestone,
)
from opticstream.flows.lsm.utils import (
    channel_ident_from_payload,
    load_scan_config_for_payload,
)
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSM_STATE_SERVICE,
)


@task
def _upload_channel_volume_to_dandi(
    channel_ident: LSMChannelId,
    volume_path: str,
) -> None:
    """Placeholder: upload channel volume zarr (wire DANDI like strip_upload when ready)."""
    logger = get_run_logger()
    logger.info(
        f"Channel volume upload placeholder for {channel_ident} path={volume_path}"
    )


@flow(flow_run_name="upload-channel-volume-{channel_ident}")
def upload_channel_volume(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
    *,
    force_rerun: bool = False,
) -> None:
    """
    Upload channel volume, mark_completed, emit CHANNEL_VOLUME_UPLOADED.
    """
    logger = get_run_logger()
    ch_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    uploaded = bool(ch_view and ch_view.volume_uploaded)
    if (
        prepare_idempotent_channel_milestone(
            logger,
            milestone_done=uploaded,
            force_rerun=force_rerun,
            skip_log=(
                f"Channel {channel_ident} volume already uploaded; skipping (force_rerun=False)"
            ),
            force_log=(
                f"Channel {channel_ident} volume already uploaded; forcing rerun"
            ),
            channel_ident=channel_ident,
            reset_channel=lambda ch: ch.reset_volume_uploaded(),
        )
        == "skip"
    ):
        return

    volume_path = channel_zarr_volume_path(channel_ident, scan_config)
    _upload_channel_volume_to_dandi(channel_ident, volume_path)

    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        ch.set_volume_uploaded(True)
        ch.mark_completed()

    emit_channel_lsm_event(CHANNEL_VOLUME_UPLOADED, channel_ident)
    logger.info(f"Channel {channel_ident} volume uploaded")


@flow
def upload_channel_volume_event(payload: Dict[str, Any]) -> None:
    """Event entrypoint on CHANNEL_VOLUME_STITCHED."""
    channel_ident = channel_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(channel_ident.project_name, payload)
    upload_channel_volume(
        channel_ident=channel_ident,
        scan_config=cfg,
        force_rerun=force_rerun_from_payload(payload),
    )
