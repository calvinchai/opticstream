"""
Upload stitched channel volume (e.g. DANDI). Triggered on CHANNEL_VOLUME_STITCHED.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Sequence

from prefect import flow, get_run_logger, task

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.events.lsm_events import (
    CHANNEL_VOLUME_STITCHED,
    CHANNEL_VOLUME_UPLOADED,
)
from opticstream.events.lsm_event_emitters import emit_channel_lsm_event
from opticstream.events.utils import get_event_trigger
from opticstream.state.state_guards import (
    force_rerun_from_payload,
    enter_milestone_stage,
    should_skip_run,
)
from opticstream.flows.lsm.utils import (
    channel_ident_from_payload,
    channel_zarr_volume_path,
    host_lsm_fs_path,
    load_scan_config_for_payload,
)
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSM_STATE_SERVICE,
)
from opticstream.hooks import (
    publish_lsm_project_hook,
    publish_lsm_slice_hook,
    slack_notification_hook,
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


@flow(
    flow_run_name="upload-channel-volume-{channel_ident}",
    on_completion=[publish_lsm_project_hook, publish_lsm_slice_hook],
    on_failure=[slack_notification_hook],
)
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
    if should_skip_run(
        enter_milestone_stage(
            item_state_view=LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident),
            item_ident=channel_ident,
            field_name="volume_uploaded",
            force_rerun=force_rerun,
        )
    ):
        return

    volume_path = host_lsm_fs_path(
        channel_zarr_volume_path(channel_ident, scan_config)
    )
    _upload_channel_volume_to_dandi(channel_ident, os.fspath(volume_path))

    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        ch.set_volume_uploaded(True)
        ch.mark_completed()

    emit_channel_lsm_event(
        CHANNEL_VOLUME_UPLOADED,
        channel_ident,
        extra_payload={
            "volume_path": os.fspath(volume_path),
        },
    )
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


def to_deployment(
    *,
    project_name: Optional[str] = None,
    deployment_name: str = "local",
    extra_tags: Sequence[str] = (),
    concurrency_limit: int = 1,
):
    """
    Create both deployments:
    - manual `upload_channel_volume`
    - event-driven `upload_channel_volume_event` (triggered by CHANNEL_VOLUME_STITCHED)
    """
    manual = upload_channel_volume.to_deployment(
        name=deployment_name,
        tags=["lsm", "channel", "upload", *list(extra_tags)],
        concurrency_limit=concurrency_limit,
    )
    event = upload_channel_volume_event.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "channel", "upload", *list(extra_tags)],
        concurrency_limit=concurrency_limit,
        triggers=[
            get_event_trigger(CHANNEL_VOLUME_STITCHED, project_name=project_name)
        ],
    )
    return [manual, event]
