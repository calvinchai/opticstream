"""
Channel volume (3D) stitching. Emits CHANNEL_VOLUME_STITCHED after zarr validation.

Upload runs in channel_upload_flow (separate deployment).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from prefect import flow, get_run_logger, task

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.events.lsm_events import CHANNEL_MIP_STITCHED, CHANNEL_VOLUME_STITCHED
from opticstream.events.lsm_event_emitters import emit_channel_lsm_event
from opticstream.events.utils import get_event_trigger
from opticstream.flows.lsm.paths import channel_zarr_volume_path
from opticstream.utils.zarr_validation import ValidationResult, validate_zarr_directory
from opticstream.state.state_guards import (
    RunDecision,
    force_rerun_from_payload,
    enter_milestone_stage,
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
def _stitch_channel_volume(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
) -> str:
    """
    Placeholder: stitch per-strip Zarr volumes into one channel volume.

    Ensures output directory exists; real implementation should write the zarr store.
    """
    import os

    logger = get_run_logger()
    out = channel_zarr_volume_path(channel_ident, scan_config)
    os.makedirs(out, exist_ok=True)
    marker = os.path.join(out, ".opticstream_volume_placeholder")
    if not os.path.exists(marker):
        with open(marker, "w", encoding="utf-8") as f:
            f.write("placeholder\n")
    logger.info(f"Volume stitch placeholder for {channel_ident} -> {out}")
    return out


@task(task_run_name="check-channel-volume-{channel_ident}")
def check_channel_volume_result(
    channel_ident: LSMChannelId,
    volume_path: str,
    zarr_size_threshold: int,
) -> ValidationResult:
    """Validate stitched channel volume zarr (same idea as strip zarr in check_compressed_result)."""
    logger = get_run_logger()
    return validate_zarr_directory(
        logger,
        volume_path,
        zarr_size_threshold,
        context=str(channel_ident),
        missing_reason="channel volume zarr missing",
        empty_reason="channel volume directory empty",
        below_threshold_reason="channel volume zarr below size threshold",
    )


@flow(flow_run_name="process-channel-volume-{channel_ident}")
def process_channel_volume(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
    *,
    force_rerun: bool = False,
) -> Optional[str]:
    """
    Stitch (placeholder), validate zarr, set volume_stitched, emit CHANNEL_VOLUME_STITCHED.
    Does not mark_completed (upload flow does after upload).
    """
    logger = get_run_logger()
    ch_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    if (
        enter_milestone_stage(
            item_state_view=ch_view,
            item_ident=channel_ident,
            field_name="volume_stitched",
            force_rerun=force_rerun,
        )
        == RunDecision.SKIPPED
    ):
        return None

    volume_path = _stitch_channel_volume(
        channel_ident=channel_ident,
        scan_config=scan_config,
    )
    zthr = (
        0
        if scan_config.skip_channel_volume_zarr_validation
        else scan_config.channel_volume_zarr_size_threshold
    )
    check = check_channel_volume_result(
        channel_ident=channel_ident,
        volume_path=volume_path,
        zarr_size_threshold=zthr,
    )
    if not check.ok:
        with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
            ch.mark_failed()
        raise RuntimeError(
            f"Channel volume validation failed for {channel_ident}: {check.reason}"
        )

    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        ch.set_volume_stitched(True)

    emit_channel_lsm_event(
        CHANNEL_VOLUME_STITCHED,
        channel_ident,
        extra_payload={
            "volume_path": volume_path,
            "zarr_size_threshold": zthr,
        },
    )
    logger.info(f"Emitted {CHANNEL_VOLUME_STITCHED} for {channel_ident}")
    return volume_path


@flow
def process_channel_volume_event(payload: Dict[str, Any]) -> None:
    """Event entrypoint on CHANNEL_MIP_STITCHED."""
    channel_ident = channel_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(channel_ident.project_name, payload)
    process_channel_volume(
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
    - manual `process_channel_volume`
    - event-driven `process_channel_volume_event` (triggered by CHANNEL_MIP_STITCHED)
    """
    manual = process_channel_volume.to_deployment(
        name=deployment_name,
        tags=["lsm", "channel", "volume-stitch", *list(extra_tags)],
        concurrency_limit=concurrency_limit,
    )
    event = process_channel_volume_event.to_deployment(
        name=deployment_name,
        tags=["event-driven", "lsm", "channel", "volume-stitch", *list(extra_tags)],
        concurrency_limit=concurrency_limit,
        triggers=[get_event_trigger(CHANNEL_MIP_STITCHED, project_name=project_name)],
    )
    return [manual, event]
