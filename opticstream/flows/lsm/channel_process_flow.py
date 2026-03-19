"""
Channel-level LSM processing flows.

After all strips in a given (slice, channel) are compressed, we synchronously
stitch per-strip MIP outputs for QC. If volume stitching is enabled, we emit
CHANNEL_MIP_STITCHED so channel_volume_flow can run 3D stitching afterward.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from prefect import flow, get_run_logger, task

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.flows.lsm.paths import strip_mip_output_path
from opticstream.flows.lsm.event import CHANNEL_MIP_STITCHED
from opticstream.flows.lsm.prefect_events import emit_channel_lsm_event
from opticstream.flows.lsm.state_guards import (
    enter_flow_stage,
    force_rerun_from_payload,
    RunDecision,
)
from opticstream.flows.lsm.utils import (
    channel_ident_from_payload,
    load_scan_config_for_payload,
)
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSMStripId,
    LSM_STATE_SERVICE,
)
from opticstream.utils.slack_notification import notify_slack


@task
def _collect_channel_mip_paths(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
) -> List[str]:
    """
    Collect expected per-strip MIP paths for a given (slice, channel).

    This mirrors the naming used in process_strip_flow for MIP outputs and
    uses strips_per_slice from the scan configuration to determine how many
    strips to expect.
    """
    logger = get_run_logger()

    mip_paths: List[str] = []
    for strip_id in range(1, scan_config.strips_per_slice + 1):
        sid = LSMStripId(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
            strip_id=strip_id,
        )
        mip_path = strip_mip_output_path(sid, scan_config)
        if os.path.exists(mip_path):
            mip_paths.append(mip_path)
        else:
            logger.warning(
                f"Expected MIP not found for slice {channel_ident.slice_id} "
                f"strip {strip_id} channel {channel_ident.channel_id} at {mip_path}"
            )

    return mip_paths


@task
def _stitch_channel_mips(
    channel_ident: LSMChannelId,
    mip_paths: List[str],
    scan_config: LSMScanConfigModel,
) -> Optional[str]:
    """
    Stitch per-strip MIP images for a channel into a single QC mosaic.

    This is a placeholder implementation that simply logs the list of MIP
    files that would be stitched. It can be extended to call a real stitching
    library when available.
    """
    logger = get_run_logger()

    if not mip_paths:
        logger.warning(
            f"No MIP images found to stitch for slice {channel_ident.slice_id} "
            f"channel {channel_ident.channel_id} in project {channel_ident.project_name}"
        )
        return None

    logger.info(
        f"Stitching {len(mip_paths)} MIP images for slice {channel_ident.slice_id} "
        f"channel {channel_ident.channel_id} in project {channel_ident.project_name}"
    )

    # TODO: replace this placeholder with actual stitching logic.
    import os.path as op

    output_root = scan_config.output_path or scan_config.project_base_path
    stitched_name = (
        f"{channel_ident.project_name}_slice-{channel_ident.slice_id:02d}_"
        f"channel-{channel_ident.channel_id:02d}_mip_qc.tiff"
    )
    stitched_path = op.join(output_root, stitched_name)

    logger.info(
        f"Channel-level stitched QC image would be written to {stitched_path}"
    )
    return stitched_path


@task(task_run_name="notify-mip-{channel_ident}")
def _notify_channel_mip_stitched(
    channel_ident: LSMChannelId,
    stitched_path: Optional[str],
) -> None:
    """Slack notification after MIP stitch (placeholder path until real write)."""
    if stitched_path is None:
        return
    notify_slack(
        status="success",
        message=(
            f"MIP QC stitch finished for slice {channel_ident.slice_id} "
            f"channel {channel_ident.channel_id}"
        ),
        details={
            "project_name": channel_ident.project_name,
            "stitched_path": stitched_path,
        },
    )


@flow(flow_run_name="process-channel-{channel_ident}")
def process_channel(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
    *,
    force_rerun: bool = False,
) -> Optional[str]:
    """
    Process a (slice, channel) after strips are ready: synchronous MIP stitch,
    then either complete the channel or emit CHANNEL_MIP_STITCHED for volume flow.
    """
    logger = get_run_logger()
    logger.info(f"Processing channel: {channel_ident}")

    channel_view = LSM_STATE_SERVICE.peek_channel(channel_ident=channel_ident)
    if (
        enter_flow_stage(
        channel_view,
        force_rerun=force_rerun,
        skip_if_running=True,
        item_ident=channel_ident,
        )
        == RunDecision.SKIPPED
    ):
        return None
    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        ch.reset_mip_stitched()

    mip_stitched_path: Optional[str] = None

    if scan_config.generate_mip:
        mip_paths = _collect_channel_mip_paths(
            channel_ident=channel_ident,
            scan_config=scan_config,
        )
        mip_stitched_path = _stitch_channel_mips(
            channel_ident=channel_ident,
            mip_paths=mip_paths,
            scan_config=scan_config,
        )
        _notify_channel_mip_stitched(
            channel_ident=channel_ident,
            stitched_path=mip_stitched_path,
        )

        expected = scan_config.strips_per_slice
        if len(mip_paths) < expected or mip_stitched_path is None:
            with LSM_STATE_SERVICE.open_channel(
                channel_ident=channel_ident
            ) as ch:
                ch.mark_failed()
            raise RuntimeError(
                f"MIP stitch failed for {channel_ident}: "
                f"found {len(mip_paths)}/{expected} MIPs, stitched_path={mip_stitched_path!r}"
            )

    with LSM_STATE_SERVICE.open_channel(channel_ident=channel_ident) as ch:
        ch.set_mip_stitched(True)
        if not scan_config.stitch_volume:
            ch.set_volume_stitched(True)
            ch.mark_completed()

    if not scan_config.stitch_volume:
        logger.info(f"Completed channel {channel_ident} (no volume stitch)")
        return mip_stitched_path

    emit_channel_lsm_event(CHANNEL_MIP_STITCHED, channel_ident)
    logger.info(
        f"Emitted {CHANNEL_MIP_STITCHED} for {channel_ident}; "
        f"volume flow will complete the channel"
    )
    return mip_stitched_path


@flow
def process_channel_event(payload: Dict[str, Any]) -> None:
    """Event entrypoint (e.g. on CHANNEL_READY)."""
    channel_ident = channel_ident_from_payload(payload)
    cfg = load_scan_config_for_payload(channel_ident.project_name, payload)
    process_channel(
        channel_ident=channel_ident,
        scan_config=cfg,
        force_rerun=force_rerun_from_payload(payload),
    )
