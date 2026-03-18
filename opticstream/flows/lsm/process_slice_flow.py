"""
Channel-level LSM processing flows.

After all strips in a given (slice, channel) are finished, we stitch
per-strip MIP outputs into a quick QC mosaic for that channel.
"""

from __future__ import annotations

import os
import os.path as op
from typing import Dict, List, Optional

from prefect import flow, get_run_logger, task

from opticstream.config.lsm_scan_config import (
    LSMScanConfigModel,
    get_lsm_scan_config,
)
from opticstream.flows.lsm.event import STRIP_COMPRESSED
from opticstream.state.lsm_project_state import (
    LSMChannelId,
    LSM_STATE_SERVICE,
)
from opticstream.events import get_event_trigger


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

    output_root = scan_config.output_path or scan_config.project_base_path
    acq = f"camera-{channel_ident.channel_id:02d}"

    mip_paths: List[str] = []
    for strip_id in range(1, scan_config.strips_per_slice + 1):
        mip_name = scan_config.output_mip_format.format(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            strip_id=strip_id,
            acq=acq,
        )
        mip_path = op.join(output_root, mip_name)
        if os.path.exists(mip_path):
            mip_paths.append(mip_path)
        else:
            logger.warning(
                "Expected MIP not found for slice %s strip %s channel %s at %s",
                channel_ident.slice_id,
                strip_id,
                channel_ident.channel_id,
                mip_path,
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
            "No MIP images found to stitch for slice %s channel %s in project %s",
            channel_ident.slice_id,
            channel_ident.channel_id,
            channel_ident.project_name,
        )
        return None

    logger.info(
        "Stitching %d MIP images for slice %s channel %s in project %s",
        len(mip_paths),
        channel_ident.slice_id,
        channel_ident.channel_id,
        channel_ident.project_name,
    )

    # TODO: replace this placeholder with actual stitching logic.
    # For now, we simply record the path where a stitched QC image would go.
    output_root = scan_config.output_path or scan_config.project_base_path
    stitched_name = (
        f"{channel_ident.project_name}_slice-{channel_ident.slice_id:02d}_channel-{channel_ident.channel_id:02d}_mip_qc.tiff"
    )
    stitched_path = op.join(output_root, stitched_name)

    logger.info(
        "Channel-level stitched QC image would be written to %s", stitched_path
    )
    return stitched_path


@flow
def process_channel_flow(
    channel_ident: LSMChannelId,
    scan_config: LSMScanConfigModel,
) -> Optional[str]:
    """
    Process a single (slice, channel) once all strips are finished.

    Currently this means stitching per-strip MIP images for quick QC and
    marking the channel as stitched/completed in the LSM project state.
    """
    logger = get_run_logger()
    logger.info(f"Starting channel-level processing for {channel_ident}")

    mip_paths = _collect_channel_mip_paths(
        channel_ident=channel_ident,
        scan_config=scan_config,
    )

    stitched_path = _stitch_channel_mips(
        channel_ident=channel_ident,
        mip_paths=mip_paths,
        scan_config=scan_config,
    )

    # Update channel state to mark it as stitched/completed.
    with LSM_STATE_SERVICE.open_channel(
        channel_ident=channel_ident,
    ) as channel_state:
        channel_state.set_stitched(True)
        channel_state.mark_completed()

    logger.info(
        "Completed channel-level processing for project=%s slice=%s channel=%s",
        channel_ident.project_name,
        channel_ident.slice_id,
        channel_ident.channel_id,
    )
    return stitched_path


@flow
def lsm_channel_completion_flow(payload: Dict[str, object]) -> None:
    """
    Event-driven flow that checks if all strips in a channel are finished.

    Triggered on STRIP_COMPRESSED events. Uses strips_per_slice from the LSM
    scan configuration and LSM_STATE_SERVICE to detect when a channel is
    ready for channel-level processing, then calls process_channel_flow.
    """
    logger = get_run_logger()

    project_name = str(payload.get("project_name"))
    slice_id = int(payload.get("slice_id"))
    channel_id_raw = payload.get("camera_id") or payload.get("channel_id") or 1
    channel_id = int(channel_id_raw)
    override_config_name = payload.get("override_config_name")

    logger.info(
        "Evaluating channel completion for project=%s slice=%s channel=%s",
        project_name,
        slice_id,
        channel_id,
    )

    scan_config = get_lsm_scan_config(
        project_name,
        override_config_name=override_config_name,
    )

    channel_ident = LSMChannelId(
        project_name=project_name,
        slice_id=slice_id,
        channel_id=channel_id,
    )

    # Read the current channel view to decide whether all strips are finished.
    channel_view = LSM_STATE_SERVICE.read_channel(
        channel_ident=channel_ident,
    )
    if channel_view is None:
        logger.info(
            "No channel state found yet for project=%s slice=%s channel=%s; skipping",
            project_name,
            slice_id,
            channel_id,
        )
        return

    if channel_view.stitched:
        logger.info(
            "Channel already stitched for project=%s slice=%s channel=%s; skipping",
            project_name,
            slice_id,
            channel_id,
        )
        return

    if not channel_view.all_completed(total_strips=scan_config.strips_per_slice):
        logger.info(
            "Channel not yet finished (some strips pending) for project=%s slice=%s channel=%s",
            project_name,
            slice_id,
            channel_id,
        )
        return

    logger.info(
        "All strips finished for project=%s slice=%s channel=%s; starting channel processing",
        project_name,
        slice_id,
        channel_id,
    )
    process_channel_flow(
        channel_ident=channel_ident,
        scan_config=scan_config,
    )


if __name__ == "__main__":
    lsm_channel_completion_deployment = lsm_channel_completion_flow.to_deployment(
        name="lsm_channel_completion_flow",
        tags=["lsm", "channel-completion"],
        triggers=[
            get_event_trigger(STRIP_COMPRESSED),
        ],
    )

