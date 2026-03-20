"""LSM strip/slice/project Prefect artifacts: pure summaries, rendering, and tasks."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from prefect import get_run_logger, task

from opticstream.artifacts.common import (
    bool_cell_emoji,
    format_milestone_lines,
    pct,
    publish_table_artifact,
    timestamp_str,
)
from opticstream.config.lsm_scan_config import get_lsm_scan_config
from opticstream.state.lsm_project_state import (
    LSM_STATE_SERVICE,
    LSMProjectId,
    LSMProjectStateView,
    LSMSliceId,
    LSMSliceStateView,
    LSMStripStateView,
)
from opticstream.state.project_state_core import ProcessingState
from opticstream.utils.naming_convention import normalize_project_name


def _lsm_strip_row(
    *,
    strip_id: int,
    channel_id: int,
    strip_view: LSMStripStateView | None,
) -> dict[str, Any]:
    """One row per strip×channel, same column pattern as OCT batch tables (fixed keys, string values)."""
    if strip_view is None:
        return {
            "Strip": strip_id,
            "Channel": channel_id,
            "Processing": "pending",
            "Compressed": bool_cell_emoji(False),
            "Archived": bool_cell_emoji(False),
            "Uploaded": bool_cell_emoji(False),
        }
    return {
        "Strip": strip_id,
        "Channel": channel_id,
        "Processing": strip_view.processing_state.value,
        "Compressed": bool_cell_emoji(strip_view.compressed),
        "Archived": bool_cell_emoji(strip_view.archived),
        "Uploaded": bool_cell_emoji(strip_view.uploaded),
    }


@dataclass(frozen=True)
class LSMStripChannelRollup:
    """Per-slice, per-channel strip counts for the project summary table."""

    slice_id: int
    channel_id: int
    total_strips: int
    compressed: int
    archived: int
    uploaded: int
    completed: int

    @property
    def progress_pct(self) -> float:
        return pct(self.completed, self.total_strips)

    def to_table_row(self) -> dict[str, Any]:
        return {
            "Slice": self.slice_id,
            "Channel": self.channel_id,
            "Total Strips": self.total_strips,
            "Compressed": self.compressed,
            "Archived": self.archived,
            "Uploaded": self.uploaded,
            "Completed": self.completed,
            "Progress": f"{self.progress_pct:.1f}%",
        }


def build_project_strip_summary_rows(
    state: LSMProjectStateView,
    strips_per_slice: int,
) -> tuple[list[dict[str, Any]], int, int]:
    rollups: list[LSMStripChannelRollup] = []
    total_completed_strips = 0
    total_strip_slots = 0

    for slice_id in sorted(state.slices.keys()):
        slice_view = state.slices[slice_id]
        for channel_id in sorted(slice_view.channels.keys()):
            channel_view = slice_view.channels[channel_id]
            total = strips_per_slice
            compressed = archived = uploaded = completed = 0
            for strip_id in range(1, total + 1):
                sv = channel_view.strips.get(strip_id)
                if sv is None:
                    continue
                if sv.compressed:
                    compressed += 1
                if sv.archived:
                    archived += 1
                if sv.uploaded:
                    uploaded += 1
                if sv.processing_state == ProcessingState.COMPLETED:
                    completed += 1
            rollups.append(
                LSMStripChannelRollup(
                    slice_id=slice_id,
                    channel_id=channel_id,
                    total_strips=total,
                    compressed=compressed,
                    archived=archived,
                    uploaded=uploaded,
                    completed=completed,
                )
            )
            total_completed_strips += completed
            total_strip_slots += total

    return [r.to_table_row() for r in rollups], total_completed_strips, total_strip_slots


def build_slice_strip_matrix_rows(
    slice_view: LSMSliceStateView,
    strips_per_slice: int,
) -> list[dict[str, Any]]:
    """
    Long-format strip status table (same idea as OCT per-batch tables): one row per
    (strip, channel) with fixed columns. Wide pivot tables with ``Ch N`` columns
    break Markdown rendering in the Prefect UI.
    """
    channel_ids = sorted(slice_view.channels.keys())
    table_data: list[dict[str, Any]] = []
    for strip_id in range(1, strips_per_slice + 1):
        for channel_id in channel_ids:
            cell_strip = slice_view.channels[channel_id].strips.get(strip_id)
            table_data.append(
                _lsm_strip_row(
                    strip_id=strip_id,
                    channel_id=channel_id,
                    strip_view=cell_strip,
                )
            )
    return table_data


def build_lsm_project_artifact_key(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}-lsm-strip-progress"


def build_lsm_slice_matrix_artifact_key(project_name: str, slice_id: int) -> str:
    return f"{normalize_project_name(project_name)}-lsm-slice-{slice_id:04d}-strips"


def build_lsm_project_strip_summary_description(
    *,
    project_name: str,
    total_completed_strips: int,
    total_strip_slots: int,
) -> str:
    overall_pct = pct(total_completed_strips, total_strip_slots)
    return f"""LSM project strip processing summary

Project: {project_name}
Last Updated: {timestamp_str()}

Overall completed strip slots: {total_completed_strips}/{total_strip_slots} ({overall_pct:.1f}%)

{format_milestone_lines(overall_pct)}
"""


def build_lsm_slice_matrix_description(*, project_name: str, slice_id: int) -> str:
    return f"""LSM slice strip status (one row per strip × channel)

Project: {project_name}
Slice: {slice_id}
Last Updated: {timestamp_str()}

Columns match the OCT batch table style: Processing is workflow state; Compressed, Archived, and Uploaded use ✅ / ❌.
"""


@task
def publish_project_artifact_task(
    project_ident: LSMProjectId,
    *,
    override_config_name: str | None = None,
) -> str:
    return publish_lsm_project_artifact(
        project_ident,
        override_config_name=override_config_name,
        logger=get_run_logger(),
    )


def publish_lsm_project_artifact(
    project_ident: LSMProjectId,
    *,
    override_config_name: str | None = None,
    logger: Any | None = None,
) -> str:
    logger = logger or logging.getLogger(__name__)
    project_name = project_ident.project_name
    state = LSM_STATE_SERVICE.peek_project(project_ident)
    cfg = get_lsm_scan_config(project_name, override_config_name)
    strips_per_slice = cfg.strips_per_slice

    artifact_key = build_lsm_project_artifact_key(project_name)
    table_data, total_completed_strips, total_strip_slots = build_project_strip_summary_rows(
        state=state,
        strips_per_slice=strips_per_slice,
    )

    if not table_data:
        logger.warning("No slice/channel rows in LSM state for artifact %s", artifact_key)
        return ""

    description = build_lsm_project_strip_summary_description(
        project_name=project_name,
        total_completed_strips=total_completed_strips,
        total_strip_slots=total_strip_slots,
    )
    publish_table_artifact(key=artifact_key, table=table_data, description=description)
    return artifact_key


@task
def publish_slice_artifact_task(
    slice_ident: LSMSliceId,
    *,
    override_config_name: str | None = None,
) -> str:
    return publish_lsm_slice_artifact(
        slice_ident,
        override_config_name=override_config_name,
        logger=get_run_logger(),
    )


def publish_lsm_slice_artifact(
    slice_ident: LSMSliceId,
    *,
    override_config_name: str | None = None,
    logger: Any | None = None,
) -> str:
    logger = logger or logging.getLogger(__name__)
    project_name = slice_ident.project_name
    slice_view = LSM_STATE_SERVICE.peek_slice(slice_ident)
    if slice_view is None:
        logger.warning(
            "No LSM slice state for slice %s in project %s",
            slice_ident.slice_id,
            project_name,
        )
        return ""

    cfg = get_lsm_scan_config(project_name, override_config_name)
    strips_per_slice = cfg.strips_per_slice
    slice_id = slice_ident.slice_id
    artifact_key = build_lsm_slice_matrix_artifact_key(project_name, slice_id)
    slice_table = build_slice_strip_matrix_rows(
        slice_view=slice_view,
        strips_per_slice=strips_per_slice,
    )
    description = build_lsm_slice_matrix_description(project_name=project_name, slice_id=slice_id)
    publish_table_artifact(key=artifact_key, table=slice_table, description=description)
    return artifact_key


@task
def publish_all_slice_matrix_artifacts_task(
    project_ident: LSMProjectId,
    *,
    override_config_name: str | None = None,
) -> list[str]:
    """Publish one strip-matrix artifact per slice in the project; returns all artifact keys."""
    project_name = project_ident.project_name
    state = LSM_STATE_SERVICE.peek_project(project_ident)
    cfg = get_lsm_scan_config(project_name, override_config_name)
    strips_per_slice = cfg.strips_per_slice

    keys: list[str] = []
    for slice_id in sorted(state.slices.keys()):
        slice_view = state.slices[slice_id]
        slice_table = build_slice_strip_matrix_rows(
            slice_view=slice_view,
            strips_per_slice=strips_per_slice,
        )
        slice_artifact_key = build_lsm_slice_matrix_artifact_key(project_name, slice_id)
        description = build_lsm_slice_matrix_description(
            project_name=project_name,
            slice_id=slice_id,
        )
        publish_table_artifact(
            key=slice_artifact_key,
            table=slice_table,
            description=description,
        )
        keys.append(slice_artifact_key)
    return keys
