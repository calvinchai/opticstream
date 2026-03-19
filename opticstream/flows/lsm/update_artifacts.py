from datetime import datetime
from typing import Any, Dict, List

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from opticstream.state.lsm_project_state import (LSMProjectStateView, LSMSliceStateView,
                                                 LSMStripStateView)
from opticstream.state.project_state_core import ProcessingState


def _strip_status_cell(strip_view: LSMStripStateView | None) -> str:
    if strip_view is None:
        return "pending | cN aN uN"
    return (
        f"{strip_view.processing_state.value} | "
        f"c{'Y' if strip_view.compressed else 'N'} "
        f"a{'Y' if strip_view.archived else 'N'} "
        f"u{'Y' if strip_view.uploaded else 'N'}"
    )


def build_project_strip_summary_rows(
    state: LSMProjectStateView,
    strips_per_slice: int,
) -> tuple[List[Dict[str, Any]], int, int]:
    table_data: List[Dict[str, Any]] = []
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
            pct = (completed / total * 100.0) if total else 0.0
            table_data.append(
                {
                    "Slice": slice_id,
                    "Channel": channel_id,
                    "Total Strips": total,
                    "Compressed": compressed,
                    "Archived": archived,
                    "Uploaded": uploaded,
                    "Completed": completed,
                    "Progress": f"{pct:.1f}%",
                }
            )
            total_completed_strips += completed
            total_strip_slots += total

    return table_data, total_completed_strips, total_strip_slots


def build_slice_strip_matrix_rows(
    slice_view: LSMSliceStateView,
    strips_per_slice: int,
) -> List[Dict[str, Any]]:
    channel_ids = sorted(slice_view.channels.keys())
    table_data: List[Dict[str, Any]] = []

    for strip_id in range(1, strips_per_slice + 1):
        row: Dict[str, Any] = {"Strip": strip_id}
        for channel_id in channel_ids:
            strip_view = slice_view.channels[channel_id].strips.get(strip_id)
            row[f"Ch {channel_id}"] = _strip_status_cell(strip_view)
        table_data.append(row)

    return table_data


@task(task_run_name="{project_name}-lsm-project-strip-artifact")
def publish_project_strip_summary_artifact_task(
    project_name: str,
    state: LSMProjectStateView,
    strips_per_slice: int,
) -> str:
    logger = get_run_logger()
    project_slug = project_name.lower().replace("_", "-")
    artifact_key = f"{project_slug}-lsm-strip-progress"
    table_data, total_completed_strips, total_strip_slots = (
        build_project_strip_summary_rows(
            state=state,
            strips_per_slice=strips_per_slice,
        )
    )

    if not table_data:
        logger.warning(
            f"No slice/channel rows in LSM state for artifact {artifact_key}"
        )
        return ""

    overall_pct = (
        total_completed_strips / total_strip_slots * 100.0
        if total_strip_slots
        else 0.0
    )
    milestones = [
        "✅" if overall_pct >= 25 else "⏳",
        "✅" if overall_pct >= 50 else "⏳",
        "✅" if overall_pct >= 75 else "⏳",
        "✅" if overall_pct >= 100 else "⏳",
    ]
    description = f"""LSM project strip processing summary

Project: {project_name}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Overall completed strip slots: {total_completed_strips}/{total_strip_slots} ({overall_pct:.1f}%)

Milestones:
- {milestones[0]} 25% Complete
- {milestones[1]} 50% Complete
- {milestones[2]} 75% Complete
- {milestones[3]} 100% Complete
"""

    create_table_artifact(key=artifact_key, table=table_data, description=description)
    return artifact_key


@task(task_run_name="{project_name}-lsm-slice-strip-artifacts")
def publish_slice_strip_matrix_artifacts_task(
    project_name: str,
    state: LSMProjectStateView,
    strips_per_slice: int,
) -> int:
    project_slug = project_name.lower().replace("_", "-")
    updated = 0
    for slice_id in sorted(state.slices.keys()):
        slice_view = state.slices[slice_id]
        slice_table = build_slice_strip_matrix_rows(
            slice_view=slice_view,
            strips_per_slice=strips_per_slice,
        )
        slice_artifact_key = f"{project_slug}-lsm-slice-{slice_id:04d}-strips"
        slice_description = f"""LSM slice strip status matrix

Project: {project_name}
Slice: {slice_id}
Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Cell legend: <state> | cY/cN aY/aN uY/uN
"""
        create_table_artifact(
            key=slice_artifact_key,
            table=slice_table,
            description=slice_description,
        )
        updated += 1
    return updated
