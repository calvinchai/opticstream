"""Per-project detail: hierarchical state view for LSM and OCT projects."""

from __future__ import annotations

from datetime import datetime

import streamlit as st

from opticstream.state.project_state_core import ProcessingState
from opticstream.state.lsm_state_service import LSM_STATE_SERVICE
from opticstream.state.oct_state_service import OCT_STATE_SERVICE
from opticstream.state.lsm_models import (
    LSMProjectStateView,
    LSMSliceStateView,
    LSMChannelStateView,
)
from opticstream.state.oct_models import (
    OCTProjectStateView,
    OCTSliceStateView,
    OCTMosaicStateView,
)


def _status_icon(status: str) -> str:
    if status == "completed":
        return ":material/check_circle:"
    if status == "running":
        return ":material/sync:"
    if status == "failed":
        return ":material/error:"
    return ":material/hourglass_empty:"


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _state_metrics(state) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", state.processing_state.value)
    c2.write(f"**Created:** {_fmt_dt(state.created_at)}")
    c3.write(f"**Updated:** {_fmt_dt(state.updated_at)}")
    started = _fmt_dt(state.processing_started_at)
    finished = _fmt_dt(state.processing_finished_at)
    c4.write(f"**Started:** {started}  \n**Finished:** {finished}")


def _render_lsm_project(name: str) -> None:
    try:
        view = LSM_STATE_SERVICE.peek_project_by_parts(name)
    except Exception as e:
        st.error(f"Failed to load project: {e}")
        return

    _state_metrics(view)

    if not view.slices:
        st.info("No slices in this project.")
        return

    st.subheader(f"Slices ({len(view.slices)})")
    for sid in sorted(view.slices):
        sl = view.slices[sid]
        label = f"Slice {sid} — {sl.processing_state.value}"
        with st.expander(f"{_status_icon(sl.processing_state.value)} {label}", expanded=False):
            _state_metrics(sl)

            if not sl.channels:
                st.caption("No channels.")
                continue

            for cid in sorted(sl.channels):
                ch = sl.channels[cid]
                ch_label = f"Channel {cid} — {ch.processing_state.value}"
                st.markdown(f"**{_status_icon(ch.processing_state.value)} {ch_label}**")

                flags = []
                if ch.mip_stitched:
                    flags.append("mip_stitched")
                if ch.volume_stitched:
                    flags.append("volume_stitched")
                if ch.volume_uploaded:
                    flags.append("volume_uploaded")
                if flags:
                    st.caption(" · ".join(flags))

                if ch.strips:
                    strip_rows = []
                    for stid in sorted(ch.strips):
                        s = ch.strips[stid]
                        strip_rows.append(
                            {
                                "Strip": stid,
                                "Status": s.processing_state.value,
                                "Archived": s.archived,
                                "Compressed": s.compressed,
                                "Uploaded": s.uploaded,
                                "Updated": _fmt_dt(s.updated_at),
                            }
                        )
                    st.dataframe(strip_rows, use_container_width=True, hide_index=True)


def _render_oct_project(name: str) -> None:
    try:
        view = OCT_STATE_SERVICE.peek_project_by_parts(name)
    except Exception as e:
        st.error(f"Failed to load project: {e}")
        return

    _state_metrics(view)

    if not view.slices:
        st.info("No slices in this project.")
        return

    st.subheader(f"Slices ({len(view.slices)})")
    for sid in sorted(view.slices):
        sl = view.slices[sid]
        label = f"Slice {sid} — {sl.processing_state.value}"
        slice_flags = []
        if sl.registered:
            slice_flags.append("registered")
        if sl.uploaded:
            slice_flags.append("uploaded")
        extra = f" ({', '.join(slice_flags)})" if slice_flags else ""

        with st.expander(
            f"{_status_icon(sl.processing_state.value)} {label}{extra}",
            expanded=False,
        ):
            _state_metrics(sl)

            if not sl.mosaics:
                st.caption("No mosaics.")
                continue

            for mid in sorted(sl.mosaics):
                mo = sl.mosaics[mid]
                mo_label = f"Mosaic {mid} — {mo.processing_state.value}"
                st.markdown(f"**{_status_icon(mo.processing_state.value)} {mo_label}**")

                flags = []
                if mo.enface_stitched:
                    flags.append("enface_stitched")
                if mo.volume_stitched:
                    flags.append("volume_stitched")
                if mo.enface_uploaded:
                    flags.append("enface_uploaded")
                if mo.volume_uploaded:
                    flags.append("volume_uploaded")
                if flags:
                    st.caption(" · ".join(flags))

                if mo.batches:
                    batch_rows = []
                    for bid in sorted(mo.batches):
                        b = mo.batches[bid]
                        batch_rows.append(
                            {
                                "Batch": bid,
                                "Status": b.processing_state.value,
                                "Complexed": b.complexed,
                                "Vol proc": b.volume_processed,
                                "Enface proc": b.enface_processed,
                                "Uploaded": b.uploaded,
                                "Archived": b.archived,
                                "Updated": _fmt_dt(b.updated_at),
                            }
                        )
                    st.dataframe(batch_rows, use_container_width=True, hide_index=True)


def main() -> None:
    project_name = st.session_state.get("_hub_open_project")
    project_type = st.session_state.get("_hub_open_project_type")

    if not project_name or not project_type:
        st.info("No project selected. Pick a project from the **Projects** page.")
        return

    if st.button(":material/arrow_back: Back to projects", key="project_detail_back"):
        st.session_state.pop("_hub_open_project", None)
        st.session_state.pop("_hub_open_project_type", None)
        st.rerun()

    st.header(f"{project_type.upper()} project: `{project_name}`")

    if project_type == "lsm":
        _render_lsm_project(project_name)
    elif project_type == "oct":
        _render_oct_project(project_name)
    else:
        st.error(f"Unknown project type: {project_type}")
