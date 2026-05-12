"""Per-project detail: hierarchical state view for LSM and OCT projects."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st
from redis import Redis
from rq import Queue

from opticapi.naming import ProjectQueueKind, queue_name_for_project
from opticapi.project_state.lsm_state_service import LSMProjectStateService
from opticapi.project_state.oct_state_service import OCTProjectStateService

from optichub.dashboard.hub_ui import hub_settings
from optichub.dashboard.project_hub import make_state_backend, project_status_icon


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


def _fmt_any_ts(val: object) -> str:
    if val is None:
        return "—"
    if isinstance(val, datetime):
        return _fmt_dt(val)
    return str(val)


def _queue_target_label(kind: ProjectQueueKind, payload: dict[str, Any]) -> str:
    if kind == "lsm":
        sid = payload.get("lsm_strip_id")
        if not isinstance(sid, dict):
            return "?"
        return (
            f"slice={sid.get('slice_id', '?')}/ch={sid.get('channel_id', '?')}/strip={sid.get('strip_id', '?')}"
        )
    bid = payload.get("batch_id")
    if not isinstance(bid, dict):
        return "?"
    return f"slice={bid.get('slice_id', '?')}/mosaic={bid.get('mosaic_id', '?')}/batch={bid.get('batch_id', '?')}"


def _func_short_tag(func_name: str | None) -> str:
    if not func_name:
        return "?"
    return func_name.rsplit(".", maxsplit=1)[-1]


def _render_queues(project_name: str, kind: ProjectQueueKind, redis_url: str) -> None:
    st.subheader("Queues")
    try:
        conn = Redis.from_url(redis_url, decode_responses=False)
    except Exception as e:
        st.warning(f"Could not connect to Redis for queue view: {e}")
        return

    for label, backlog in (("Realtime", False), ("Backlog", True)):
        qname = queue_name_for_project(project_name, kind, backlog=backlog)
        try:
            q = Queue(qname, connection=conn)
            jobs = q.jobs
        except Exception as e:
            st.warning(f"Could not read queue `{qname}`: {e}")
            continue

        st.caption(f"{label} queue: `{qname}` ({len(jobs)} jobs)")
        if not jobs:
            st.caption("Empty")
            continue

        rows: list[dict[str, Any]] = []
        for job in jobs:
            payload: dict[str, Any] = {}
            if job.args and len(job.args) >= 1 and isinstance(job.args[0], dict):
                payload = job.args[0]
            jid = job.id or ""
            short_id = jid[:8] + ("…" if len(jid) > 8 else "")
            rows.append(
                {
                    "Job": short_id,
                    "Enqueued": _fmt_dt(job.enqueued_at),
                    "Target": _queue_target_label(kind, payload),
                    "Event time": _fmt_any_ts(payload.get("timestamp")),
                    "Force rerun": payload.get("force_rerun", False),
                    "Handler": _func_short_tag(job.func_name),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_lsm_project(name: str, lsm_svc: LSMProjectStateService, redis_url: str) -> None:
    try:
        view = lsm_svc.peek_project_by_parts(name)
    except Exception as e:
        st.error(f"Failed to load project: {e}")
        return

    _state_metrics(view)
    _render_queues(name, "lsm", redis_url)

    if not view.slices:
        st.info("No slices in this project.")
        return

    st.subheader(f"Slices ({len(view.slices)})")
    for sid in sorted(view.slices):
        sl = view.slices[sid]
        label = f"Slice {sid} — {sl.processing_state.value}"
        with st.expander(f"{project_status_icon(sl.processing_state.value)} {label}", expanded=False):
            _state_metrics(sl)

            if not sl.channels:
                st.caption("No channels.")
                continue

            for cid in sorted(sl.channels):
                ch = sl.channels[cid]
                ch_label = f"Channel {cid} — {ch.processing_state.value}"
                st.markdown(f"**{project_status_icon(ch.processing_state.value)} {ch_label}**")

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


def _render_oct_project(name: str, oct_svc: OCTProjectStateService, redis_url: str) -> None:
    try:
        view = oct_svc.peek_project_by_parts(name)
    except Exception as e:
        st.error(f"Failed to load project: {e}")
        return

    _state_metrics(view)
    _render_queues(name, "oct", redis_url)

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
            f"{project_status_icon(sl.processing_state.value)} {label}{extra}",
            expanded=False,
        ):
            _state_metrics(sl)

            if not sl.mosaics:
                st.caption("No mosaics.")
                continue

            for mid in sorted(sl.mosaics):
                mo = sl.mosaics[mid]
                mo_label = f"Mosaic {mid} — {mo.processing_state.value}"
                st.markdown(f"**{project_status_icon(mo.processing_state.value)} {mo_label}**")

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
        st.switch_page("pages/Projects.py")

    st.header(f"{project_type.upper()} project: `{project_name}`")

    settings = hub_settings()
    backend = make_state_backend(settings.redis_url)

    if project_type == "lsm":
        _render_lsm_project(project_name, LSMProjectStateService(backend), settings.redis_url)
    elif project_type == "oct":
        _render_oct_project(project_name, OCTProjectStateService(backend), settings.redis_url)
    else:
        st.error(f"Unknown project type: {project_type}")


main()
