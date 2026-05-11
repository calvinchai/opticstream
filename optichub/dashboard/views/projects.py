"""Projects list page: all LSM and OCT projects from state services."""

from __future__ import annotations

import streamlit as st

from opticapi.project_state.state_reader import LSMStateReader, OCTStateReader

from optichub.dashboard.hub_ui import hub_settings


def _status_icon(status: str) -> str:
    if status == "completed":
        return ":material/check_circle:"
    if status == "running":
        return ":material/sync:"
    if status == "failed":
        return ":material/error:"
    return ":material/hourglass_empty:"


def _render_project_table(
    project_type: str,
    names: list[str],
    peek_fn,
) -> None:
    if not names:
        st.info(f"No {project_type} projects found.")
        return

    rows: list[dict[str, str]] = []
    for name in names:
        try:
            view = peek_fn(name)
            status = view.processing_state.value
            created = view.created_at.strftime("%Y-%m-%d %H:%M")
            updated = view.updated_at.strftime("%Y-%m-%d %H:%M")
        except Exception:
            status = "error"
            created = "—"
            updated = "—"

        rows.append(
            {
                "Project": name,
                "Type": project_type.upper(),
                "Status": status,
                "Created": created,
                "Updated": updated,
            }
        )

    st.dataframe(rows, use_container_width=True, hide_index=True)

    for name in names:
        col_name, col_open = st.columns([5, 1])
        try:
            view = peek_fn(name)
            status = view.processing_state.value
        except Exception:
            status = "error"
        col_name.write(f"{_status_icon(status)} `{name}`")
        if col_open.button("Open", key=f"proj_open_{project_type}_{name}", use_container_width=True):
            st.session_state["_hub_open_project"] = name
            st.session_state["_hub_open_project_type"] = project_type
            st.rerun()


def main() -> None:
    st.subheader("Projects")
    st.caption("LSM and OCT project states from Redis.")

    settings = hub_settings()
    lsm_reader = LSMStateReader(settings.redis_url)
    oct_reader = OCTStateReader(settings.redis_url)

    tab_lsm, tab_oct = st.tabs(["LSM", "OCT"])

    with tab_lsm:
        with st.spinner("Loading LSM projects…"):
            try:
                lsm_names = lsm_reader.list_project_names()
            except Exception as e:
                st.error(f"Failed to connect to state service: {e}")
                lsm_names = []
        _render_project_table("lsm", lsm_names, lsm_reader.peek_project_by_parts)

    with tab_oct:
        with st.spinner("Loading OCT projects…"):
            try:
                oct_names = oct_reader.list_project_names()
            except Exception as e:
                st.error(f"Failed to connect to state service: {e}")
                oct_names = []
        _render_project_table("oct", oct_names, oct_reader.peek_project_by_parts)
