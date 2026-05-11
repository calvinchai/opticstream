"""Projects list page: all LSM and OCT projects from state services."""

from __future__ import annotations

import streamlit as st

from opticstream.state.lsm_state_service import LSM_STATE_SERVICE
from opticstream.state.oct_state_service import OCT_STATE_SERVICE


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

    tab_lsm, tab_oct = st.tabs(["LSM", "OCT"])

    with tab_lsm:
        with st.spinner("Loading LSM projects…"):
            try:
                lsm_names = LSM_STATE_SERVICE.list_project_names()
            except Exception as e:
                st.error(f"Failed to connect to state service: {e}")
                lsm_names = []
        _render_project_table("lsm", lsm_names, LSM_STATE_SERVICE.peek_project_by_parts)

    with tab_oct:
        with st.spinner("Loading OCT projects…"):
            try:
                oct_names = OCT_STATE_SERVICE.list_project_names()
            except Exception as e:
                st.error(f"Failed to connect to state service: {e}")
                oct_names = []
        _render_project_table("oct", oct_names, OCT_STATE_SERVICE.peek_project_by_parts)
