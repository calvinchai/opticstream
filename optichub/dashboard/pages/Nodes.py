"""Nodes list page: all registered nodes with status and delete."""

from __future__ import annotations

import time

import streamlit as st

from optichub.dashboard.hub_ui import hub_settings
from optichub.dashboard.node_fleet import node_status_icon, node_status_view, node_table_row_dict
from optichub.redis_client import list_manage_node_ids, purge_node


def main() -> None:
    settings = hub_settings()

    st.subheader("Nodes")
    st.caption("All registered nodes. Click a node to view details, or remove it from the registry.")

    all_ids = list_manage_node_ids(settings.redis_url)

    if not all_ids:
        st.info("No nodes registered in Redis yet.")
        return

    now = time.time()
    views = [node_status_view(settings, node_id, now) for node_id in all_ids]
    rows = [node_table_row_dict(v) for v in views]
    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.divider()

    for v in views:
        node_id = v.node_id
        col_name, col_open, col_del = st.columns([4, 1, 1])
        col_name.write(f"{node_status_icon(v.status)} `{node_id}`")
        if col_open.button("Open", key=f"nodes_open_{node_id}", use_container_width=True):
            st.session_state["_hub_open_node"] = node_id
            st.switch_page("pages/Node_detail.py")
        if col_del.button("Delete", key=f"nodes_del_{node_id}", type="secondary", use_container_width=True):
            st.session_state[f"_confirm_delete_{node_id}"] = True

        if st.session_state.get(f"_confirm_delete_{node_id}"):
            st.warning(
                f"Are you sure you want to remove **{node_id}** from the registry? "
                "A running opticnode will re-register on the next heartbeat."
            )
            c_yes, c_no = st.columns(2)
            if c_yes.button("Yes, remove", key=f"nodes_confirm_del_{node_id}", type="primary"):
                purge_node(settings.redis_url, node_id)
                st.session_state.pop(f"_confirm_delete_{node_id}", None)
                st.rerun()
            if c_no.button("Cancel", key=f"nodes_cancel_del_{node_id}"):
                st.session_state.pop(f"_confirm_delete_{node_id}", None)
                st.rerun()


main()
