"""Dashboard landing page: fleet status metrics, node cards, summary table."""

from __future__ import annotations

import time

import streamlit as st
from streamlit_extras.card_selector import card_selector

from optichub.dashboard.hub_ui import hub_settings
from optichub.dashboard.node_fleet import node_status_icon, node_status_view, node_table_row_dict
from optichub.redis_client import list_manage_node_ids


def main() -> None:
    settings = hub_settings()

    st.subheader("Dashboard")

    all_ids = list_manage_node_ids(settings.redis_url)
    now = time.time()

    online = degraded = offline = 0
    card_items: list[dict[str, str]] = []
    card_node_ids: list[str] = []

    for node_id in all_ids:
        v = node_status_view(settings, node_id, now)
        if v.status == "online":
            online += 1
        elif v.status == "degraded":
            degraded += 1
        else:
            offline += 1

        description = (
            f"{v.status} · last seen {v.last_seen}"
        )
        card_items.append({"icon": node_status_icon(v.status), "title": v.rec.node_id, "description": description})
        card_node_ids.append(v.rec.node_id)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total nodes", len(all_ids))
    m2.metric("Online", online)
    m3.metric("Degraded", degraded)
    m4.metric("Offline", offline)

    if not card_items:
        st.info("No nodes registered in Redis yet.")
    else:
        st.caption("Click a node card to open its detail page.")
        selected = card_selector(card_items, key="hub_node_cards")
        prev_idx = st.session_state.get("_hub_dashboard_card_prev_idx")
        if selected is not None and selected != prev_idx:
            st.session_state["_hub_dashboard_card_prev_idx"] = selected
            st.session_state["_hub_open_node"] = card_node_ids[selected]
            st.switch_page("pages/Node_detail.py")
        elif selected is None:
            st.session_state["_hub_dashboard_card_prev_idx"] = None

    if all_ids:
        st.subheader("Fleet summary")
        table_now = time.time()
        table_rows = [node_table_row_dict(node_status_view(settings, nid, table_now)) for nid in all_ids]
        st.dataframe(table_rows, use_container_width=True, hide_index=True)


main()
