"""Dashboard landing page: fleet status metrics, node cards, summary table."""

from __future__ import annotations

import time

import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_extras.card_selector import card_selector

from optichub.config import HubSettings
from optichub.dashboard.hub_ui import fmt_age, hub_settings
from optichub.grpc_client import ping_node
from optichub.redis_client import get_node, list_manage_node_ids


def _status_icon(status: str) -> str:
    if status == "online":
        return ":material/cloud_done:"
    if status == "degraded":
        return ":material/warning:"
    return ":material/cloud_off:"


def _node_table_rows(
    settings: HubSettings,
    all_ids: list[str],
    now: float,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for listed_id in all_ids:
        rec = get_node(settings.redis_url, listed_id, online_grace_s=settings.online_grace_s, now=now)
        grpc_display = f"{rec.ipv4}:{rec.grpc_port}" if rec.ipv4 else f":{rec.grpc_port}"

        if rec.online:
            rtt = ping_node(rec.ipv4, rec.grpc_port, timeout_ms=settings.grpc_ping_timeout_ms)
            if rtt is not None:
                status, latency = "online", f"{rtt:.1f} ms"
            else:
                status, latency = "degraded", "ping failed"
            last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "—"
        else:
            status, latency = "offline", "—"
            last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "never"

        rows.append(
            {
                "Node": listed_id,
                "Status": status,
                "IPv4": rec.ipv4 or "—",
                "gRPC": grpc_display,
                "Last seen": last_seen,
                "Latency": latency,
            }
        )
    return rows


def main() -> None:
    settings = hub_settings()
    st_autorefresh(interval=int(settings.dashboard_refresh_s * 1000), key="hub_dashboard_refresh")

    st.subheader("Dashboard")

    all_ids = list_manage_node_ids(settings.redis_url)
    now = time.time()

    online = degraded = offline = 0
    card_items: list[dict[str, str]] = []
    card_node_ids: list[str] = []

    for node_id in all_ids:
        rec = get_node(settings.redis_url, node_id, online_grace_s=settings.online_grace_s, now=now)
        grpc_display = f"{rec.ipv4}:{rec.grpc_port}" if rec.ipv4 else f":{rec.grpc_port}"

        if rec.online:
            rtt = ping_node(rec.ipv4, rec.grpc_port, timeout_ms=settings.grpc_ping_timeout_ms)
            if rtt is not None:
                status, latency = "online", f"{rtt:.1f} ms"
                last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "—"
                online += 1
            else:
                status, latency = "degraded", "ping failed"
                last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "never"
                degraded += 1
        else:
            status, latency = "offline", "—"
            last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "never"
            offline += 1

        description = f"{status} · {rec.ipv4 or '—'} · {grpc_display} · last {last_seen} · latency {latency}"
        card_items.append({"icon": _status_icon(status), "title": rec.node_id, "description": description})
        card_node_ids.append(rec.node_id)

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
            st.rerun()
        elif selected is None:
            st.session_state["_hub_dashboard_card_prev_idx"] = None

    if all_ids:
        st.subheader("Fleet summary")
        table_rows = _node_table_rows(settings, all_ids, time.time())
        st.dataframe(table_rows, use_container_width=True, hide_index=True)
