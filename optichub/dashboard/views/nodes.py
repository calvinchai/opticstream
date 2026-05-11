"""Nodes list page: all registered nodes with status and delete."""

from __future__ import annotations

import time

import streamlit as st

from optichub.config import HubSettings
from optichub.dashboard.hub_ui import fmt_age, hub_settings
from optichub.grpc_client import ping_node
from optichub.redis_client import (
    get_node,
    list_manage_node_ids,
    purge_node,
)


def _status_icon(status: str) -> str:
    if status == "online":
        return ":material/cloud_done:"
    if status == "degraded":
        return ":material/warning:"
    return ":material/cloud_off:"


def _build_node_rows(
    settings: HubSettings,
    all_ids: list[str],
    now: float,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for node_id in all_ids:
        rec = get_node(settings.redis_url, node_id, online_grace_s=settings.online_grace_s, now=now)
        grpc_display = f"{rec.ipv4}:{rec.grpc_port}" if rec.ipv4 else f":{rec.grpc_port}"

        if rec.online:
            rtt = ping_node(rec.ipv4, rec.grpc_port, timeout_ms=settings.grpc_ping_timeout_ms)
            if rtt is not None:
                status = "online"
                latency = f"{rtt:.1f} ms"
            else:
                status = "degraded"
                latency = "ping failed"
            last_seen = (
                fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "—"
            )
        else:
            status = "offline"
            latency = "—"
            last_seen = (
                fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "never"
            )

        rows.append(
            {
                "Node": node_id,
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

    st.subheader("Nodes")
    st.caption("All registered nodes. Click a node to view details, or remove it from the registry.")

    all_ids = list_manage_node_ids(settings.redis_url)

    if not all_ids:
        st.info("No nodes registered in Redis yet.")
        return

    now = time.time()
    rows = _build_node_rows(settings, all_ids, now)
    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.divider()

    for node_id in all_ids:
        col_name, col_open, col_del = st.columns([4, 1, 1])
        rec = get_node(settings.redis_url, node_id, online_grace_s=settings.online_grace_s, now=now)
        status_str = "online" if rec.online else "offline"
        col_name.write(f"{_status_icon(status_str)} `{node_id}`")
        if col_open.button("Open", key=f"nodes_open_{node_id}", use_container_width=True):
            st.session_state["_hub_open_node"] = node_id
            st.rerun()
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
