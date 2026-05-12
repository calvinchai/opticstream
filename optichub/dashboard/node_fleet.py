"""Shared node registry + gRPC ping status for hub screens."""

from __future__ import annotations

from dataclasses import dataclass

from optichub.config import HubSettings
from optichub.dashboard.hub_ui import fmt_age
from optichub.grpc_client import ping_node
from optichub.redis_client import NodeRecord, get_node


def node_status_icon(status: str) -> str:
    if status == "online":
        return ":material/cloud_done:"
    if status == "degraded":
        return ":material/warning:"
    return ":material/cloud_off:"


@dataclass(frozen=True)
class NodeStatusView:
    """One node's connectivity as shown on Dashboard / Nodes."""

    node_id: str
    status: str
    latency: str
    last_seen: str
    grpc_display: str
    ipv4_display: str
    rec: NodeRecord


def node_status_view(settings: HubSettings, node_id: str, now: float) -> NodeStatusView:
    rec = get_node(
        settings.redis_url,
        node_id,
        online_grace_s=settings.online_grace_s,
        now=now,
    )
    grpc_display = f"{rec.ipv4}:{rec.grpc_port}" if rec.ipv4 else f":{rec.grpc_port}"
    ipv4_display = rec.ipv4 or "—"

    if rec.online:
        rtt = ping_node(rec.ipv4, rec.grpc_port, timeout_ms=settings.grpc_ping_timeout_ms)
        if rtt is not None:
            status, latency = "online", f"{rtt:.1f} ms"
            last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "—"
        else:
            status, latency = "degraded", "ping failed"
            last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "never"
    else:
        status, latency = "offline", "—"
        last_seen = fmt_age(now - rec.last_seen_ts) if rec.last_seen_ts is not None else "never"

    return NodeStatusView(
        node_id=rec.node_id,
        status=status,
        latency=latency,
        last_seen=last_seen,
        grpc_display=grpc_display,
        ipv4_display=ipv4_display,
        rec=rec,
    )


def node_table_row_dict(v: NodeStatusView) -> dict[str, str]:
    return {
        "Node": v.node_id,
        "Status": v.status,
        "IPv4": v.ipv4_display,
        "gRPC": v.grpc_display,
        "Last seen": v.last_seen,
        "Latency": v.latency,
    }
