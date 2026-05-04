"""Local addressing, dual-plane NIC classification, and small socket helpers."""

from __future__ import annotations

import socket
from contextlib import closing
from dataclasses import dataclass

import psutil


# psutil.net_if_stats().speed is in megabits per second (per psutil docs).
_MGMT_MIN_MBPS = 1000
_DATA_MIN_MBPS = 25000


@dataclass(frozen=True)
class NetworkPlanes:
    """Management (1G-class) vs data (25G-class) interface names and primary IPv4s."""

    mgmt: tuple[str, ...]
    data: tuple[str, ...]
    mgmt_ip: str
    data_ip: str


def get_primary_ipv4(*, connect_host: str = "8.8.8.8", connect_port: int = 80) -> str:
    """Best-effort primary IPv4 by opening a UDP route without sending data."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        try:
            s.connect((connect_host, connect_port))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"


def _ipv4_for_iface(name: str) -> str | None:
    for addr in psutil.net_if_addrs().get(name, []):
        if addr.family != socket.AF_INET:
            continue
        if not addr.address or addr.address.startswith("127."):
            continue
        return addr.address
    return None


def _pick_primary_ip(iface_names: tuple[str, ...], *, preferred: str | None) -> str:
    if preferred and preferred in iface_names:
        ip = _ipv4_for_iface(preferred)
        if ip:
            return ip
    for name in iface_names:
        ip = _ipv4_for_iface(name)
        if ip:
            return ip
    return get_primary_ipv4()


def classify_interfaces(
    *,
    mgmt_iface: str | None = None,
    data_iface: str | None = None,
) -> NetworkPlanes:
    """Classify NICs by link speed: data >= 25 Gbps, mgmt >= 1 Gbps and < 25 Gbps."""
    stats = psutil.net_if_stats()
    mgmt_list: list[str] = []
    data_list: list[str] = []

    for name, st in stats.items():
        if not st.isup:
            continue
        speed = int(st.speed or 0)
        if speed >= _DATA_MIN_MBPS:
            data_list.append(name)
        elif speed >= _MGMT_MIN_MBPS:
            mgmt_list.append(name)

    if mgmt_iface and mgmt_iface not in mgmt_list:
        if mgmt_iface in stats and stats[mgmt_iface].isup:
            mgmt_list = [mgmt_iface, *[n for n in mgmt_list if n != mgmt_iface]]
    if data_iface and data_iface not in data_list:
        if data_iface in stats and stats[data_iface].isup:
            data_list = [data_iface, *[n for n in data_list if n != data_iface]]

    mgmt_t = tuple(mgmt_list)
    data_t = tuple(data_list)

    mgmt_ip = _pick_primary_ip(mgmt_t, preferred=mgmt_iface)
    data_ip = _pick_primary_ip(data_t, preferred=data_iface) if data_t else ""

    return NetworkPlanes(mgmt=mgmt_t, data=data_t, mgmt_ip=mgmt_ip, data_ip=data_ip)


def bind_available_port(host: str = "", preferred: int = 0) -> tuple[socket.socket, int]:
    """Bind to `preferred` if > 0, else an ephemeral port; returns (socket, port)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if preferred > 0:
            s.bind((host or "0.0.0.0", preferred))
        else:
            s.bind((host or "0.0.0.0", 0))
        return s, s.getsockname()[1]
    except Exception:
        s.close()
        raise
