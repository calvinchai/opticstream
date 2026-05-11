"""System telemetry: psutil metrics — CPU, RAM, and Network."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

import psutil

from opticnode.app.config import Settings
from opticnode.utils.network import NetworkPlanes

logger = logging.getLogger(__name__)


@dataclass
class NetIfaceCounters:
    name: str
    bytes_sent: int
    bytes_recv: int


@dataclass
class TelemetrySnapshot:
    collected_at_unix: float
    cpu_pct: float
    ram_used_pct: float
    net: list[NetIfaceCounters] = field(default_factory=list)


class TelemetryEngine:
    """Collects CPU, RAM, and network throughput metrics via psutil."""

    def __init__(
        self,
        settings: Settings,
        planes: NetworkPlanes | None = None,
    ) -> None:
        self._settings = settings
        self._planes = planes
        self._last_net: dict[str, tuple[int, int]] = {}
        self._last_net_ts: float = 0.0

    def set_planes(self, planes: NetworkPlanes | None) -> None:
        self._planes = planes

    def collect(self) -> TelemetrySnapshot:
        now = time.time()
        cpu_pct = float(psutil.cpu_percent(interval=None))
        vm = psutil.virtual_memory()
        ram_used_pct = float(vm.percent)

        net: list[NetIfaceCounters] = []
        try:
            io = psutil.net_io_counters(pernic=True)
            dt = (now - self._last_net_ts) if self._last_net_ts > 0 else 1.0
            for name, c in sorted(io.items()):
                prev = self._last_net.get(name)
                if prev is not None:
                    ds = max(0, c.bytes_sent - prev[0])
                    dr = max(0, c.bytes_recv - prev[1])
                else:
                    ds = dr = 0
                rate_sent = int(ds / dt) if dt > 0 else 0
                rate_recv = int(dr / dt) if dt > 0 else 0
                net.append(NetIfaceCounters(name=name, bytes_sent=rate_sent, bytes_recv=rate_recv))
            self._last_net = {n: (io[n].bytes_sent, io[n].bytes_recv) for n in io}
            self._last_net_ts = now
            if self._planes is not None:
                want = set(self._planes.mgmt) | set(self._planes.data)
                if want:
                    net = [n for n in net if n.name in want]
        except Exception:
            logger.exception("net_io_counters failed")

        return TelemetrySnapshot(
            collected_at_unix=now,
            cpu_pct=cpu_pct,
            ram_used_pct=ram_used_pct,
            net=net,
        )


def snapshot_to_flat_dict(snap: TelemetrySnapshot) -> dict[str, str]:
    """Flatten snapshot for Redis HSET (string values only)."""
    return {
        "collected_at_unix": str(snap.collected_at_unix),
        "cpu_pct": str(snap.cpu_pct),
        "ram_used_pct": str(snap.ram_used_pct),
        "net_throughput_json": json.dumps(
            [{"name": n.name, "bytes_sent": n.bytes_sent, "bytes_recv": n.bytes_recv} for n in snap.net]
        ),
    }


__all__ = [
    "TelemetryEngine",
    "TelemetrySnapshot",
    "NetIfaceCounters",
    "snapshot_to_flat_dict",
]

