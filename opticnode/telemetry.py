"""System telemetry: psutil metrics and optional PrimoCache (rxpcc) stats."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field

import psutil

from .config import Settings
from .utils.cli_parsers import PrimoCacheStats, parse_rxpcc_stats
from .utils.network import NetworkPlanes

logger = logging.getLogger(__name__)

_RXPCC_MIN_INTERVAL_S = 10.0


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
    primocache_binary_present: bool = False
    is_primocache_active: bool = False
    primocache: PrimoCacheStats | None = None
    primocache_raw_error: str = ""


class TelemetryEngine:
    """Collects psutil metrics and optionally runs rxpcc for PrimoCache."""

    def __init__(
        self,
        settings: Settings,
        planes: NetworkPlanes | None = None,
    ) -> None:
        self._settings = settings
        self._planes = planes
        self._rxpcc_path: str | None = shutil.which(settings.primocache_exe)
        self._last_net: dict[str, tuple[int, int]] = {}
        self._last_net_ts: float = 0.0
        self._last_rxpcc_ts: float = 0.0
        self._pc_active: bool = False
        self._pc_stats: PrimoCacheStats | None = None
        self._pc_err: str = ""
        if self._rxpcc_path is None:
            logger.warning(
                "PrimoCache CLI %r not found on PATH — PrimoCache telemetry disabled.",
                settings.primocache_exe,
            )

    @property
    def primocache_binary_present(self) -> bool:
        return self._rxpcc_path is not None

    def set_planes(self, planes: NetworkPlanes | None) -> None:
        self._planes = planes

    def _refresh_primocache(self, now: float) -> None:
        if self._rxpcc_path is None:
            self._pc_active = False
            self._pc_stats = None
            self._pc_err = ""
            return
        if self._last_rxpcc_ts > 0 and (now - self._last_rxpcc_ts) < _RXPCC_MIN_INTERVAL_S:
            return
        self._last_rxpcc_ts = now
        try:
            completed = subprocess.run(
                [self._rxpcc_path],
                capture_output=True,
                text=True,
                timeout=15.0,
                check=False,
            )
            out = (completed.stdout or "") + (completed.stderr or "")
            low = out.lower()
            if "no cache found" in low:
                self._pc_active = False
                self._pc_stats = None
                self._pc_err = ""
                return
            self._pc_stats = parse_rxpcc_stats(out)
            self._pc_active = bool(out.strip())
            self._pc_err = ""
        except subprocess.TimeoutExpired:
            self._pc_active = False
            self._pc_stats = None
            self._pc_err = "rxpcc timed out"
        except Exception as exc:
            self._pc_active = False
            self._pc_stats = None
            self._pc_err = str(exc)
            logger.exception("rxpcc execution failed")

    def collect(self) -> TelemetrySnapshot:
        now = time.time()
        cpu_pct = float(psutil.cpu_percent(interval=None))
        vm = psutil.virtual_memory()
        ram_used_pct = float(vm.percent)

        net: list[NetIfaceCounters] = []
        try:
            io = psutil.net_io_counters(pernic=True)
            dt = (now - self._last_net_ts) if self._last_net_ts > 0 else 1.0
            if self._last_net_ts <= 0:
                dt = 1.0
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

        self._refresh_primocache(now)

        return TelemetrySnapshot(
            collected_at_unix=now,
            cpu_pct=cpu_pct,
            ram_used_pct=ram_used_pct,
            net=net,
            primocache_binary_present=self._rxpcc_path is not None,
            is_primocache_active=self._pc_active,
            primocache=self._pc_stats,
            primocache_raw_error=self._pc_err,
        )


def snapshot_to_flat_dict(snap: TelemetrySnapshot) -> dict[str, str]:
    """Flatten snapshot for Redis HSET (string values only)."""
    base: dict[str, str] = {
        "collected_at_unix": str(snap.collected_at_unix),
        "cpu_pct": str(snap.cpu_pct),
        "ram_used_pct": str(snap.ram_used_pct),
        "is_primocache_active": str(snap.is_primocache_active).lower(),
        "primocache_binary_present": str(snap.primocache_binary_present).lower(),
        "net_throughput_json": json.dumps(
            [{"name": n.name, "bytes_sent": n.bytes_sent, "bytes_recv": n.bytes_recv} for n in snap.net]
        ),
    }
    if snap.primocache_raw_error:
        base["primocache_error"] = snap.primocache_raw_error[:2000]
    if snap.primocache is not None:
        d = asdict(snap.primocache)
        raw_labels = d.pop("raw_labels", {})
        base["primocache_stats_json"] = json.dumps(d, default=str)
        if raw_labels:
            base["primocache_raw_labels_json"] = json.dumps(raw_labels)[:8000]
    return base
