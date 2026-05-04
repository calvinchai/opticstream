"""Parse CLI tool output (e.g. rxpcc / PrimoCache) with regex."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PrimoCacheStats:
    """Structured PrimoCache / rxpcc performance profile (best-effort from text)."""

    l1_hit_rate_current: float | None = None
    l1_hit_rate_cumulative: float | None = None
    l2_hit_rate_current: float | None = None
    l2_hit_rate_cumulative: float | None = None
    cache_used_mb: float | None = None
    cache_free_mb: float | None = None
    write_buffer_deferred_blocks: int | None = None
    write_buffer_urgent_writes: int | None = None
    io_trimmed_blocks: int | None = None
    io_read_bytes: int | None = None
    io_written_bytes: int | None = None
    raw_labels: dict[str, str] = field(default_factory=dict)


def _first_float(pattern: str, text: str, flags: int = re.IGNORECASE) -> float | None:
    m = re.search(pattern, text, flags)
    if not m:
        return None
    try:
        return float(m.group(1))
    except (ValueError, IndexError):
        return None


def _first_int(pattern: str, text: str, flags: int = re.IGNORECASE) -> int | None:
    m = re.search(pattern, text, flags)
    if not m:
        return None
    try:
        return int(float(m.group(1)))
    except (ValueError, IndexError):
        return None


def _bytes_from_match(s: str) -> int | None:
    s = s.strip().upper().replace(",", "")
    m = re.match(r"^([\d.]+)\s*([KMGT]?B?)$", s)
    if not m:
        try:
            return int(float(s))
        except ValueError:
            return None
    val = float(m.group(1))
    unit = m.group(2) or ""
    mult = 1
    if unit.startswith("K"):
        mult = 1024
    elif unit.startswith("M"):
        mult = 1024**2
    elif unit.startswith("G"):
        mult = 1024**3
    elif unit.startswith("T"):
        mult = 1024**4
    return int(val * mult)


def parse_rxpcc_stats(text: str) -> PrimoCacheStats:
    """Extract PrimoCache metrics from rxpcc stdout or log text (locale/layout tolerant)."""
    if not text.strip():
        return PrimoCacheStats()

    t = text
    labels: dict[str, str] = {}

    # Hit rates: allow % or ratio
    l1_cur = _first_float(
        r"L1[^\n%]*?hit[^\n%]*?(?:current|now)?[^\d]*([\d.]+)\s*%",
        t,
    ) or _first_float(r"L1[^\n]*Current[^\d]*([\d.]+)", t)
    l1_cum = _first_float(r"L1[^\n%]*?(?:cumulative|total)[^\d]*([\d.]+)\s*%", t) or _first_float(
        r"L1[^\n]*Cumulative[^\d]*([\d.]+)", t
    )
    l2_cur = _first_float(
        r"L2[^\n%]*?hit[^\n%]*?(?:current|now)?[^\d]*([\d.]+)\s*%",
        t,
    ) or _first_float(r"L2[^\n]*Current[^\d]*([\d.]+)", t)
    l2_cum = _first_float(r"L2[^\n%]*?(?:cumulative|total)[^\d]*([\d.]+)\s*%", t) or _first_float(
        r"L2[^\n]*Cumulative[^\d]*([\d.]+)", t
    )

    # Occupancy: MB or labeled Free/Used
    used_mb = _first_float(
        r"(?:cache\s*)?occup(?:ancy|ied)[^\d]*used[^\d]*([\d.]+)\s*MB",
        t,
    ) or _first_float(r"Used[^\d]*([\d.]+)\s*MB", t)
    free_mb = _first_float(
        r"(?:cache\s*)?occup(?:ancy|ied)[^\d]*free[^\d]*([\d.]+)\s*MB",
        t,
    ) or _first_float(r"Free[^\d]*([\d.]+)\s*MB", t)

    deferred = _first_int(
        r"(?:deferred|deferr?ed)\s*blocks?[^\d]*([\d,]+)",
        t,
    ) or _first_int(r"Deferred[^\d]*([\d,]+)", t)
    urgent = _first_int(r"urgent\s*writes?[^\d]*([\d,]+)", t) or _first_int(
        r"Urgent[^\d]*([\d,]+)", t
    )

    trimmed = _first_int(r"trim(?:med)?\s*blocks?[^\d]*([\d,]+)", t) or _first_int(
        r"Trimmed[^\d]*([\d,]+)", t
    )

    read_b = _first_int(r"total\s*read[^\d]*([\d,]+)\s*(?:bytes|B)?", t, re.IGNORECASE)
    if read_b is None:
        m = re.search(r"(?:read|bytes\s*read)[^\d]*([\d.,]+\s*[KMGT]?B?)", t, re.IGNORECASE)
        if m:
            read_b = _bytes_from_match(m.group(1))

    written_b = _first_int(r"total\s*writt?en[^\d]*([\d,]+)\s*(?:bytes|B)?", t, re.IGNORECASE)
    if written_b is None:
        m = re.search(r"(?:written|bytes\s*written)[^\d]*([\d.,]+\s*[KMGT]?B?)", t, re.IGNORECASE)
        if m:
            written_b = _bytes_from_match(m.group(1))

    # Capture labeled "Key: value" lines for hub debugging
    for line in t.splitlines():
        if ":" in line and len(line) < 200:
            key, _, rest = line.partition(":")
            key = key.strip()
            if key and len(key) < 80:
                labels[key] = rest.strip()[:500]

    return PrimoCacheStats(
        l1_hit_rate_current=l1_cur,
        l1_hit_rate_cumulative=l1_cum,
        l2_hit_rate_current=l2_cur,
        l2_hit_rate_cumulative=l2_cum,
        cache_used_mb=used_mb,
        cache_free_mb=free_mb,
        write_buffer_deferred_blocks=deferred,
        write_buffer_urgent_writes=urgent,
        io_trimmed_blocks=trimmed,
        io_read_bytes=read_b,
        io_written_bytes=written_b,
        raw_labels=labels,
    )


def parse_rxpcc_memory_blocks(text: str) -> list[tuple[str, float]]:
    """Backward-compatible: labeled MB rows from rxpcc output."""
    rx = re.compile(
        r"^(?P<label>[\w\s.-]+?)\s+(?P<value>\d+(?:\.\d+)?)\s*MB",
        re.MULTILINE | re.IGNORECASE,
    )
    return [(m.group("label").strip(), float(m.group("value"))) for m in rx.finditer(text)]
