"""Shared formatting and Prefect table publishing helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from prefect.artifacts import create_table_artifact


def pct(part: int, total: int) -> float:
    return (part / total * 100.0) if total else 0.0


def bool_cell_emoji(value: bool) -> str:
    """Compact ✅ / ❌ for Prefect markdown tables (aligned across OCT and LSM)."""
    return "✅" if value else "❌"


def timestamp_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def milestone_icons(overall_pct: float) -> list[str]:
    return [
        "✅" if overall_pct >= t else "⏳"
        for t in (25.0, 50.0, 75.0, 100.0)
    ]


def format_milestone_lines(overall_pct: float) -> str:
    icons = milestone_icons(overall_pct)
    return (
        "Milestones:\n"
        f"- {icons[0]} 25% Complete\n"
        f"- {icons[1]} 50% Complete\n"
        f"- {icons[2]} 75% Complete\n"
        f"- {icons[3]} 100% Complete"
    )


def publish_table_artifact(
    *,
    key: str,
    table: Sequence[dict[str, Any]],
    description: str,
) -> None:
    create_table_artifact(key=key, table=list(table), description=description)
