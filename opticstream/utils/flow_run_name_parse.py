"""Helpers for extracting ident fields from Prefect flow run names."""

from __future__ import annotations

import re
from typing import Any

_TOKEN_RE = re.compile(
    r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>'[^']*'|\"[^\"]*\"|-?\d+)"
)


def parse_flow_run_name_fields(flow_run_name: str) -> dict[str, str | int]:
    """
    Parse known ``key=value`` pairs embedded in ``flow_run.name``.

    Supported value forms:
    - quoted string: ``project_name='111'``
    - integer: ``slice_id=1``

    Extra keys are preserved; callers can filter by required fields.
    """
    parsed: dict[str, str | int] = {}
    for match in _TOKEN_RE.finditer(flow_run_name):
        key = match.group("key")
        raw = match.group("value")
        if raw.startswith(("'", '"')) and raw.endswith(("'", '"')):
            parsed[key] = raw[1:-1]
        else:
            parsed[key] = int(raw)
    return parsed


def missing_required_fields(parsed: dict[str, Any], required_fields: tuple[str, ...]) -> list[str]:
    """Return required fields that are missing in parsed values."""
    return [field for field in required_fields if field not in parsed]
