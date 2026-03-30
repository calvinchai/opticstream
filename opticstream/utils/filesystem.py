"""General filesystem helpers shared across flows."""

import os
import shutil
from typing import Any, Dict, Optional


def format_bytes(num_bytes: int) -> str:
    """Format a byte count into a human-readable string."""
    n = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:3.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def get_disk_usage_info(path: Optional[str]) -> Dict[str, Any]:
    """
    Return disk-usage information for the filesystem containing ``path``.

    If path is None or does not exist, return zeroed fields.
    """
    if path is None or not os.path.exists(path):
        total = used = free = 0
    else:
        total, used, free = shutil.disk_usage(path)

    used_percent = (used / total * 100.0) if total else 0.0

    return {
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "total_human": format_bytes(total),
        "used_human": format_bytes(used),
        "free_human": format_bytes(free),
        "used_percent": f"{used_percent:.2f}%",
    }
