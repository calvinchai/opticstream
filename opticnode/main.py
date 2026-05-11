"""Thin shim kept for packaging and external references.

The real CLI entrypoint lives in opticnode.cli.main. This module forwards
calls there so that existing `opticnode = "opticnode.main:main"` script
entries and bootstrap_exe.py continue to work without changes.
"""

from __future__ import annotations

from opticnode.cli.main import main

__all__ = ["main"]
