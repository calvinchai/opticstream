from __future__ import annotations

"""
opticstream.cli package

This package implements the Cyclopts-based command-line interface for
opticstream. The root App and subgroups are defined in `root`, while
individual commands are registered in sibling modules (e.g. deploy_cmds,
utils_cmds).
"""

from .root import app
from .init import init_cli
from .oct import oct_cli
from .lsm import lsm_cli  # noqa: F401
from .utils import utils_cli  # noqa: F401
from .process_priority import process_priority_cli  # noqa: F401


def main() -> None:
    """
    Entry point for the `opticstream` console script.

    Importing the command modules here ensures that all commands are
    registered with the shared App instance before the CLI runs.
    """

    app()
