from __future__ import annotations

from .cli import lsm_cli
from . import serve  # noqa: F401 - register lsm serve commands
from . import watch  # noqa: F401 - register lsm watch commands
from . import setup  # noqa: F401 - register lsm setup commands
from . import delete  # noqa: F401 - register lsm delete commands

__all__ = ["lsm_cli", "serve", "watch", "setup", "delete"]
