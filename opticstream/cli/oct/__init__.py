from .cli import oct_cli
from . import deploy  # noqa: F401 - register oct deploy commands
from . import serve  # noqa: F401 - register oct serve commands
from . import setup  # noqa: F401 - register oct setup commands
from . import watch  # noqa: F401 - register oct watch commands

__all__ = ["oct_cli", "deploy", "serve", "setup", "watch"]
