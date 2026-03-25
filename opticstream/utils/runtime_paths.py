from __future__ import annotations

import os
from pathlib import Path


def chdir_to_opticstream_install_root() -> Path:
    """
    Change working directory to the installed `opticstream` package directory.

    This makes CLI execution independent of the user's current working directory.
    """
    import opticstream  # local import to avoid import cycles at module import time

    install_root = Path(opticstream.__file__).resolve().parent
    os.chdir(install_root)
    return install_root
