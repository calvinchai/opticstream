from __future__ import annotations

"""
Helpers for locating and managing the external MATLAB package.

This module centralizes:
- Resolution of the MATLAB package path (CLI/env/config-like mechanisms).
- Management of a default \"managed\" clone location for the GitHub repo.
"""

import os
import subprocess
from pathlib import Path
from typing import Optional

from prefect.logging import get_run_logger


# Environment variable that can override the MATLAB package path.
ENV_MATLAB_PACKAGE_PATH = "OPTICSTREAM_MATLAB_PACKAGE_PATH"


def _default_managed_base_dir() -> Path:
    """
    Return the base directory for managed MATLAB packages.

    We keep this under the user's home directory so it is writable in most
    environments and clearly scoped to OpticStream.
    """
    return Path.home() / ".opticstream" / "matlab-packages"


def default_managed_package_path(package_name: str = "matlab-package") -> Path:
    """
    Path where the default managed MATLAB package clone should live.

    Parameters
    ----------
    package_name :
        Folder name for the package within the managed base directory.
    """
    return _default_managed_base_dir() / package_name


def resolve_matlab_package_path(
    explicit_path: Optional[str] = None,
    *,
    package_name: str = "matlab-package",
    allow_missing: bool = False,
) -> Optional[str]:
    """
    Resolve the filesystem path to the MATLAB package.

    Resolution order:
    1. An explicit path provided by the caller.
    2. The OPTICSTREAM_MATLAB_PACKAGE_PATH environment variable.
    3. A default managed clone location under ~/.opticstream/matlab-packages/.

    Parameters
    ----------
    explicit_path :
        Path passed directly from the caller (e.g., CLI flag or config field).
    package_name :
        Name of the folder under the managed base dir for the default clone.
    allow_missing :
        When True, returns None instead of raising when no path is found.

    Returns
    -------
    Optional[str]
        Resolved path to the MATLAB package, or None when allow_missing is True.

    Raises
    ------
    FileNotFoundError
        When a candidate path is configured but does not exist.
    RuntimeError
        When no usable path can be determined and allow_missing is False.
    """
    logger = get_run_logger()

    # 1) Explicit path from caller
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(
                f"Configured MATLAB package path does not exist: {path}"
            )
        return str(path)

    # 2) Environment variable
    env_path = os.getenv(ENV_MATLAB_PACKAGE_PATH)
    if env_path:
        path = Path(env_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(
                f"{ENV_MATLAB_PACKAGE_PATH} points to a non-existent path: {path}"
            )
        logger.info(f"Using MATLAB package from {ENV_MATLAB_PACKAGE_PATH}: {path}")
        return str(path)

    # 3) Default managed clone location
    managed_path = default_managed_package_path(package_name)
    if managed_path.exists():
        logger.info(f"Using managed MATLAB package at: {managed_path}")
        return str(managed_path)

    if allow_missing:
        return None

    raise RuntimeError(
        "Could not determine MATLAB package path. "
        "Tried explicit path, "
        f"{ENV_MATLAB_PACKAGE_PATH}, and managed directory {managed_path}. "
        "Set OPTICSTREAM_MATLAB_PACKAGE_PATH, pass an explicit matlab_script_path, "
        "or clone the MATLAB repo into the managed directory using "
        "`opticstream utils matlab-deps install`."
    )


def ensure_managed_repo(
    repo_url: str,
    *,
    package_name: str = "matlab-package",
    branch_or_tag: Optional[str] = None,
    update: bool = True,
) -> str:
    """
    Ensure that a managed clone of the MATLAB repo exists (and optionally update it).

    This is intended to be called from a CLI command, not from flows. It uses
    the default managed directory (~/.opticstream/matlab-packages/<package_name>).

    Parameters
    ----------
    repo_url :
        Git URL of the MATLAB package repository.
    package_name :
        Folder name for the clone within the managed base directory.
    branch_or_tag :
        Optional branch or tag to check out after cloning/updating.
    update :
        When True and the repo already exists, perform a `git fetch` and
        `git checkout` of the requested ref (if provided).

    Returns
    -------
    str
        Filesystem path to the managed clone.
    """
    logger = get_run_logger()
    base_dir = _default_managed_base_dir()
    base_dir.mkdir(parents=True, exist_ok=True)

    dest = base_dir / package_name

    if not dest.exists():
        logger.info(f"Cloning MATLAB package from {repo_url} into {dest}")
        cmd = ["git", "clone", repo_url, str(dest)]
        subprocess.run(cmd, check=True)
    else:
        logger.info(f"MATLAB package directory already exists at {dest}")

    if branch_or_tag:
        logger.info(f"Checking out ref {branch_or_tag} in {dest}")
        subprocess.run(["git", "-C", str(dest), "fetch", "--all"], check=update)
        subprocess.run(
            ["git", "-C", str(dest), "checkout", branch_or_tag],
            check=True,
        )

    return str(dest)

