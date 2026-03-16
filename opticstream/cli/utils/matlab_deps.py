from __future__ import annotations

"""
CLI helpers for managing the external MATLAB package dependency.

Provides:
- `opticstream utils matlab-deps install` to clone/update the MATLAB GitHub repo
  into the managed directory used by the runtime.
"""

from typing import Optional

from cyclopts import Parameter

from opticstream.cli.utils.cli import utils_cli
from opticstream.utils.matlab_package import ensure_managed_repo, default_managed_package_path


@utils_cli.command
def matlab_deps_install(
    repo_url: str,
    *,
    package_name: str = "matlab-package",
    ref: Optional[str] = Parameter(
        name="--ref",
        help="Optional branch or tag to check out after cloning.",
    ),
    no_update: bool = Parameter(
        name="--no-update",
        help=(
            "Do not fetch/update an existing clone; only clone when missing. "
            "By default, an existing clone is fetched and the requested ref "
            "is checked out when provided."
        ),
        default=False,
    ),
) -> None:
    """
    Install or update the external MATLAB package used by OpticStream.

    This command clones the given GitHub repository into the managed directory
    (by default: ~/.opticstream/matlab-packages/<package_name>) so that
    workflows can locate it automatically.

    Examples
    --------
    - Clone for the first time:
        opticstream utils matlab-deps install https://github.com/USER/MATLAB-REPO.git

    - Clone into a custom package folder:
        opticstream utils matlab-deps install https://github.com/USER/MATLAB-REPO.git --package-name=psoct-matlab

    - Pin to a specific tag or branch:
        opticstream utils matlab-deps install https://github.com/USER/MATLAB-REPO.git --ref=v1.0.0
    """
    path = ensure_managed_repo(
        repo_url=repo_url,
        package_name=package_name,
        branch_or_tag=ref,
        update=not no_update,
    )

    managed_default = default_managed_package_path(package_name)
    print(f"MATLAB package installed at: {path}")
    print(
        "At runtime, OpticStream will look for the package via:\n"
        "  1) An explicit matlab_script_path argument (when provided)\n"
        "  2) The OPTICSTREAM_MATLAB_PACKAGE_PATH environment variable\n"
        f"  3) The managed directory: {managed_default}"
    )

