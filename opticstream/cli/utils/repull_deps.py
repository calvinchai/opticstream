from __future__ import annotations

import subprocess
from typing import Annotated, List, Optional

from cyclopts import Parameter

from .cli import _resolve_deps, utils_cli


@utils_cli.command
def repull_deps(
    *,
    dependency: Annotated[
        Optional[List[str]],
        Parameter(name=["-d", "--dependency"]),
    ] = None,
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry-run", "-n"]),
    ] = False,
) -> None:
    """
    Repull one or more external Git-based dependencies using uv.

    By default this repulls all three core dependencies (linc-convert,
    nifti-zarr, psoct-toolbox) from their configured Git URLs, ignoring
    local editable paths configured via [tool.uv.sources]. You can
    target a subset with -d/--dependency and preview commands with
    --dry-run.

    Examples
    --------
    Repull all three dependencies:

        opticstream utils repull-deps

    Repull only linc-convert and psoct-toolbox:

        opticstream utils repull-deps -d linc-convert -d psoct-toolbox

    Show the uv commands without executing them:

        opticstream utils repull-deps --dry-run
    """
    deps = _resolve_deps(dependency)

    for spec in deps:
        cmd = [
            "uv",
            "pip",
            "install",
            "--force-reinstall",
            "--no-deps",
            spec.git_spec,
        ]
        pretty_cmd = " ".join(cmd)
        if dry_run:
            print(f"[DRY] {spec.name}: {pretty_cmd}")
            continue

        print(f"[RUN] {spec.name}: {pretty_cmd}")
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            print(f"[ERROR] Failed to repull {spec.name}: {exc}")

