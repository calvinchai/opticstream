from __future__ import annotations

import importlib.metadata as im
from pathlib import Path
from typing import Annotated, List, Optional

from cyclopts import Parameter

from .cli import _resolve_deps, utils_cli


@utils_cli.command
def inspect_deps(
    *,
    dependency: Annotated[
        Optional[List[str]],
        Parameter(name=["-d", "--dependency"]),
    ] = None,
) -> None:
    """
    Inspect versions and installation sources for key external dependencies.

    For each selected dependency, prints the installed version (if any),
    on-disk package path, whether it appears to be coming from a local
    editable path configured via [tool.uv.sources], and the configured
    Git spec from pyproject.toml.

    Examples
    --------
    Inspect all three dependencies:

        opticstream utils inspect-deps

    Inspect only nifti-zarr:

        opticstream utils inspect-deps -d nifti-zarr
    """
    deps = _resolve_deps(dependency)

    for spec in deps:
        print(f"=== {spec.name} ===")
        version: Optional[str]
        pkg_path: Optional[Path] = None
        source: str

        try:
            version = im.version(spec.dist_name)
            dist = im.distribution(spec.dist_name)
            files = list(dist.files or [])
            candidate: Optional[Path] = None
            for f in files:
                if f.name == "__init__.py":
                    candidate = Path(dist.locate_file(f)).resolve()
                    break
            if candidate is None and files:
                candidate = Path(dist.locate_file(files[0])).resolve()
            pkg_path = candidate.parent if candidate and candidate.is_file() else candidate
        except im.PackageNotFoundError:
            version = None

        if version is None:
            source = "NOT INSTALLED"
        else:
            if pkg_path is not None:
                local_root = spec.local_path.resolve()
                try:
                    if pkg_path.is_relative_to(local_root):
                        source = f"local editable (under {local_root})"
                    else:
                        source = f"installed (under {pkg_path})"
                except TypeError:
                    source = f"installed (under {pkg_path})"
            else:
                source = "installed (path unknown)"

        print(f"Name: {spec.name}")
        print(f"Distribution: {spec.dist_name}")
        print(f"Installed version: {version if version is not None else 'NOT INSTALLED'}")
        print(f"Source: {source}")
        print(f"Configured Git spec: {spec.git_spec}")
        print()

