from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from cyclopts import App

from opticstream.cli.root import app


utils_cli = app.command(App(name="utils"))


@dataclass(frozen=True)
class _DepSpec:
    name: str
    dist_name: str
    git_spec: str
    local_path: Path


_DEPS: dict[str, _DepSpec] = {
    "linc-convert": _DepSpec(
        name="linc-convert",
        dist_name="linc-convert",
        git_spec="linc-convert[all] @ git+https://github.com/lincbrain/linc-convert.git@lsm-features",
        local_path=Path("/space/megaera/1/users/kchai/linc-convert"),
    ),
    "nifti-zarr": _DepSpec(
        name="nifti-zarr",
        dist_name="nifti-zarr",
        git_spec="nifti-zarr @ git+https://github.com/calvinchai/nifti-zarr-py.git@port-multizarr",
        local_path=Path("/space/megaera/1/users/kchai/nifti-zarr"),
    ),
    "psoct-toolbox": _DepSpec(
        name="psoct-toolbox",
        dist_name="psoct-toolbox",
        git_spec="psoct-toolbox @ git+https://github.com/calvinchai/psoct-toolbox.git",
        local_path=Path("/space/megaera/1/users/kchai/code/psoct-toolbox"),
    ),
}


def _resolve_deps(selected: Optional[List[str]]) -> list[_DepSpec]:
    if not selected:
        return list(_DEPS.values())

    resolved: list[_DepSpec] = []
    for name in selected:
        if name not in _DEPS:
            valid = ", ".join(sorted(_DEPS))
            raise ValueError(f"Unknown dependency {name!r}. Valid options: {valid}")
        resolved.append(_DEPS[name])
    return resolved
