"""Project name normalization and RQ queue names shared across hub, node, and workers."""

from typing import Literal

ProjectQueueKind = Literal["lsm", "oct"]


def normalize_project_name(project_name: str) -> str:
    """
    Return a stable slug: lowercase with underscores replaced by hyphens.

    Use this for Prefect Variable keys, lock names, Block names, artifact keys,
    and RQ queue names so filesystem-style project names map to one canonical string.
    """
    return project_name.lower().replace("_", "-")


def queue_name_for_project(
    project_name: str,
    kind: ProjectQueueKind,
    *,
    backlog: bool = False,
) -> str:
    name = normalize_project_name(project_name)
    tail = "backlog" if backlog else "realtime"
    return f"{kind}:project:{name}:{tail}"
