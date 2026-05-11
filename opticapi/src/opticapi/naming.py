"""Project name normalization and RQ queue names shared across hub, node, and workers."""


def normalize_project_name(project_name: str) -> str:
    """
    Return a stable slug: lowercase with underscores replaced by hyphens.

    Use this for Prefect Variable keys, lock names, Block names, artifact keys,
    and RQ queue names so filesystem-style project names map to one canonical string.
    """
    return project_name.lower().replace("_", "-")


def queue_name_for_project(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}:realtime"


def backlog_queue_name_for_project(project_name: str) -> str:
    return f"{normalize_project_name(project_name)}:backlog"
