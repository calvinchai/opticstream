"""Project name normalization for Prefect keys, block names, and artifact keys."""


def normalize_project_name(project_name: str) -> str:
    """
    Return a stable slug: lowercase with underscores replaced by hyphens.

    Use this for Prefect Variable keys, lock names, Block names, and artifact keys
    so filesystem-style project names map to a single canonical string.
    """
    return project_name.lower().replace("_", "-")
