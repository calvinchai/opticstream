def normalize_project_name(project_name: str) -> str:
    """
    Normalize the project name so prefect can use it.
    """
    return project_name.lower().replace("_", "-")
    