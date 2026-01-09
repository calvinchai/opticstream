"""
Project-level parameter management using Prefect Blocks.

See: https://docs.prefect.io/v3/concepts/blocks
"""

from typing import Any, Callable, Dict, List, Optional

from workflow.config.blocks import PSOCTScanConfig
from workflow.config.constants import TileSavingType


def get_project_config_block(project_name: str) -> Optional[PSOCTScanConfig]:
    """
    Load a project configuration block.

    Block instances are saved with name: "{project_name}-config"

    Parameters
    ----------
    project_name : str
        Project identifier

    Returns
    -------
    ProjectConfig, optional
        Project configuration block, or None if not found
    """
    block_name = f"{project_name.lower().replace('_', '-')}-config"
    try:
        return PSOCTScanConfig.load(block_name)
    except Exception:
        # Block not found or error loading - return None
        return None


def _get_field_default(model_class, field_name: str) -> Any:
    """
    Get default value for a Pydantic model field, supporting both v1 and v2.

    Parameters
    ----------
    model_class
        Pydantic model class (e.g., PSOCTScanConfig)
    field_name : str
        Name of the field to get default for

    Returns
    -------
    Any
        Default value for the field, or None if not found
    """
    # Try Pydantic v2 first (model_fields)
    if hasattr(model_class, "model_fields") and field_name in model_class.model_fields:
        field_info = model_class.model_fields[field_name]
        if hasattr(field_info, "default"):
            return field_info.default
    # Try Pydantic v1 (__fields__)
    if hasattr(model_class, "__fields__") and field_name in model_class.__fields__:
        field_info = model_class.__fields__[field_name]
        if hasattr(field_info, "default"):
            return field_info.default
    # Fallback: return None
    return None


def resolve_config_param(
    payload: Dict[str, Any],
    param_key: str,
    project_config: Optional[PSOCTScanConfig],
    default: Any = None,
    converter: Optional[Callable[[Any], Any]] = None,
    config_attr: Optional[str] = None,
    class_default_attr: Optional[str] = None,
) -> Any:
    """
    Resolve a parameter value using priority: payload → config block → class default.

    Parameters
    ----------
    payload : Dict[str, Any]
        Event payload dictionary
    param_key : str
        Key to look up in payload
    project_config : Optional[PSOCTScanConfig]
        Project configuration block (may be None)
    default : Any, optional
        Fallback default value if not found in payload or config
    converter : Optional[Callable[[Any], Any]], optional
        Function to convert the value (e.g., int, float, bool)
    config_attr : Optional[str], optional
        Attribute name in project_config to use (defaults to param_key).
        If provided, also used as class_default_attr unless class_default_attr is explicitly set.
    class_default_attr : Optional[str], optional
        Attribute name in PSOCTScanConfig class to get default from.
        Defaults to config_attr if provided, otherwise param_key.

    Returns
    -------
    Any
        Resolved parameter value

    Examples
    --------
    >>> # Simple resolution
    >>> value = resolve_config_param(payload, "tile_overlap", config, default=20.0)
    >>> # With type conversion
    >>> value = resolve_config_param(payload, "grid_size_y", config, converter=int)
    >>> # With different config attribute name (automatically used for class default too)
    >>> value = resolve_config_param(
    ...     payload, "archive_format", config,
    ...     config_attr="archive_tile_name_format"
    ... )
    """
    # Determine class_default_attr: use explicit value, or config_attr, or param_key
    if class_default_attr is None:
        class_default_attr = config_attr if config_attr is not None else param_key

    # Priority 1: Check payload
    if param_key in payload:
        value = payload[param_key]
        # Apply converter if provided
        if converter is not None:
            return converter(value)
        return value

    # Priority 2: Check config block
    if project_config is not None:
        attr_name = config_attr if config_attr is not None else param_key
        if hasattr(project_config, attr_name):
            value = getattr(project_config, attr_name)
            # Apply converter if provided
            if converter is not None:
                return converter(value)
            return value

    # Priority 3: Check class default
    class_default = _get_field_default(PSOCTScanConfig, class_default_attr)
    if class_default is not None:
        if converter is not None:
            return converter(class_default)
        return class_default

    # Priority 4: Use provided default
    if converter is not None and default is not None:
        return converter(default)
    return default


def resolve_tile_saving_type(
    payload: Dict[str, Any],
    project_config: Optional[PSOCTScanConfig],
) -> TileSavingType:
    """
    Resolve tile_saving_type with special handling for string-to-enum conversion.

    Parameters
    ----------
    payload : Dict[str, Any]
        Event payload dictionary
    project_config : Optional[PSOCTScanConfig]
        Project configuration block

    Returns
    -------
    TileSavingType
        Resolved tile saving type enum
    """
    value = resolve_config_param(
        payload,
        "tile_saving_type",
        project_config,
        default=TileSavingType.SPECTRAL,
        config_attr="tile_saving_type",
    )

    # Convert string to enum if needed
    if isinstance(value, str):
        return TileSavingType[value.upper()]

    return value


def resolve_config(payload: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """
    Resolve configuration values from payload and project config.

    Priority: payload[key] → project_config.key → omit key

    Does not apply defaults - defaults must be in processing flow signature.

    Parameters
    ----------
    payload : Dict[str, Any]
        Event payload dictionary (must contain "project_name")
    keys : List[str]
        List of configuration keys to resolve

    Returns
    -------
    Dict[str, Any]
        Dictionary with resolved configuration values (only keys that were found)
    """
    project_name = payload["project_name"]
    project_config = get_project_config_block(project_name)

    resolved = {}
    for key in keys:
        if key in payload:
            value = payload[key]
        elif project_config is not None and hasattr(project_config, key):
            value = getattr(project_config, key)
        else:
            continue  # Omit key if not found

        # Special handling for tile_saving_type
        if key == "tile_saving_type" and isinstance(value, str):
            value = TileSavingType[value.upper()]

        resolved[key] = value

    return resolved


def get_grid_size_x(project_name: str, mosaic_id: int) -> int:
    """
    Get grid size x for a given project and mosaic id.

    Parameters
    ----------
    project_name: str
    mosaic_id: int

    Returns
    -------
    int
        Grid size x (number of columns/batches) for the mosaic

    Raises
    ------
    ValueError
        If project config block is not found
    """
    project_config = get_project_config_block(project_name)
    if project_config is None:
        raise ValueError(
            f"Project config block for '{project_name}' not found. "
            f"Cannot determine grid_size_x. Please create config block '{project_name}-config' or provide grid_size_x explicitly."
        )
    return (
        project_config.grid_size_x_normal
        if mosaic_id % 2 == 1
        else project_config.grid_size_x_tilted
    )


def get_mask_threshold(project_name: str, mosaic_id: int) -> float:
    """
    Get mask threshold for a given project and mosaic id.

    Parameters
    ----------
    project_name: str
    mosaic_id: int

    Returns
    -------
    float
    """
    project_config = get_project_config_block(project_name)
    if project_config is None:
        raise ValueError(
            f"Project config block for '{project_name}' not found. "
            f"Cannot determine mask threshold. Please create config block '{project_name}-config' or provide mask_threshold explicitly."
        )
    return (
        project_config.mask_threshold_normal
        if mosaic_id % 2 == 1
        else project_config.mask_threshold_tilted
    )
