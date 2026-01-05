from enum import Enum


class TileSavingType(str, Enum):
    """
    Enumeration of tile saving types.
    """
    COMPLEX = "complex"
    SPECTRAL = "spectral"
