"""
Filename parsing and manipulation utilities.

These are regular Python functions (not Prefect tasks) for filename operations
used across multiple tasks.
"""

import os.path as op
import re
from pathlib import Path


def spectral_to_complex_filename(spectral_file: str, complex_path: Path) -> str:
    """
    Convert spectral filename to complex filename.

    Converts `mosaic_001_image_0000_spectral_0000.nii` → `mosaic_001_image_0000_complex.nii`
    Handles spectral→complex replacement and image index normalization to 4 digits.

    Parameters
    ----------
    spectral_file : str
        Path to spectral file
    complex_path : Path
        Path to complex directory

    Returns
    -------
    str
        Full path to complex file
    """
    base_name = op.basename(spectral_file)
    # Replace anything starting with 'spectral' before the extension with 'complex'
    # This corresponds to: regexprep(base_name, 'spectral.*$', 'complex')
    if "spectral" in base_name:
        # Find start of 'spectral', remove everything from there to extension, append 'complex'
        idx = base_name.find("spectral")
        name_no_ext = base_name[:idx] + "complex"
        ext = op.splitext(base_name)[1]
        new_base = name_no_ext + ext
    else:
        new_base = base_name

    # Extract name before extension for further regex
    name_no_ext = op.splitext(new_base)[0]

    # Attempt to match '^mosaic_(\d{3})_image_(\d{3})_complex$'
    match = re.match(r"^mosaic_(\d{3})_image_(\d{3,4})_complex$", name_no_ext)
    if match:
        mosaic_str = match.group(1)
        image_idx = int(match.group(2))
        image_str = f"{image_idx:04d}"
        # Reconstruct name with 4-digit image index and original extension
        name_no_ext = f"mosaic_{mosaic_str}_image_{image_str}_complex"
        new_base = name_no_ext + op.splitext(new_base)[1]

    return str(Path(complex_path) / new_base)


def normalize_image_index(filename: str) -> str:
    """
    Normalize image index in filename to 4-digit padding.

    Handles `mosaic_(\d{3})_image_(\d{3,4})` → `mosaic_(\d{3})_image_(\d{4})`

    Parameters
    ----------
    filename : str
        Filename to normalize (can be full path or just basename)

    Returns
    -------
    str
        Normalized filename with 4-digit image index (basename only)

    Raises
    ------
    ValueError
        If filename doesn't match expected pattern
    """
    name = op.basename(filename)
    name_no_ext = op.splitext(name)[0]
    match = re.match(r"^mosaic_(\d{3})_image_(\d{3,4})(.*)$", name_no_ext)
    if not match:
        raise ValueError(f"Invalid filename pattern: {filename}")
    mosaic_str = match.group(1)
    image_idx = int(match.group(2))
    image_str = f"{image_idx:04d}"
    suffix = match.group(3)  # Everything after image index (e.g., "_complex" or empty)
    # Reconstruct with normalized image index
    normalized_name = f"mosaic_{mosaic_str}_image_{image_str}{suffix}"
    ext = op.splitext(name)[1]
    return normalized_name + ext


def extract_tile_index_from_filename(file_path: str) -> int:
    """
    Extract tile index from filename.

    Extracts tile index from filename (e.g., `mosaic_001_image_0003_...` → `3`)

    Parameters
    ----------
    file_path : str
        Path to file

    Returns
    -------
    int
        Tile index (image number)
    """
    return int(op.basename(file_path).split("_")[3])


def complex_to_complex_filename(complex_file: str, complex_path: Path) -> str:
    """
    Normalize complex filename and construct full path.

    Normalizes image index to 4 digits and ensures filename ends with `_complex.nii`.
    Converts `mosaic_001_image_000_complex.nii` → `mosaic_001_image_0000_complex.nii`

    Parameters
    ----------
    complex_file : str
        Path to complex file
    complex_path : Path
        Path to complex directory

    Returns
    -------
    str
        Full path to normalized complex file
    """
    # Normalize image index to 4 digits
    normalized_name = normalize_image_index(complex_file)

    # Ensure the normalized name ends with _complex.nii
    name_no_ext = op.splitext(normalized_name)[0]
    if not name_no_ext.endswith("_complex"):
        # Extract mosaic and image parts, force _complex suffix
        match = re.match(r"^mosaic_(\d{3})_image_(\d{4})", name_no_ext)
        if match:
            name_no_ext = f"mosaic_{match.group(1)}_image_{match.group(2)}_complex"
            normalized_name = name_no_ext + ".nii"

    return str(Path(complex_path) / normalized_name)


def replace_spectral_with_complex_in_path(file_path: str) -> str:
    """
    Replace 'spectral' with 'complex' in file path.

    Simple string replacement for complex→processed conversion.

    Parameters
    ----------
    file_path : str
        File path containing 'spectral'

    Returns
    -------
    str
        File path with 'spectral' replaced by 'complex'
    """
    return file_path.replace("spectral", "complex")
