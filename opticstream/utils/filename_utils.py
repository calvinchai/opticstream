"""
Filename parsing and manipulation utilities.

These are regular Python functions (not Prefect tasks) for filename operations
used across multiple tasks.
"""

import os
import os.path as op
import re
from pathlib import Path
from typing import Tuple


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


# Pattern: Run<run> or Run<run>_<strip_suffix> or Run<run>_C2 or Run<run>_C2_<strip_suffix>
_LSM_RUN_FOLDER_RE = re.compile(r"^Run(\d+)(_C2)?(_(\d+))?$", re.IGNORECASE)


def parse_lsm_run_folder_name(folder_name: str) -> tuple[int, int, int]:
    """
    Parse an LSM run folder name into run_index, strip_index, and channel_index.

    Folder names follow the format:
    - Run<N>           → run_index=N, strip_index=1, channel_index=1
    - Run<N>_<s>       → run_index=N, strip_index=s+1, channel_index=1
    - Run<N>_C2        → run_index=N, strip_index=1, channel_index=2
    - Run<N>_C2_<s>    → run_index=N, strip_index=s+1, channel_index=2

    Parameters
    ----------
    folder_name : str
        Folder name or path (e.g. "Run1", "Run1_2", "Run1_C2", "Run1_C2_3").
        Trailing slashes and parent path are ignored; only the basename is parsed.

    Returns
    -------
    tuple of (int, int, int)
        (run_index, strip_index, channel_index)

    Raises
    ------
    ValueError
        If folder_name does not match the expected pattern.

    Examples
    --------
    >>> parse_lsm_run_folder_name("Run1")
    (1, 1, 1)
    >>> parse_lsm_run_folder_name("Run1_1")
    (1, 2, 1)
    >>> parse_lsm_run_folder_name("Run1_4")
    (1, 5, 1)
    >>> parse_lsm_run_folder_name("Run1_C2")
    (1, 1, 2)
    >>> parse_lsm_run_folder_name("Run1_C2_3")
    (1, 4, 2)
    """
    name = op.basename(folder_name.rstrip(op.sep))
    m = _LSM_RUN_FOLDER_RE.match(name)
    if not m:
        raise ValueError(f"Folder name does not match LSM run pattern 'Run<N>[_C2][_<suffix>]': {folder_name!r}")
    run_index = int(m.group(1))
    channel_index = 2 if m.group(2) else 1
    suffix = m.group(4)
    strip_index = int(suffix) + 1 if suffix is not None else 1
    return (run_index, strip_index, channel_index)

def parse_lsm_strip_index(strip_index: int, channel_index: int, strips_per_slice: int) -> Tuple[int, int, int]:
    """
    Parse an LSM strip index into a slice index, strip index, and channel index.

    Parameters
    ----------
    strip_index : int
        1-based strip index within the entire acquisition.
    channel_index : int
        Channel index (e.g. 1 or 2).
    strips_per_slice : int
        Number of strips acquired per slice.

    Returns
    -------
    Tuple[int, int, int]
        (slice_index, strip_index_within_slice, channel_index)
    """
    slice_index = (strip_index-1) // strips_per_slice + 1
    strip_index_within_slice = (strip_index-1) % strips_per_slice + 1
    return (slice_index, strip_index_within_slice, channel_index)


def parse_lsm_strip_index_from_filename(
    folder_name: str,
    strips_per_slice: int = 1,
) -> Tuple[int, int, int]:
    """
    Parse LSM slice/strip/channel indices from a run folder name.

    This combines `parse_lsm_run_folder_name` and `parse_lsm_strip_index`:

    - First, the folder name is interpreted as an LSM run folder, e.g.:
      - ``Run1``, ``Run1_2``, ``Run1_C2``, ``Run1_C2_3``.
    - Then the resulting strip index is split into a slice index and a strip index
      within that slice using ``strips_per_slice``.

    Parameters
    ----------
    folder_name : str
        Folder name or path; only the basename is parsed.
    strips_per_slice : int, optional
        Number of strips acquired per slice. Defaults to 1.

    Returns
    -------
    Tuple[int, int, int]
        (slice_index, strip_index_within_slice, channel_index)
    """
    run_index, strip_index, channel_index = parse_lsm_run_folder_name(
        os.path.basename(folder_name)
    )
    # Currently, run_index is not used here but is preserved for future extensions.
    return parse_lsm_strip_index(strip_index, channel_index, strips_per_slice)
