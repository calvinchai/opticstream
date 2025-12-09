from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple, Union

import nibabel as nib
import numpy as np
import dask.array as da


ArrayLike = Union[np.ndarray, "da.Array"]
BackendName = Literal["numpy", "dask"]

EPS32 = np.finfo(np.float32).eps


@dataclass
class ComplexFields:
    """Holds the complex Jones vectors along with the source image metadata."""
    j1: ArrayLike
    j2: ArrayLike
    header: nib.Nifti1Header
    affine: np.ndarray


@dataclass
class OpticVolumes:
    """Primary 3D volumes produced from the Jones vectors."""
    # intensity
    dBI3D: ArrayLike
    # retardance 
    R3D: ArrayLike
    # orientation 
    O3D: ArrayLike
    

def _get_backend(backend: BackendName):
    if backend == "numpy":
        return np
    if backend == "dask":
        if not _HAVE_DASK:
            raise ImportError("dask is not installed; install dask[array] to use this backend.")
        return da
    raise ValueError(f"Unsupported backend {backend!r}.")


def _infer_backend(array: ArrayLike):
    if isinstance(array, np.ndarray):
        return np
    if _HAVE_DASK and isinstance(array, da.Array):
        return da
    raise TypeError("Array backend could not be inferred; expected numpy or dask array.")


def _as_backend_array(array: ArrayLike, backend: BackendName, *, chunks: Union[str, Tuple[int, ...], None] = "auto"):
    xp = _get_backend(backend)
    if xp is np:
        return np.asarray(array)
    if isinstance(array, da.Array):
        return array.rechunk(chunks)
    return da.from_array(array, chunks=chunks)


def load_complex_fields(
    input_path: Union[str, Path],
    *,
    backend: BackendName = "numpy",
    chunks: Union[str, Tuple[int, ...], None] = "auto",
) -> ComplexFields:
    """
    Load a complex PS-OCT NIfTI where the first dimension stacks the Jones components.

    Parameters
    ----------
    input_path
        Path to a NIfTI volume sized (4 * X) × Y × Z with blocks
        [J1_real, J1_imag, J2_real, J2_imag] along dim-0.
    backend
        "numpy" (default) returns np.ndarray; "dask" returns a dask.array.
    chunks
        Chunk shape for dask arrays (ignored for numpy).
    """

    img = nib.load(str(input_path))
    data = img.get_fdata(dtype=np.float32)
    xp = _get_backend(backend)
    vol = _as_backend_array(data, backend, chunks=chunks)

    if vol.shape[0] % 4 != 0:
        raise ValueError(f"Input first dimension must be a multiple of 4, got {vol.shape[0]}.")

    nx = vol.shape[0] // 4
    # Reshape to (4, nx, ny, nz) for better memory access and cache locality
    vol_reshaped = vol.reshape(4, nx, *vol.shape[1:])
    
    # Extract components using direct indexing (faster than multiple slices)
    j1_real = vol_reshaped[0]
    j1_imag = vol_reshaped[1]
    j2_real = vol_reshaped[2]
    j2_imag = vol_reshaped[3]

    # Construct complex arrays more efficiently
    j1 = j1_real + 1j * j1_imag
    j2 = j2_real + 1j * j2_imag

    return ComplexFields(j1=j1, j2=j2, header=img.header, affine=img.affine)


def compute_optic_volumes(fields: ComplexFields) -> OpticVolumes:
    """Compute dBI, R3D, and O3D volumes from Jones fields."""

    xp = _infer_backend(fields.j1)
    j1, j2 = fields.j1, fields.j2

    intensity = xp.abs(j1) ** 2 + xp.abs(j2) ** 2
    dBI3D = xp.flip(10.0 * xp.log10(xp.maximum(intensity, EPS32)), axis=2)

    denom = xp.maximum(xp.abs(j2), EPS32)
    R3D = xp.flip(xp.arctan(xp.abs(j1) / denom) / math.pi * 180.0, axis=2)

    phi = xp.angle(j1) - xp.angle(j2)
    phi = xp.mod(phi + math.pi, 2 * math.pi) - math.pi
    O3D = xp.flip(phi / (2 * math.pi) * 180.0, axis=2)

    return OpticVolumes(dBI3D=dBI3D, R3D=R3D, O3D=O3D)


def estimate_surface(intensity: ArrayLike, *, smooth: Optional[Tuple[float, float, float]] = None) -> ArrayLike:
    """
    Estimate the surface index per (x, y) by locating the axial maximum intensity.

    Parameters
    ----------
    intensity
        3D array sized [nx, ny, nz].
    smooth
        Optional Gaussian sigma tuple for axial smoothing (requires scipy; omitted here).
    """

    xp = _infer_backend(intensity)
    if smooth is not None:
        raise NotImplementedError("Smoothing is not implemented to keep dependencies minimal.")
    surface = xp.argmax(intensity, axis=2)
    return surface.astype(np.int32)


def _prepare_surface(
    surface: Optional[Union[str, Path, ArrayLike]],
    intensity: ArrayLike,
    *,
    find_surface: bool,
) -> ArrayLike:
    xp = _infer_backend(intensity)
    nx, ny, _ = intensity.shape

    if surface is None:
        if find_surface:
            return estimate_surface(intensity)
        return xp.zeros((nx, ny), dtype=np.int32)

    if isinstance(surface, (str, Path)):
        surf_img = nib.load(str(surface))
        surf = surf_img.get_fdata(dtype=np.float32)
        if surf.shape != (nx, ny):
            raise ValueError(f"Surface size mismatch. Expected {(nx, ny)}, got {surf.shape}.")
        return _as_backend_array(surf, "numpy" if xp is np else "dask").astype(np.int32)

    surf_arr = surface
    surf_xp = _infer_backend(surf_arr)
    if surf_xp is not xp:
        surf_arr = _as_backend_array(np.asarray(surface), "numpy" if xp is np else "dask")
    return surf_arr.astype(np.int32)


def _window_mask(start: ArrayLike, stop: ArrayLike, nz: int, xp):
    start = xp.asarray(start)
    stop = xp.asarray(stop)
    z = xp.arange(nz)[None, None, :]
    mask = (z >= start[..., None]) & (z <= stop[..., None])
    return mask


def _clamp_surface(surf: ArrayLike, nz: int):
    xp = _infer_backend(surf)
    surf = xp.clip(surf, 0, nz - 1)
    return surf


def enface_mean(volume: ArrayLike, surf: ArrayLike, depth: int) -> ArrayLike:
    xp = _infer_backend(volume)
    surf = _clamp_surface(surf, volume.shape[2])
    stop = xp.minimum(surf + depth, volume.shape[2] - 1)
    mask = _window_mask(surf, stop, volume.shape[2], xp)
    masked = volume * mask
    counts = xp.maximum(mask.sum(axis=2, dtype=np.float32), 1)
    return masked.sum(axis=2) / counts


def enface_max(volume: ArrayLike, surf: ArrayLike, depth: int) -> ArrayLike:
    xp = _infer_backend(volume)
    surf = _clamp_surface(surf, volume.shape[2])
    stop = xp.minimum(surf + depth, volume.shape[2] - 1)
    mask = _window_mask(surf, stop, volume.shape[2], xp)
    masked = xp.where(mask, volume, -np.inf)
    return masked.max(axis=2)


def enface_orientation(O3D_deg: ArrayLike, surf: ArrayLike, depth: int) -> ArrayLike:
    xp = _infer_backend(O3D_deg)
    surf = _clamp_surface(surf, O3D_deg.shape[2])
    stop = xp.minimum(surf + depth, O3D_deg.shape[2] - 1)
    mask = _window_mask(surf, stop, O3D_deg.shape[2], xp)

    theta = (2.0 * O3D_deg) * (math.pi / 180.0)
    cos_comp = xp.cos(theta) * mask
    sin_comp = xp.sin(theta) * mask
    sum_cos = cos_comp.sum(axis=2)
    sum_sin = sin_comp.sum(axis=2)
    angle = xp.arctan2(sum_sin, sum_cos) / 2.0
    angle = xp.mod(angle, math.pi)
    return angle * 180.0 / math.pi


def complex_to_processed(
    input_path: Union[str, Path],
    *,
    depth: int = 100,
    backend: BackendName = "numpy",
    chunks: Union[str, Tuple[int, ...], None] = "auto",
    surface: Optional[Union[str, Path, ArrayLike]] = None,
    find_surface: bool = False,
) -> Tuple[OpticVolumes, ArrayLike, ArrayLike]:
    """
    Convenience wrapper replicating the MATLAB Complex2Processed entry point.

    Returns
    -------
    OpticVolumes
        dBI3D, R3D, O3D, and intensity volumes.
    ArrayLike
        Surface indices (z start) per (x, y).
    ArrayLike
        Stop indices per (x, y) computed as surface + depth.
    """

    fields = load_complex_fields(input_path, backend=backend, chunks=chunks)
    volumes = compute_optic_volumes(fields)
    surf = _prepare_surface(surface, volumes.intensity, find_surface=find_surface)
    xp = _infer_backend(surf)
    nz = volumes.dBI3D.shape[2]
    stop = xp.minimum(surf + depth, nz - 1)
    return volumes, surf, stop


__all__ = [
    "ComplexFields",
    "OpticVolumes",
    "complex_to_processed",
    "compute_optic_volumes",
    "enface_mean",
    "enface_max",
    "enface_orientation",
    "estimate_surface",
    "load_complex_fields",
]

