#!/usr/bin/env python3
from __future__ import annotations

"""
Convert images (including NIfTI and MATLAB .mat) into compressed RGB images.

Features:
- Optional manual windowing (`window_min`/`window_max`)
- Optional "angle to RGB" mapping (`angle_to_rgb=True`) using an HSV hue map
- Optional slice extraction for 3D+ volumes (`slice_index`, default: middle slice)
- Output saved as compressed RGB (default: WebP)
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional

import numpy as np
from cyclopts import App
from PIL import Image

_OutputFormat = Literal["webp", "jpg", "jpeg", "png"]

app = App(name="convert_image")


@dataclass(frozen=True)
class _LoadedData:
    data: np.ndarray


def _load_nifti(input_path: Path) -> _LoadedData:
    import nibabel as nib

    nifti_img = nib.load(str(input_path))
    data = np.asarray(nifti_img.dataobj)
    return _LoadedData(data=data)


def _load_matlab(input_path: Path, variable: Optional[str]) -> _LoadedData:
    from scipy.io import loadmat

    mat: dict[str, Any] = loadmat(str(input_path))
    keys = [k for k in mat.keys() if not k.startswith("__")]
    if not keys:
        raise ValueError(f"No variables found in MAT file: {input_path}")

    if variable is not None:
        if variable not in mat:
            raise ValueError(
                f"MAT variable {variable!r} not found. Available: {sorted(keys)!r}"
            )
        data = mat[variable]
    else:
        data = mat[keys[0]]

    # MAT arrays sometimes come in as matrices; normalize to ndarray and squeeze.
    data_arr = np.asarray(data)
    return _LoadedData(data=data_arr)


def _load_raster_image(input_path: Path) -> _LoadedData:
    # Prefer imageio for broader format support, but fall back to PIL.
    try:
        import imageio.v3 as iio  # type: ignore

        arr = iio.imread(str(input_path))
    except Exception:
        with Image.open(str(input_path)) as img:
            arr = np.asarray(img)

    return _LoadedData(data=arr)


def _infer_slice(data: np.ndarray, slice_index: Optional[int]) -> np.ndarray:
    if data.ndim <= 2:
        return data
    if data.ndim < 3:
        raise ValueError(f"Unsupported data ndim for slicing: {data.ndim}")

    z = data.shape[0]
    idx = z // 2 if slice_index is None else slice_index
    return data[idx]


def _auto_window(data: np.ndarray) -> tuple[float, float]:
    data_f = data.astype(np.float32, copy=False)
    vmin = float(np.nanmin(data_f))
    vmax = float(np.nanmax(data_f))
    return vmin, vmax


def _apply_window_to_uint8(
    data: np.ndarray,
    *,
    window_min: Optional[float],
    window_max: Optional[float],
) -> np.ndarray:
    # Convert complex -> magnitude for display.
    if np.iscomplexobj(data):
        data = np.abs(data)

    data_f = data.astype(np.float32, copy=False)

    if window_min is None or window_max is None:
        auto_min, auto_max = _auto_window(data_f)
        vmin = auto_min if window_min is None else float(window_min)
        vmax = auto_max if window_max is None else float(window_max)
    else:
        vmin = float(window_min)
        vmax = float(window_max)

    if not np.isfinite(vmin) or not np.isfinite(vmax):
        raise ValueError(f"Invalid window_min/window_max: {(vmin, vmax)}")
    if vmax <= vmin:
        # Degenerate window: return zeros.
        return np.zeros(data_f.shape, dtype=np.uint8)

    clipped = np.clip(data_f, vmin, vmax)
    normalized = (clipped - vmin) / (vmax - vmin)
    return (normalized * 255.0).round().astype(np.uint8)


def _angle_to_rgb_uint8(
    angles: np.ndarray,
    *,
    window_min: Optional[float],
    window_max: Optional[float],
) -> np.ndarray:
    """
    Map angle/radian values to RGB using HSV hue.

    If window_min/window_max are provided, normalize within that range.
    Otherwise, wrap angles into [0, 2*pi) and map directly.
    """
    if np.iscomplexobj(angles):
        angles = np.abs(angles)

    angles_f = angles.astype(np.float32, copy=False)

    import matplotlib.cm as cm

    if window_min is not None and window_max is not None:
        vmin = float(window_min)
        vmax = float(window_max)
        if vmax <= vmin:
            hue = np.zeros_like(angles_f, dtype=np.float32)
        else:
            clipped = np.clip(angles_f, vmin, vmax)
            hue = (clipped - vmin) / (vmax - vmin)
    else:
        two_pi = float(2.0 * np.pi)
        hue = (angles_f % two_pi) / two_pi

    rgba = cm.hsv(np.clip(hue, 0.0, 1.0))
    rgb = (rgba[..., :3] * 255.0).round().astype(np.uint8)
    return rgb


def _ensure_rgb_uint8(img: np.ndarray) -> np.ndarray:
    if img.ndim == 2:
        return np.stack([img, img, img], axis=-1)
    if img.ndim == 3:
        if img.shape[-1] == 1:
            return np.repeat(img, repeats=3, axis=-1).astype(np.uint8, copy=False)
        if img.shape[-1] >= 3:
            rgb = img[..., :3]
            if rgb.dtype == np.uint8:
                return rgb
            # Scale non-uint8 RGB to 0..255.
            rgb_f = rgb.astype(np.float32, copy=False)
            vmin, vmax = float(np.nanmin(rgb_f)), float(np.nanmax(rgb_f))
            if vmax <= vmin:
                return np.zeros(rgb.shape[:2] + (3,), dtype=np.uint8)
            normalized = (np.clip(rgb_f, vmin, vmax) - vmin) / (vmax - vmin)
            return (normalized * 255.0).round().astype(np.uint8)
    raise ValueError(f"Unsupported image shape for RGB conversion: {img.shape}")


def _save_rgb_uint8(
    rgb: np.ndarray,
    output_path: Path,
    *,
    output_format: _OutputFormat,
    quality: int,
) -> None:
    if rgb.dtype != np.uint8:
        raise ValueError(f"Expected uint8 RGB array, got dtype={rgb.dtype}")

    pil_img = Image.fromarray(rgb, mode="RGB")

    fmt: str
    if output_format == "webp":
        fmt = "WEBP"
    elif output_format == "jpg":
        fmt = "JPEG"
    elif output_format == "jpeg":
        fmt = "JPEG"
    elif output_format == "png":
        fmt = "PNG"
    else:
        raise ValueError(f"Unsupported output_format: {output_format}")

    save_kwargs: dict[str, Any] = {}
    if fmt in {"WEBP", "JPEG"}:
        save_kwargs["quality"] = int(quality)
    pil_img.save(str(output_path), format=fmt, **save_kwargs)


def load_data(
    *,
    input_path: Optional[str | Path],
    mat_variable: Optional[str],
) -> np.ndarray:
    """
    Load data from a file into a numpy array.
    """
    if input_path is None:
        raise ValueError("input_path must be provided when loading from file.")

    input_path_obj = Path(input_path)
    # Example: "/path/x.nii.gz" -> ".nii.gz"
    suffix = "".join(input_path_obj.suffixes).lower()

    if suffix in {".nii", ".nii.gz"}:
        return _load_nifti(input_path_obj).data
    if suffix == ".mat" or suffix.endswith(".mat"):
        return _load_matlab(input_path_obj, mat_variable).data

    return _load_raster_image(input_path_obj).data


def _save_rgb_split(
    rgb: np.ndarray,
    output_path: Path,
    *,
    split: int,
    output_format: _OutputFormat,
    quality: int,
) -> None:
    h, w = rgb.shape[:2]
    axis = 0 if h >= w else 1
    total = rgb.shape[axis]
    chunk_size = int(np.ceil(total / split))

    stem = output_path.stem
    suffix = output_path.suffix

    for i in range(split):
        start = i * chunk_size
        end = min(start + chunk_size, total)
        if start >= total:
            break
        part = rgb[start:end, :] if axis == 0 else rgb[:, start:end]
        part_path = output_path.with_name(f"{stem}.part{i + 1}{suffix}")
        _save_rgb_uint8(part, part_path, output_format=output_format, quality=quality)


def convert_image(
    *,
    input: str | Path | None = None,
    data: np.ndarray | None = None,
    output: str | Path | None = None,
    angle_to_rgb: bool = False,
    window_min: float | None = None,
    window_max: float | None = None,
    mat_variable: str | None = None,
    slice_index: int | None = None,
    quality: int = 85,
    output_format: _OutputFormat = "jpg",
    split: int = 1,
) -> np.ndarray:
    """
    Convert input data into compressed RGB suitable for network sharing.

    Exactly one of (`input`, `data`) must be provided.
    If `output` is provided, the RGB result is saved and also returned.
    """
    if (input is None) == (data is None):
        raise ValueError("Provide exactly one of `input` or `data`.")

    if output is None:
        # Python call is allowed to omit output; default to a no-save flow.
        output_path: Optional[Path] = None
    else:
        output_path = Path(output)

    if data is None:
        data = load_data(input_path=input, mat_variable=mat_variable)

    # Extract 2D slice if needed.
    data_slice = _infer_slice(data, slice_index=slice_index)

    if angle_to_rgb:
        if data_slice.ndim != 2:
            raise ValueError(
                "angle_to_rgb expects a single-channel 2D slice (or a volume whose slice is 2D). "
                f"Got shape={data_slice.shape}"
            )
        rgb = _angle_to_rgb_uint8(
            data_slice, window_min=window_min, window_max=window_max
        )
    else:
        # If the input is already RGB/RGBA, preserve it; otherwise window to grayscale.
        if data_slice.ndim == 3 and data_slice.shape[-1] in {3, 4}:
            rgb_in = data_slice[..., :3]
            if rgb_in.dtype != np.uint8:
                # Apply windowing for non-uint8 RGB too, to keep behavior consistent.
                rgb_uint8 = _apply_window_to_uint8(
                    rgb_in, window_min=window_min, window_max=window_max
                )
                rgb = rgb_uint8 if rgb_uint8.ndim == 3 else _ensure_rgb_uint8(rgb_uint8)
            else:
                rgb = rgb_in
        else:
            gray_uint8 = _apply_window_to_uint8(
                data_slice, window_min=window_min, window_max=window_max
            )
            rgb = _ensure_rgb_uint8(gray_uint8)

    if output_path is not None:
        if split > 1:
            _save_rgb_split(
                rgb,
                output_path,
                split=split,
                output_format=output_format,
                quality=quality,
            )
        else:
            _save_rgb_uint8(
                rgb, output_path, output_format=output_format, quality=quality
            )

    return rgb


@app.default
def main(
    input: str,
    output: str,
    angle_to_rgb: bool = False,
    window_min: Optional[float] = None,
    window_max: Optional[float] = None,
    mat_variable: Optional[str] = None,
    slice_index: Optional[int] = None,
    quality: int = 85,
    output_format: _OutputFormat = "webp",
) -> None:
    """
    Convert an input image/volume to a compressed RGB image.
    """
    convert_image(
        input=input,
        data=None,
        output=output,
        angle_to_rgb=angle_to_rgb,
        window_min=window_min,
        window_max=window_max,
        mat_variable=mat_variable,
        slice_index=slice_index,
        quality=quality,
        output_format=output_format,
    )


if __name__ == "__main__":
    app()
