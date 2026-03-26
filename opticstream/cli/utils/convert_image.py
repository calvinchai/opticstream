from __future__ import annotations

from typing import Literal, Optional

from opticstream.data_processing.qc.convert_image import convert_image

from .cli import utils_cli

_OutputFormat = Literal["webp", "jpg", "jpeg", "png"]


@utils_cli.command(name="convert-image")
def convert_image_cmd(
    *,
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
    Convert an input image/volume into a compressed RGB output.

    - For NIfTI (.nii/.nii.gz) and other multi-dimensional arrays, `slice_index`
      selects the slice along axis 0 (default: middle slice).
    - For MATLAB (.mat), `mat_variable` selects which variable to extract.
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
