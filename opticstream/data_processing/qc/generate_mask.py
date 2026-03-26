#!/usr/bin/env python3
"""
Generate binary mask from input image or nifti file.

This script creates a binary mask by thresholding the input image.
Values below the threshold are set to background (0), values above are set to
foreground (1).
"""

from pathlib import Path
from typing import Optional, Tuple, Union

import imageio
import nibabel as nib
import numpy as np
from PIL import Image
from cyclopts import App
from scipy import ndimage

app = App(name="generate_mask")


def load_image(
    input_path: Union[str, Path],
) -> Tuple[np.ndarray, Optional[nib.Nifti1Image], Optional[np.ndarray]]:
    """
    Load image from file, supporting both nifti and regular image formats.

    Parameters
    ----------
    input_path : str
        Path to input file

    Returns
    -------
    data : np.ndarray
        Image data as numpy array
    nifti_img : Optional[nib.Nifti1Image]
        Nifti image object if input was nifti, None otherwise
    affine : Optional[np.ndarray]
        Affine transformation matrix if input was nifti, None otherwise
    """
    input_path_obj = Path(input_path)
    suffix = input_path_obj.suffix.lower()
    if suffix in [".nii", ".gz", ".nii.gz"]:
        # Load nifti file
        nifti_img = nib.load(str(input_path_obj))
        data = np.asarray(nifti_img.dataobj)
        affine = nifti_img.affine
        return data, nifti_img, affine
    else:
        # Load regular image file
        try:
            # Try using imageio first (supports many formats)
            data = imageio.imread(str(input_path_obj))
        except Exception:
            # Fall back to PIL
            img = Image.open(str(input_path_obj))
            data = np.asarray(img)

        # Convert to grayscale if needed
        if data.ndim == 3:
            # If RGB/RGBA, convert to grayscale using luminance formula
            if data.shape[2] == 3:  # RGB
                data = np.dot(data[..., :3], [0.299, 0.587, 0.114])
            elif data.shape[2] == 4:  # RGBA
                data = np.dot(data[..., :3], [0.299, 0.587, 0.114])
            else:
                # Take first channel
                data = data[:, :, 0]

        return data, None, None


def save_mask(
    mask: np.ndarray,
    output_path: Union[str, Path],
    nifti_img: Optional[nib.Nifti1Image] = None,
    affine: Optional[np.ndarray] = None,
):
    """
    Save mask to file, format determined by output file extension.

    Parameters
    ----------
    mask : np.ndarray
        Binary mask array
    output_path : str
        Output file path
    nifti_img : Optional[nib.Nifti1Image]
        Original nifti image object (for saving nifti)
    affine : Optional[np.ndarray]
        Affine transformation matrix (for saving nifti)
    """
    output_path_obj = Path(output_path)
    suffix = output_path_obj.suffix.lower()

    # Ensure mask is uint8 with values 0 and 1
    mask_uint8 = (mask.astype(np.uint8) * 255).astype(np.uint8)

    if suffix in [".nii", ".nii.gz", ".gz"]:
        # Save as nifti
        if nifti_img is not None and affine is not None:
            # Use original nifti header and affine
            mask_img = nib.Nifti1Image(mask_uint8, affine, nifti_img.header)
        elif affine is not None:
            mask_img = nib.Nifti1Image(mask_uint8, affine)
        else:
            # Create default affine
            mask_img = nib.Nifti1Image(mask_uint8, np.eye(4))
        nib.save(mask_img, str(output_path_obj))
    elif suffix in [".tif", ".tiff"]:
        # Save as TIFF
        imageio.imwrite(str(output_path_obj), mask_uint8)
    else:
        # Save as regular image (PNG, JPEG, etc.)
        mask_image = Image.fromarray(mask_uint8, mode="L")
        mask_image.save(str(output_path_obj))


def ensure_single_component(mask: np.ndarray, keep_largest: bool = True) -> np.ndarray:
    """
    Ensure mask has only one connected component for background and object.

    Parameters
    ----------
    mask : np.ndarray
        Binary mask
    keep_largest : bool
        If True, keep the largest component. If False, keep the component
        that contains the center of the image.

    Returns
    -------
    mask : np.ndarray
        Mask with only one connected component
    """
    # Label connected components
    labeled_mask, num_features = ndimage.label(mask)

    if num_features <= 1:
        # Already has one or zero components
        return mask

    if keep_largest:
        # Find the largest component
        component_sizes = ndimage.sum(mask, labeled_mask, range(1, num_features + 1))
        largest_component = np.argmax(component_sizes) + 1
        result = (labeled_mask == largest_component).astype(np.uint8)
    else:
        # Keep the component that contains the center
        center = tuple(s // 2 for s in mask.shape)
        center_label = labeled_mask[center]
        if center_label == 0:
            # Center is background, find largest background component
            labeled_bg, num_bg = ndimage.label(~mask.astype(bool))
            if num_bg > 1:
                bg_sizes = ndimage.sum(
                    ~mask.astype(bool), labeled_bg, range(1, num_bg + 1)
                )
                largest_bg = np.argmax(bg_sizes) + 1
                # Keep everything except the largest background component
                result = mask.copy()
                result[labeled_bg == largest_bg] = 1
            else:
                result = mask
        else:
            result = (labeled_mask == center_label).astype(np.uint8)

    # Also ensure background has only one component
    labeled_bg, num_bg = ndimage.label(~result.astype(bool))
    if num_bg > 1:
        # Find the largest background component (usually the outer one)
        bg_sizes = ndimage.sum(~result.astype(bool), labeled_bg, range(1, num_bg + 1))
        largest_bg = np.argmax(bg_sizes) + 1
        # Set all other background components to foreground
        result[labeled_bg != largest_bg] = 1
        result[labeled_bg == largest_bg] = 0

    return result.astype(np.uint8)


@app.default
def main(
    input: Union[str, Path],
    output: Union[str, Path],
    threshold: float,
    gaussian: bool = True,
    gaussian_sigma: float = 1.0,
    single_component: bool = True,
):
    """
    Generate binary mask from input image or nifti file.

    Parameters
    ----------
    input : str
        Path to input file (nifti or image format)
    output : str
        Path to output mask file (format determined by extension)
    threshold : float
        Threshold value. Values below this will be set to background (0),
        values above will be set to foreground (1).
    gaussian : bool
        Apply Gaussian filter before thresholding (default: True)
    gaussian_sigma : float
        Standard deviation for Gaussian filter (default: 1.0)
    single_component : bool
        Ensure mask has only one connected component for background/object.
        This prevents dark areas within the object from being classified as background.
        (default: False)
    """
    # Load input image
    input_path = Path(input)
    output_path = Path(output)
    print(f"Loading input from {input_path}...")
    data, nifti_img, affine = load_image(input_path)
    print(
        f"  Shape: {data.shape}, dtype: {data.dtype}, range: [{data.min():.2f}, {data.max():.2f}]"
    )

    # Apply Gaussian filter if requested
    if gaussian:
        print(f"Applying Gaussian filter (sigma={gaussian_sigma})...")
        data = ndimage.gaussian_filter(data.astype(float), sigma=gaussian_sigma)
        print(f"  Filtered range: [{data.min():.2f}, {data.max():.2f}]")

    # Create binary mask based on threshold
    print(f"Creating binary mask with threshold={threshold}...")
    mask = (data > threshold).astype(np.uint8)
    print(
        f"  Foreground pixels: {np.sum(mask)}, Background pixels: {np.sum(~mask.astype(bool))}"
    )

    # Ensure single component if requested
    if single_component:
        print("Ensuring single connected component...")
        mask = ensure_single_component(mask, keep_largest=True)
        print(
            f"  After single component: Foreground pixels: {np.sum(mask)}, Background pixels: {np.sum(~mask.astype(bool))}"
        )

    # Save mask
    print(f"Saving mask to {output_path}...")
    save_mask(mask, output_path, nifti_img, affine)
    print("Done!")


if __name__ == "__main__":
    app()
