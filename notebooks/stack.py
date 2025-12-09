# %%
import nibabel as nib
import linc_convert
from linc_convert.utils.zarr_config import GeneralConfig

from tifffile import imread
import numpy as np

# List of paths for mosaic ret TIFFs across slices
tif_paths = [
    '/space/zircon/5/users/kchai/I55_slice5/analysis/mosaic_001_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice6/analysis/mosaic_003_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice6/analysis/mosaic_005_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice6/analysis/mosaic_007_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice6/analysis/mosaic_009_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice6/analysis/mosaic_011_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice6/analysis/mosaic_013_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice12/analysis/mosaic_015_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice13/analysis/mosaic_017_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice14/analysis/mosaic_019_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice15/analysis/mosaic_021_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice16/analysis/mosaic_023_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice17/analysis/mosaic_025_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice18/analysis/mosaic_027_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice19/analysis/mosaic_029_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice20/analysis/mosaic_031_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice21/analysis/mosaic_033_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice22/analysis/mosaic_035_ret.tif',
    '/space/zircon/5/users/kchai/I55_slice23/analysis/mosaic_037_ret.tif'
]

# Read and stack TIFFs (assume 2D images, shape=(y,x))
images = [imread(p) for p in tif_paths]

ref_image = nib.load('/autofs/space/nyx_002/users/tgong/Microstructure/data/I55_slab4/dmri.te1/dwi_biascorr_las.nii.gz')

# %%
images = np.stack(images, axis=2)

# %%
images=images.transpose(1,2,0)

voxel_sizes = (0.01, 0.5, 0.01)  # in mm, which is 10, 10, 500 micron


# %%
images= images[::-1,:,::-1]

# %%
header= np.eye(4)
header[0,0] = voxel_sizes[0]
header[1,1] = voxel_sizes[1]
header[2,2] = voxel_sizes[2]
image = nib.Nifti1Image(images, header)

# %%
import numpy as np
import nibabel as nib
from nibabel.affines import apply_affine
def align_centers_affine(moving_nii, fixed_nii):
    """
    Aligns the center of the moving_nii to the center of the fixed_nii 
    by modifying the affine matrix of the moving image.
    
    This function does NOT resample the image data; it only changes the 
    spatial definition (header) of the moving image.

    Parameters:
    -----------
    moving_nii : nibabel.nifti1.Nifti1Image
        The image to be shifted.
    fixed_nii : nibabel.nifti1.Nifti1Image
        The reference image defining the target center.

    Returns:
    --------
    nibabel.nifti1.Nifti1Image
        A new NIfTI image with the same data as moving_nii but a modified affine.
    """
    
    # 1. Calculate the geometric center in Voxel Space for both images
    # The center of a volume with N voxels is typically at (N - 1) / 2
    moving_shape = moving_nii.shape[:3]
    fixed_shape = fixed_nii.shape[:3]
    
    moving_center_vox = np.array([(n - 1) / 2.0 for n in moving_shape])
    fixed_center_vox = np.array([(n - 1) / 2.0 for n in fixed_shape])

    # 2. Convert Voxel Centers to World Coordinates using existing affines
    # We append 1 to the voxel coordinates to perform matrix multiplication
    # Affine dot [x, y, z, 1] -> [x_world, y_world, z_world, 1]
    
    moving_affine = moving_nii.affine
    fixed_affine = fixed_nii.affine
    
    # nib.affines.apply_affine is a helper to do (Affine * coord)[:3]
    moving_center_world = nib.affines.apply_affine(moving_affine, moving_center_vox)
    fixed_center_world = nib.affines.apply_affine(fixed_affine, fixed_center_vox)

    # 3. Calculate the translation vector required
    # Vector = Target - Current
    translation = fixed_center_world - moving_center_world

    # 4. Create the new affine
    # We copy the old affine and simply add the translation to the last column
    new_affine = moving_affine.copy()
    new_affine[:3, 3] += translation
    # 5. Create new NIfTI object
    # We use the original data object (no resampling) and the new affine
    shifted_img = nib.Nifti1Image(moving_nii.dataobj, new_affine)
    # shifted_img.header.set_zooms(moving_nii.header.get_zooms())
    return shifted_img

# %%
shifted_img = align_centers_affine(image, ref_image)

# %%
nib.save(align_centers_affine(image, ref_image), '/scratch/sub-I55_ret.nii')


