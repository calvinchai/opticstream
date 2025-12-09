import numpy as np
import dask.array as da
from typing import Union, Optional
from numpy.typing import ArrayLike
from scipy.ndimage import gaussian_filter1d, median_filter, convolve1d, gaussian_filter
from scipy import stats
import argparse
import os
from pathlib import Path
import nibabel as nib
from nibabel.nifti1 import Nifti1Image

# Import complex2volume
# Note: This assumes the script is run from the oct-pipe root directory
# or that the package is properly installed
# try:
#     from volume_3d.complex2vol import complex2volume
# except ModuleNotFoundError:
    # Fallback: add parent directory to path
import sys
from pathlib import Path
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))
from volume_3d.complex2vol import complex2volume

def find_surface_gradient(inten: np.ndarray) -> np.ndarray:
    """
    Find surface z-indices using gradient-based method (vectorized).
    
    This implements the MATLAB algorithm from Complex2Processed.m (lines 98-123).
    The algorithm:
    1. Applies 1D Gaussian smoothing along z-axis
    2. Computes gradient using a custom kernel
    3. Finds maximum gradient position for each pixel
    4. Applies median filtering
    
    Parameters
    ----------
    inten : np.ndarray
        3D intensity volume, shape (nx, ny, nz)
        
    Returns
    -------
    np.ndarray
        2D array of surface z-indices, shape (nx, ny)
    """
    nx, ny, nz = inten.shape
    
    # Parameters matching MATLAB code
    w = 5
    w2 = 5
    # Gradient kernel: [-ones(1, w)/w, ones(1, w2)/w2]
    kernel = np.concatenate([-np.ones(w) / w, np.ones(w2) / w2])
    
    # Apply 1D Gaussian smoothing along z-axis (axis=2) for all pixels
    data = gaussian_filter1d(inten, sigma=5, axis=2)
    grad = np.gradient(data, axis=2)
    surf= np.argmax(grad, axis=2).astype(np.int32)
    return surf
    # Compute gradient using convolution along z-axis
    # Use mode='constant' with cval=0 to match MATLAB's conv behavior
    grad = -convolve1d(data, kernel, axis=2, mode='constant', cval=0.0)
    
    # Find valid length for each pixel (where inten > 0.01)
    valid_len = np.sum(inten > 0.01, axis=2)  # Shape: (nx, ny)
    
    # Create mask for pixels with sufficient valid length
    valid_mask = valid_len > (w + w2)  # Shape: (nx, ny)
    
    # The gradient convolution reduces the z-dimension
    # grad has shape (nx, ny, nz - len(kernel) + 1)
    grad_nz = grad.shape[2]
    
    # For each pixel, we need to mask out gradient values beyond valid_len - w
    # Create a mask array: for each pixel (i,j), mask positions >= (valid_len[i,j] - w)
    z_indices = np.arange(grad_nz)  # Indices into grad array
    # Position in original z-axis corresponding to each grad index
    z_positions_in_grad = w + 1 + z_indices  # These are the actual z-positions
    
    # Create mask: for each pixel, only consider grad positions where
    # z_positions_in_grad < valid_len - w + 1
    # This matches MATLAB: positions = (w+1):(valid_len-w+1)
    max_valid_pos = (valid_len - w + 1)[:, :, None]  # Shape: (nx, ny, 1)
    z_pos_mask = z_positions_in_grad[None, None, :] < max_valid_pos  # Shape: (nx, ny, grad_nz)
    
    # Apply mask to gradient: set invalid positions to -inf so they won't be selected
    grad_masked = np.where(z_pos_mask, grad, -np.inf)
    
    # Find argmax along z-axis for each pixel (now only considering valid positions)
    grad_argmax = np.argmax(grad_masked, axis=2)  # Shape: (nx, ny)
    
    # Convert grad_argmax to actual z-positions
    z_positions = z_positions_in_grad[grad_argmax]  # Shape: (nx, ny)
    
    # Set surface values: add 10 to z_position for valid pixels, else 11
    surf = np.where(valid_mask, z_positions, 0).astype(np.float32)
    
    # Median filtering with 3x3 kernel and symmetric padding
    # MATLAB's 'symmetric' mode corresponds to 'reflect' in scipy
    surf = median_filter(surf, size=3, mode='reflect')
    
    return surf.astype(np.int32)


def compute_orientation_circular_mean(O3D_enface: ArrayLike) -> da.Array:
    """
    Compute circular mean of O3D over enface volume using doubled-angle trick.
    O3D is 180°-periodic, so we double the angles before averaging.
    
    Parameters
    ----------
    O3D_enface : ArrayLike
        3D enface volume of O3D, shape (nx, ny, depth)
        
    Returns
    -------
    da.Array
        2D enface map in degrees [0, 180), shape (nx, ny)
    """
    # Double the angles for 180°-periodic circular mean
    theta_rad = da.deg2rad(O3D_enface) * 2
    
    # Compute circular mean: sum cos and sin, then atan2
    x_sum = da.nansum(da.cos(theta_rad), axis=2)
    y_sum = da.nansum(da.sin(theta_rad), axis=2)
    
    # Compute mean angle and convert back
    mean_angle_rad = da.arctan2(y_sum, x_sum)
    result = da.rad2deg(mean_angle_rad) / 2
    
    return result


def compute_orientation_histogram(O3D_enface: ArrayLike, binsize: float = 1.0) -> np.ndarray:
    """
    Compute orientation using histogram-based mode finding (matches orien_enface.m).
    For each pixel, finds the bin with maximum count in histogram of O3D values.
    
    Parameters
    ----------
    O3D_enface : ArrayLike
        3D enface volume of O3D, shape (nx, ny, depth)
    binsize : float, default=1.0
        Bin size in degrees for histogram
        
    Returns
    -------
    np.ndarray
        2D enface map in degrees, shape (nx, ny)
    """
    # Convert to numpy for processing (histogram computation)
    if isinstance(O3D_enface, da.Array):
        O3D = np.asarray(O3D_enface.compute())
    else:
        O3D = np.asarray(O3D_enface)
    nx, ny, nz = O3D.shape
    ori2D = np.zeros((nx, ny), dtype=np.float32)
    
    # Create bin centers from -90 to 90 with binsize step (matching MATLAB)
    vectorO = np.arange(-90, 90 + binsize/1000, binsize)  # Bin centers
    bin_indices = np.round((O3D - vectorO[0]) / binsize).astype(int)
    bin_indices = np.clip(bin_indices, 0, len(vectorO)-1)
    mode_result = stats.mode(bin_indices, axis=2, keepdims=False)
    ori2D = mode_result.mode * binsize + vectorO[0]
    return ori2D


def compute_birefringence_slope_fit(
    R3D_enface: ArrayLike,
    voxel_size_z_um: float,
    wavelength_um: float,
) -> np.ndarray:
    """
    Compute birefringence using simple slope fitting (old method).
    Fits slope of OPD (cycles*lambda) vs depth to estimate Δn.
    
    Parameters
    ----------
    R3D_enface : ArrayLike
        3D enface volume of retardance in degrees, shape (nx, ny, depth)
    surface : ArrayLike
        2D array of surface z-indices, shape (nx, ny)
    stop_idx : ArrayLike
        2D array of stop z-indices, shape (nx, ny)
    voxel_size_z_um : float
        Axial voxel size in micrometers
    wavelength_um : float
        Wavelength in micrometers
    Returns
    -------
    np.ndarray
        2D birefringence map, shape (nx, ny)
    """
    R3D_enface = R3D_enface/360 *wavelength_um
    depth = np.arange(R3D_enface.shape[2]) * voxel_size_z_um
    OPD = R3D_enface.reshape(-1, R3D_enface.shape[2]).T
    birefMap = np.polyfit(depth, OPD, 1)[0,:]
    birefMap = birefMap.reshape(R3D_enface.shape[0], R3D_enface.shape[1])
    return birefMap


def compute_birefringence_unwrap_fit(
    R3D: ArrayLike,
    O3D: ArrayLike,
    voxel_size_z_um: float,
    wavelength_um: float,
) -> np.ndarray:
    """
    Compute birefringence using unwrapped retardance and slope fitting (new method).
    Unwraps retardance using Stokes parameters, then fits slope on multiple segments.
    
    Parameters
    ----------
    R3D : ArrayLike
        3D volume of retardance in degrees, shape (nx, ny, nz)
    O3D : ArrayLike
        3D volume of orientation in degrees, shape (nx, ny, nz)
    surface : ArrayLike
        2D array of surface z-indices, shape (nx, ny)
    stop_idx : ArrayLike
        2D array of stop z-indices, shape (nx, ny)
    voxel_size_z_um : float
        Axial voxel size in micrometers
    wavelength_um : float
        Wavelength in micrometers
        
    Returns
    -------
    np.ndarray
        2D birefringence map, shape (nx, ny)
    """
    nx, ny, nz = R3D.shape
    birefMap = np.zeros((nx, ny), dtype=np.float32)
    
    # Convert to radians
    RET = np.deg2rad(R3D)
    ORI = np.deg2rad(O3D)
    
    # Compute Stokes parameters with Gaussian filtering
    # imgaussfilt3 with [3, 3, 10] corresponds to sigma=[1.5, 1.5, 5] approximately
    Q = gaussian_filter(np.sin(2 * ORI) * np.sin(2 * RET), sigma=(1.5, 1.5, 5))
    U = gaussian_filter(np.cos(2 * ORI) * np.sin(2 * RET), sigma=(1.5, 1.5, 5))
    V = gaussian_filter(np.cos(2 * RET), sigma=(1.5, 1.5, 5))
    
    # Normalize the Stokes vectors
    norms = np.sqrt(Q**2 + U**2 + V**2) + np.finfo(float).eps
    Q = Q / norms
    U = U / norms
    V = V / norms
    roc = np.diff(V, axis=2)
    roc_abs = np.abs(roc)
    roc = roc_abs - roc
    RET = np.cos(2*RET)
    RET[1:] += roc
    RET = np.arccos(RET) / 2
    # V[1:] += roc
    # V = np.arccos(V) / 2
    return compute_birefringence_slope_fit(RET, voxel_size_z_um, wavelength_um)
   
    

class EnfaceVolume:
    def __init__(
        self,
        dBI3D: ArrayLike,
        R3D: ArrayLike,
        O3D: ArrayLike,
        surface: Union[str, int, ArrayLike] = "find",
        surface_finding_method: str = "default",
        skip_depth: int = 0,
        depth: int = 80,
        wavelength_um: float = 1.0,
        voxel_size_z_um: float = 2.0,
        orientation_method: str = "circular_mean",
        orientation_binsize: float = 1.0,
        birefringence_method: str = "slope_fit",
    ):
        """
        Initialize EnfaceVolume for computing enface projections from 3D volumes.
        
        Parameters
        ----------
        dBI3D : ArrayLike
            3D volume of backscatter in dB, shape (X, Y, Z)
        R3D : ArrayLike
            3D volume of retardance in degrees, shape (X, Y, Z)
        O3D : ArrayLike
            3D volume of optic axis orientation in degrees, shape (X, Y, Z)
        surface : str, int, or ArrayLike, default="find"
            Surface specification:
            - "find": automatically detect surface
            - int: constant surface z-index for all pixels
            - ArrayLike: 2D array of surface z-indices, shape (X, Y)
        surface_finding_method : str, default="default"
            Method for surface finding when surface="find":
            - "default": gradient-based method (matches MATLAB)
            - "argmax": simple argmax along z-axis
        depth : int, default=80
            Number of pixels below surface for enface window
        wavelength_um : float, default=1.3
            Wavelength in micrometers (for birefringence calculations)
        voxel_size_z_um : float, default=2.0
            Axial voxel size in micrometers
        orientation_method : str, default="circular_mean"
            Method for computing orientation:
            - "circular_mean": circular mean using doubled-angle trick (default)
            - "histogram": histogram-based mode finding (matches orien_enface.m)
        orientation_binsize : float, default=1.0
            Bin size in degrees for histogram method (only used when orientation_method="histogram")
        birefringence_method : str, default="slope_fit"
            Method for computing birefringence:
            - "slope_fit": simple slope fitting (old method, matches fitBirefringence)
            - "unwrap_fit": unwrap retardance first, then slope fitting (new method, matches unwarp_new_fitting)
        """
        self.dBI3D = dBI3D
        self.R3D = R3D
        self.O3D = O3D

        # Validate shapes
        if self.dBI3D.ndim != 3 or self.R3D.ndim != 3 or self.O3D.ndim != 3:
            raise ValueError("All volumes must be 3D arrays")
        if not (self.dBI3D.shape == self.R3D.shape == self.O3D.shape):
            raise ValueError("All volumes must have the same shape")
        
        self.nx, self.ny, self.nz = self.dBI3D.shape
        self.depth = depth
        self.wavelength_um = wavelength_um
        self.voxel_size_z_um = voxel_size_z_um
        self.orientation_method = orientation_method
        self.orientation_binsize = orientation_binsize
        self.birefringence_method = birefringence_method
        
        # Handle surface specification
        if surface == "find":
            self.surface = self.find_surface(method=surface_finding_method)
        elif isinstance(surface, (int, np.integer)):
            self.surface = da.full((self.nx, self.ny), int(surface), dtype=np.int32)
        elif isinstance(surface, (np.ndarray, da.Array)):
            if surface.shape != (self.nx, self.ny):
                raise ValueError(
                    f"Provided surface shape {surface.shape} does not match "
                    f"volume dimensions ({self.nx}, {self.ny})"
                )
            self.surface = surface.astype(np.int16)
        else:
            raise ValueError("Invalid surface specification.")
        
        # Ensure surface is within valid bounds
        self.surface = np.clip(self.surface+ skip_depth, 0, self.nz)
        self.stop_idx = np.clip(self.surface + self.depth + skip_depth, 0, self.nz)
        
        # Cache for computed properties
        self._aip = None
        self._mip = None
        self._ret = None
        self._ori = None
        self._biref = None
        self._dBI3D_enface = None
        self._R3D_enface = None
        self._O3D_enface = None

    def find_surface(self, method: str = "default") -> da.Array:
        """
        Find surface z-indices for each (x, y) position.
        
        Parameters
        ----------
        method : str
            Method to use: "default" (gradient-based) or "argmax"
            
        Returns
        -------
        da.Array
            2D array of surface z-indices, shape (nx, ny)
        """
        dBI3D = self.dBI3D
        if method == "argmax":
            # Simple maximum intensity projection along depth axis
            surface = da.argmax(self.dBI3D, axis=2)
            return surface.astype(np.int32)
        elif method == "default":
            # Gradient-based method matching MATLAB implementation (vectorized)
            # Convert to numpy if dask array for processing
            if isinstance(dBI3D, da.Array):
                inten = np.asarray(dBI3D.compute())
            else:
                inten = np.asarray(dBI3D)
            
            # Call the separate surface finding function
            surf = find_surface_gradient(inten)
            
            # Convert back to dask array if input was dask
            if isinstance(dBI3D, da.Array):
                return da.from_array(surf, chunks='auto')
            else:
                return surf
        else:
            raise ValueError(f"Unknown surface finding method: {method}")

    @property
    def aip(self) -> da.Array:
        """
        Average Intensity Projection (AIP): mean dBI over [surf, surf+depth].
        
        Returns
        -------
        da.Array
            2D enface map, shape (nx, ny)
        """
        if self._aip is None:
            self._aip = da.nanmean(self.dBI3D_enface, axis=2)
        return self._aip

    @property
    def mip(self) -> da.Array:
        """
        Maximum Intensity Projection (MIP): max dBI over [surf, surf+depth].
        
        Returns
        -------
        da.Array
            2D enface map, shape (nx, ny)
        """
        if self._mip is None:
            self._mip = da.nanmax(self.dBI3D_enface, axis=2)
        return self._mip

    @property
    def ret(self) -> da.Array:
        """
        Retardance (RET): mean R3D over [surf, surf+depth] in degrees.
        
        Returns
        -------
        da.Array
            2D enface map, shape (nx, ny)
        """
        if self._ret is None:
            self._ret = da.nanmean(self.R3D_enface, axis=2)
        return self._ret

    @property
    def ori(self) -> da.Array:
        """
        Orientation (ORI): computed from O3D over [surf, surf+depth].
        
        Method depends on orientation_method:
        - "circular_mean": circular mean using doubled-angle trick (default)
        - "histogram": histogram-based mode finding (matches orien_enface.m)
        
        Returns
        -------
        da.Array
            2D enface map, shape (nx, ny)
        """
        if self._ori is None:
            O3D_enface = self.O3D_enface
            
            if self.orientation_method == "histogram":
                self._ori = compute_orientation_histogram(O3D_enface, self.orientation_binsize)
                # Convert to dask array if input was dask
                if isinstance(O3D_enface, da.Array):
                    self._ori = da.from_array(self._ori, chunks='auto')
            elif self.orientation_method == "circular_mean":
                self._ori = compute_orientation_circular_mean(O3D_enface)
            else:
                raise ValueError(f"Unknown orientation method: {self.orientation_method}")
        return self._ori

    @property
    def biref(self) -> da.Array:
        """
        Birefringence (BIREF): computed from R3D (and optionally O3D) over [surf, surf+depth].
        
        Method depends on birefringence_method:
        - "slope_fit": simple slope fitting of OPD vs depth (old method, matches fitBirefringence)
        - "unwrap_fit": unwrap retardance using Stokes parameters, then slope fitting (new method, matches unwarp_new_fitting)
        
        Returns
        -------
        da.Array
            2D birefringence map, shape (nx, ny)
        """
        if self._biref is None:
            if self.birefringence_method == "slope_fit":
                self._biref = compute_birefringence_slope_fit(
                    self.R3D_enface,
                    self.voxel_size_z_um,
                    self.wavelength_um,
                )
            elif self.birefringence_method == "unwrap_fit":
                self._biref = compute_birefringence_unwrap_fit(
                    self.R3D,
                    self.O3D,
                    self.voxel_size_z_um,
                    self.wavelength_um,
                )
            else:
                raise ValueError(f"Unknown birefringence method: {self.birefringence_method}")
            # Convert to dask array if input was dask
            if isinstance(self.R3D, da.Array):
                self._biref = da.from_array(self._biref, chunks='auto')
        return self._biref

    @property
    def dBI3D_enface(self) -> da.Array:
        """
        3D enface volume of dBI starting from surface.
        Each A-line starts at surface[x,y] and extends for depth pixels.
        Values beyond nz are filled with NaN.
        
        Returns
        -------
        da.Array
            3D volume, shape (nx, ny, depth)
        """
        if self._dBI3D_enface is None:
            self._dBI3D_enface = self._compute_enface_3d_volume(self.dBI3D)
        return self._dBI3D_enface

    @property
    def R3D_enface(self) -> da.Array:
        """
        3D enface volume of R3D starting from surface.
        Each A-line starts at surface[x,y] and extends for depth pixels.
        Values beyond nz are filled with NaN.
        
        Returns
        -------
        da.Array
            3D volume, shape (nx, ny, depth)
        """
        if self._R3D_enface is None:
            self._R3D_enface = self._compute_enface_3d_volume(self.R3D)
        return self._R3D_enface

    @property
    def O3D_enface(self) -> da.Array:
        """
        3D enface volume of O3D starting from surface.
        Each A-line starts at surface[x,y] and extends for depth pixels.
        Values beyond nz are filled with NaN.
        
        Returns
        -------
        da.Array
            3D volume, shape (nx, ny, depth)
        """
        if self._O3D_enface is None:
            self._O3D_enface = self._compute_enface_3d_volume(self.O3D)
        return self._O3D_enface


    def _compute_enface_3d_volume(self, vol: da.Array) -> da.Array:
        """
        Extract 3D enface volume from surface to self.stop_idx.
        """
        # Padding in z so we can index safely up to (stop_idx-1)
        pad_width = [(0, 0), (0, 0), (0, self.depth)]
        vol_padded = np.pad(vol, pad_width, mode="constant", constant_values=np.nan)

        # Compute slice indices: for each (x,y), gather from surface[x,y] to stop_idx[x,y]-1
        # This creates a 3rd dimension (of length `depth`) for each A-line
        start = self.surface
        # after padding, surface and stop_idx do not need to be clipped (already done)
        # so, for each (x, y), make gather indices: [start, start+1, ..., start+depth-1]
        gather_indices = (
            start[..., None] + da.arange(self.depth)[None, None, :]
        )

        # Broadcast indices for later use with take_along_axis
        # Input to np.take_along_axis must match the dimension
        result = np.take_along_axis(
            vol_padded,
            gather_indices,
            axis=2,
        )

        return result


def process_enface_volumes(
    complex_path: Optional[Union[str, Path]] = None,
    dbi3d_path: Optional[Union[str, Path]] = None,
    r3d_path: Optional[Union[str, Path]] = None,
    o3d_path: Optional[Union[str, Path]] = None,
    flip_orientation: bool = False,
    offset: float = 100.0,
    surface: Union[str, int, ArrayLike] = "find",
    surface_finding_method: str = "default",
    skip_depth: int = 0,
    depth: int = 80,
    wavelength_um: float = 1.0,
    voxel_size_z_um: float = 2.0,
    orientation_method: str = "circular_mean",
    orientation_binsize: float = 1.0,
    birefringence_method: str = "slope_fit",
    output_aip: Optional[Union[str, Path]] = None,
    output_mip: Optional[Union[str, Path]] = None,
    output_ret: Optional[Union[str, Path]] = None,
    output_ori: Optional[Union[str, Path]] = None,
    output_biref: Optional[Union[str, Path]] = None,
) -> dict:
    """
    Wrapper function to process enface volumes from nifti files.
    
    Parameters
    ----------
    complex_path : str or Path, optional
        Path to nifti file containing complex data. If provided, will be converted
        to dBI3D, R3D, and O3D volumes. Mutually exclusive with dbi3d_path/r3d_path/o3d_path.
    dbi3d_path : str or Path, optional
        Path to nifti file containing dBI3D volume (backscatter in dB).
        Required if complex_path is not provided.
    r3d_path : str or Path, optional
        Path to nifti file containing R3D volume (retardance in degrees).
        Required if complex_path is not provided.
    o3d_path : str or Path, optional
        Path to nifti file containing O3D volume (optic axis orientation in degrees).
        Required if complex_path is not provided.
    flip_orientation : bool, default=False
        Whether to flip orientation when converting from complex data.
        Only used when complex_path is provided.
    offset : float, default=100.0
        Offset in degrees for orientation calculation when converting from complex data.
        Only used when complex_path is provided.
    surface : str, int, or ArrayLike, default="find"
        Surface specification (see EnfaceVolume.__init__)
    surface_finding_method : str, default="default"
        Method for surface finding when surface="find"
    skip_depth : int, default=0
        Number of pixels to skip from surface
    depth : int, default=80
        Number of pixels below surface for enface window
    wavelength_um : float, default=1.0
        Wavelength in micrometers
    voxel_size_z_um : float, default=2.0
        Axial voxel size in micrometers
    orientation_method : str, default="circular_mean"
        Method for computing orientation
    orientation_binsize : float, default=1.0
        Bin size in degrees for histogram method
    birefringence_method : str, default="slope_fit"
        Method for computing birefringence
    output_aip : str or Path, optional
        Output path for AIP (Average Intensity Projection) nifti file
    output_mip : str or Path, optional
        Output path for MIP (Maximum Intensity Projection) nifti file
    output_ret : str or Path, optional
        Output path for RET (Retardance) nifti file
    output_ori : str or Path, optional
        Output path for ORI (Orientation) nifti file
    output_biref : str or Path, optional
        Output path for BIREF (Birefringence) nifti file
        
    Returns
    -------
    dict
        Dictionary containing computed modalities as numpy arrays.
        Keys: 'aip', 'mip', 'ret', 'ori', 'biref' (only includes computed modalities)
    """
    # Validate input: either complex_path or all three volume paths must be provided
    if complex_path is not None:
        if dbi3d_path is not None or r3d_path is not None or o3d_path is not None:
            raise ValueError("Cannot specify both complex_path and individual volume paths. "
                           "Use either complex_path or dbi3d_path/r3d_path/o3d_path.")
        # Load complex data and convert to volumes
        print(f"Loading complex data from {complex_path}")
        complex_img = nib.load(str(complex_path))
        
        # Convert to dask array for complex2volume
        complex_da = da.from_array(complex_img.dataobj, chunks='auto')
        
        print("Converting complex data to volumes...")
        dBI3D, R3D, O3D = complex2volume(complex_da, flip_orientation=flip_orientation, offset=offset)
        
        # Convert back to numpy arrays
        dBI3D = np.asarray(dBI3D.compute())
        R3D = np.asarray(R3D.compute())
        O3D = np.asarray(O3D.compute())
        nib.save(Nifti1Image(dBI3D, complex_img.affine, complex_img.header), "/scratch/dBI3D.nii.gz")
        # Get affine and header from complex file for saving
        reference_img = complex_img
        reference_affine = reference_img.affine
        reference_header = reference_img.header
    else:
        # Load individual volume files
        if dbi3d_path is None or r3d_path is None or o3d_path is None:
            raise ValueError("Either complex_path must be provided, or all three volume paths "
                           "(dbi3d_path, r3d_path, o3d_path) must be provided.")
        
        print(f"Loading dBI3D from {dbi3d_path}")
        dbi3d_img = nib.load(str(dbi3d_path))
        dBI3D = np.asarray(dbi3d_img.dataobj)
        
        print(f"Loading R3D from {r3d_path}")
        r3d_img = nib.load(str(r3d_path))
        R3D = np.asarray(r3d_img.dataobj)
        
        print(f"Loading O3D from {o3d_path}")
        o3d_img = nib.load(str(o3d_path))
        O3D = np.asarray(o3d_img.dataobj)
        
        # Get affine and header from first volume for saving
        reference_img = dbi3d_img
        reference_affine = reference_img.affine
        reference_header = reference_img.header
    
    # Create EnfaceVolume instance
    print("Creating EnfaceVolume instance...")
    enface_vol = EnfaceVolume(
        dBI3D=dBI3D,
        R3D=R3D,
        O3D=O3D,
        surface=surface,
        surface_finding_method=surface_finding_method,
        skip_depth=skip_depth,
        depth=depth,
        wavelength_um=wavelength_um,
        voxel_size_z_um=voxel_size_z_um,
        orientation_method=orientation_method,
        orientation_binsize=orientation_binsize,
        birefringence_method=birefringence_method,
    )
    
    results = {}
    
    # Compute and save AIP if requested
    if output_aip is not None:
        print("Computing AIP...")
        aip = enface_vol.aip.compute() if isinstance(enface_vol.aip, da.Array) else enface_vol.aip
        results['aip'] = aip
        print(f"Saving AIP to {output_aip}")
        aip_img = Nifti1Image(aip, reference_affine, reference_header)
        nib.save(aip_img, str(output_aip))
    
    # Compute and save MIP if requested
    if output_mip is not None:
        print("Computing MIP...")
        mip = enface_vol.mip.compute() if isinstance(enface_vol.mip, da.Array) else enface_vol.mip
        results['mip'] = mip
        print(f"Saving MIP to {output_mip}")
        mip_img = Nifti1Image(mip, reference_affine, reference_header)
        nib.save(mip_img, str(output_mip))
    
    # Compute and save RET if requested
    if output_ret is not None:
        print("Computing RET...")
        ret = enface_vol.ret.compute() if isinstance(enface_vol.ret, da.Array) else enface_vol.ret
        results['ret'] = ret
        print(f"Saving RET to {output_ret}")
        ret_img = Nifti1Image(ret, reference_affine, reference_header)
        nib.save(ret_img, str(output_ret))
    
    # Compute and save ORI if requested
    if output_ori is not None:
        print("Computing ORI...")
        ori = enface_vol.ori.compute() if isinstance(enface_vol.ori, da.Array) else enface_vol.ori
        results['ori'] = ori
        print(f"Saving ORI to {output_ori}")
        ori_img = Nifti1Image(ori, reference_affine, reference_header)
        nib.save(ori_img, str(output_ori))
    
    # Compute and save BIREF if requested
    if output_biref is not None:
        print("Computing BIREF...")
        biref = enface_vol.biref.compute() if isinstance(enface_vol.biref, da.Array) else enface_vol.biref
        results['biref'] = biref
        print(f"Saving BIREF to {output_biref}")
        biref_img = Nifti1Image(biref, reference_affine, reference_header)
        nib.save(biref_img, str(output_biref))
    
    print("Processing complete!")
    return results


def main():
    """Command-line interface for processing enface volumes."""
    parser = argparse.ArgumentParser(
        description="Process 3D OCT volumes to generate enface projections",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Input arguments: either complex_path or all three volume paths
    parser.add_argument(
        "--complex",
        type=str,
        default=None,
        help="Path to nifti file containing complex data (will be converted to dBI3D, R3D, O3D). "
             "If provided, --dbi3d, --r3d, and --o3d are not needed."
    )
    parser.add_argument(
        "--dbi3d",
        type=str,
        default=None,
        help="Path to nifti file containing dBI3D volume (backscatter in dB). "
             "Required if --complex is not provided."
    )
    parser.add_argument(
        "--r3d",
        type=str,
        default=None,
        help="Path to nifti file containing R3D volume (retardance in degrees). "
             "Required if --complex is not provided."
    )
    parser.add_argument(
        "--o3d",
        type=str,
        default=None,
        help="Path to nifti file containing O3D volume (optic axis orientation in degrees). "
             "Required if --complex is not provided."
    )
    
    # Complex conversion parameters
    parser.add_argument(
        "--flip-orientation",
        action="store_true",
        default=False,
        help="Flip orientation when converting from complex data (only used with --complex)"
    )
    parser.add_argument(
        "--offset",
        type=float,
        default=100.0,
        help="Offset in degrees for orientation calculation when converting from complex data (only used with --complex)"
    )
    
    # Surface parameters
    parser.add_argument(
        "--surface",
        type=str,
        default="find",
        help="Surface specification: 'find' (auto-detect), integer (constant z-index), or path to nifti file with 2D surface map"
    )
    parser.add_argument(
        "--surface-finding-method",
        type=str,
        default="default",
        choices=["default", "argmax"],
        help="Method for surface finding when surface='find'"
    )
    parser.add_argument(
        "--skip-depth",
        type=int,
        default=0,
        help="Number of pixels to skip from surface"
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=80,
        help="Number of pixels below surface for enface window"
    )
    
    # Physical parameters
    parser.add_argument(
        "--wavelength-um",
        type=float,
        default=1.0,
        help="Wavelength in micrometers"
    )
    parser.add_argument(
        "--voxel-size-z-um",
        type=float,
        default=2.0,
        help="Axial voxel size in micrometers"
    )
    
    # Orientation parameters
    parser.add_argument(
        "--orientation-method",
        type=str,
        default="circular_mean",
        choices=["circular_mean", "histogram"],
        help="Method for computing orientation"
    )
    parser.add_argument(
        "--orientation-binsize",
        type=float,
        default=1.0,
        help="Bin size in degrees for histogram method (only used when orientation-method='histogram')"
    )
    
    # Birefringence parameters
    parser.add_argument(
        "--birefringence-method",
        type=str,
        default="slope_fit",
        choices=["slope_fit", "unwrap_fit"],
        help="Method for computing birefringence"
    )
    
    # Output paths for each modality
    parser.add_argument(
        "--output-aip",
        type=str,
        default=None,
        help="Output path for AIP (Average Intensity Projection) nifti file"
    )
    parser.add_argument(
        "--output-mip",
        type=str,
        default=None,
        help="Output path for MIP (Maximum Intensity Projection) nifti file"
    )
    parser.add_argument(
        "--output-ret",
        type=str,
        default=None,
        help="Output path for RET (Retardance) nifti file"
    )
    parser.add_argument(
        "--output-ori",
        type=str,
        default=None,
        help="Output path for ORI (Orientation) nifti file"
    )
    parser.add_argument(
        "--output-biref",
        type=str,
        default=None,
        help="Output path for BIREF (Birefringence) nifti file"
    )
    
    args = parser.parse_args()
    
    # Validate input: either complex_path or all three volume paths must be provided
    if args.complex is not None:
        if args.dbi3d is not None or args.r3d is not None or args.o3d is not None:
            parser.error("Cannot specify both --complex and individual volume paths (--dbi3d, --r3d, --o3d). "
                        "Use either --complex or all three volume paths.")
    else:
        if args.dbi3d is None or args.r3d is None or args.o3d is None:
            parser.error("Either --complex must be provided, or all three volume paths "
                        "(--dbi3d, --r3d, --o3d) must be provided.")
    
    # Handle surface parameter: if it's a path to a file, load it
    surface = args.surface
    if surface != "find" and not str(surface).isdigit():
        # Check if it's a file path
        surface_path = Path(surface)
        if surface_path.exists():
            print(f"Loading surface from {surface_path}")
            surface_img = nib.load(str(surface_path))
            surface = np.asarray(surface_img.dataobj)
        else:
            # Try to parse as integer
            try:
                surface = int(surface)
            except ValueError:
                raise ValueError(f"Surface must be 'find', an integer, or a path to a nifti file. Got: {surface}")
    elif str(surface).isdigit():
        surface = int(surface)
    
    # Process volumes
    process_enface_volumes(
        complex_path=args.complex,
        dbi3d_path=args.dbi3d,
        r3d_path=args.r3d,
        o3d_path=args.o3d,
        flip_orientation=args.flip_orientation,
        offset=args.offset,
        surface=surface,
        surface_finding_method=args.surface_finding_method,
        skip_depth=args.skip_depth,
        depth=args.depth,
        wavelength_um=args.wavelength_um,
        voxel_size_z_um=args.voxel_size_z_um,
        orientation_method=args.orientation_method,
        orientation_binsize=args.orientation_binsize,
        birefringence_method=args.birefringence_method,
        output_aip=args.output_aip,
        output_mip=args.output_mip,
        output_ret=args.output_ret,
        output_ori=args.output_ori,
        output_biref=args.output_biref,
    )


if __name__ == "__main__":
    main()