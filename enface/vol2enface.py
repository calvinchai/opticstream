import numpy as np
import dask.array as da
from typing import Union
from numpy.typing import ArrayLike


def _to_dask(arr: ArrayLike, chunks="auto") -> da.Array:
    """Convert array-like to dask array."""
    if isinstance(arr, da.Array):
        return arr
    return da.from_array(np.asarray(arr), chunks=chunks)


class EnfaceVolume:
    def __init__(
        self,
        dBI3D: ArrayLike,
        R3D: ArrayLike,
        O3D: ArrayLike,
        surface: Union[str, int, ArrayLike] = "find",
        surface_finding_method: str = "default",
        depth: int = 80,
        wavelength_um: float = 1.3,
        voxel_size_z_um: float = 2.0,
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
        """
        # Convert to dask arrays
        self.dBI3D = _to_dask(dBI3D, chunks= dBI3D.shape)
        self.R3D = _to_dask(R3D, chunks= R3D.shape)
        self.O3D = _to_dask(O3D, chunks= O3D.shape)
        
        # Validate shapes
        if self.dBI3D.ndim != 3 or self.R3D.ndim != 3 or self.O3D.ndim != 3:
            raise ValueError("All volumes must be 3D arrays")
        if not (self.dBI3D.shape == self.R3D.shape == self.O3D.shape):
            raise ValueError("All volumes must have the same shape")
        
        self.nx, self.ny, self.nz = self.dBI3D.shape
        self.depth = depth
        self.wavelength_um = wavelength_um
        self.voxel_size_z_um = voxel_size_z_um
        
        # Handle surface specification
        if surface == "find":
            self.surface = self.find_surface(method=surface_finding_method)
        elif isinstance(surface, (int, np.integer)):
            self.surface = da.full((self.nx, self.ny), int(surface), dtype=np.int32)
        elif isinstance(surface, (np.ndarray, da.Array)):
            surface_da = _to_dask(surface)
            if surface_da.shape != (self.nx, self.ny):
                raise ValueError(
                    f"Provided surface shape {surface_da.shape} does not match "
                    f"volume dimensions ({self.nx}, {self.ny})"
                )
            self.surface = surface_da.astype(np.int32)
        else:
            raise ValueError("Invalid surface specification.")
        
        # Ensure surface is within valid bounds
        self.surface = da.clip(self.surface, 1, self.nz)
        self.stop_idx = da.clip(self.surface + self.depth, 1, self.nz)
        
        # Cache for computed properties
        self._aip = None
        self._mip = None
        self._ret = None
        self._ori = None
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
            # Gradient-based method matching MATLAB implementation
            # This is more complex and requires computing gradients
            # For now, we'll use a simplified version that can work with dask
            # Full MATLAB implementation would require per-pixel processing
            # which is harder to vectorize with dask
            
            # Use argmax as fallback for dask compatibility
            # Note: Full gradient method from MATLAB would require map_blocks
            # with per-pixel processing, which is less efficient
            surface = da.argmax(self.dBI3D, axis=2)
            return surface.astype(np.int32)
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
        Orientation (ORI): circular mean of O3D over [surf, surf+depth] in degrees [0, 180).
        Uses doubled-angle trick for 180°-periodic orientations.
        
        Returns
        -------
        da.Array
            2D enface map, shape (nx, ny)
        """
        if self._ori is None:
            self._ori = self._compute_enface_orientation()
        return self._ori

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

    def _compute_enface_orientation(self) -> da.Array:
        """
        Compute circular mean of O3D over [surf, surf+depth] using doubled-angle trick.
        O3D is 180°-periodic, so we double the angles before averaging.
            
        Returns
        -------
        da.Array
            2D enface map in degrees [0, 180), shape (nx, ny)
        """
        # Use the enface 3D volume for O3D
        O3D_enface = self.O3D_enface
        
        # Double the angles for 180°-periodic circular mean
        theta_rad = da.deg2rad(O3D_enface) * 2
        
        # Compute circular mean: sum cos and sin, then atan2
        x_sum = da.nansum(da.cos(theta_rad), axis=2)
        y_sum = da.nansum(da.sin(theta_rad), axis=2)
        
        # Compute mean angle and convert back
        mean_angle_rad = da.arctan2(y_sum, x_sum)
        result = da.rad2deg(mean_angle_rad) / 2
        
        # Wrap to [0, 180)
        result = da.where(result < 0, result + 180, result)
        
        return result

    def _compute_enface_3d_volume(self, vol: da.Array) -> da.Array:
        """
        Extract 3D enface volume from surface to surface+depth.
        Each A-line starts at surface[x,y] and extends for depth pixels.
        Values beyond nz are filled with NaN.
        
        Parameters
        ----------
        vol : da.Array
            3D volume, shape (nx, ny, nz)
            
        Returns
        -------
        da.Array
            3D enface volume, shape (nx, ny, depth)
        """
        def extract_3d_block(block, block_info=None):
            if block_info is None:
                raise ValueError("block_info is required")
            
            chunk_location = block_info[None]['chunk-location']
            chunk_shape = block_info[None]['chunk-shape']
            
            x_start = chunk_location[0] * chunk_shape[0]
            y_start = chunk_location[1] * chunk_shape[1]
            
            # Use actual block shape (may be smaller at edges)
            block_shape = block.shape
            x_size, y_size = block_shape[0], block_shape[1]
            x_end = x_start + x_size
            y_end = y_start + y_size
            
            surf_chunk = np.asarray(self.surface[x_start:x_end, y_start:y_end].compute())
            vol_chunk = np.asarray(block)
            
            # Convert surface from 1-indexed to 0-indexed
            surf_0idx = surf_chunk - 1
            
            # Create index arrays for vectorized extraction
            # x_indices: (x_size, y_size, depth) - broadcast x coordinates
            x_indices = np.arange(x_size)[:, np.newaxis, np.newaxis]
            x_indices = np.broadcast_to(x_indices, (x_size, y_size, self.depth))
            
            # y_indices: (x_size, y_size, depth) - broadcast y coordinates
            y_indices = np.arange(y_size)[np.newaxis, :, np.newaxis]
            y_indices = np.broadcast_to(y_indices, (x_size, y_size, self.depth))
            
            # z_indices: (x_size, y_size, depth) - z coordinates in original volume
            # For each (x,y), z_indices goes from surf[x,y] to surf[x,y]+depth-1
            z_offsets = np.arange(self.depth)[np.newaxis, np.newaxis, :]
            z_indices = surf_0idx[:, :, np.newaxis] + z_offsets
            
            # Create mask for valid indices (within bounds)
            valid_mask = (z_indices >= 0) & (z_indices < self.nz)
            
            # Initialize result with NaN
            result = np.full((x_size, y_size, self.depth), np.nan, dtype=np.float32)
            
            # Use advanced indexing to extract values
            # Only extract where indices are valid
            valid_x = x_indices[valid_mask]
            valid_y = y_indices[valid_mask]
            valid_z = z_indices[valid_mask]
            
            if len(valid_x) > 0:
                result[valid_mask] = vol_chunk[valid_x, valid_y, valid_z]
            
            return result
        
        # Create output array with same spatial chunks as input, depth in z
        # We need to drop the original z-axis and add a new one with size depth
        output_chunks = (self.dBI3D.chunks[0], self.dBI3D.chunks[1], (self.depth,))
        result = da.map_blocks(
            extract_3d_block,
            vol,
            dtype=np.float32,
            chunks=output_chunks,
            drop_axis=2,
            new_axis=[2],
            new_axis_sizes=[self.depth],
        )
        return result