"""
Python implementation of s2c.m - Spectral to Complex conversion.

This module provides:
- s2c_array: Array-based processing function
- s2c_file: File-based wrapper
- CLI support via main
"""
from __future__ import annotations

'/autofs/cluster/octdata2/users/Hui/tools/dg_utils/spectralprocess/dispComp/mineraloil_LSM03/dispersion_compensation_LSM03_mineraloil_20240829/LSM03_mineral_oil_placecorrectionmeanall2.dat'
'/mnt/sas/I55_testrestart_slice5_20251120/mosaic_001_image_110_spectral_0110.nii'
'/local_mount/space/zircon/5/users/kchai/I55_1120/mosaic_001_image_100_processed_cropped.nii'

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import nibabel as nib
import numpy as np
from scipy import interpolate


def buffer2jones(original_buffer: np.ndarray, padding_factor: int, auto_corr_peak_cut: int) -> np.ndarray:
    """
    Convert interpolated buffer to Jones vector via FFT.
    
    Parameters
    ----------
    original_buffer : np.ndarray
        Input buffer, shape (depth, aline)
    padding_factor : int
        Padding factor (typically 1)
    auto_corr_peak_cut : int
        Number of samples to cut from autocorrelation peak
        
    Returns
    -------
    np.ndarray
        Jones vector, shape (depth, aline) - complex
    """
    aline_length = original_buffer.shape[0] // (2 * padding_factor)
    jones = np.fft.fft(original_buffer, axis=0)
    jones = jones[:aline_length, :]
    jones = jones[auto_corr_peak_cut:, :]
    return jones


def zero_pad_buffer(original_buffer: np.ndarray, padding_factor: int) -> np.ndarray:
    """
    Zero-pad buffer using FFT-based method.
    
    Parameters
    ----------
    original_buffer : np.ndarray
        Input buffer, shape (depth, aline)
    padding_factor : int
        Padding factor
        
    Returns
    -------
    np.ndarray
        Zero-padded buffer, shape (depth * padding_factor, aline)
    """
    aline_length = original_buffer.shape[0]
    number_alines = original_buffer.shape[1]
    mid_length = aline_length // 2 + 1
    
    transformed_buffer = np.fft.fft(original_buffer, axis=0)
    transformed_buffer[mid_length - 1, :] = transformed_buffer[mid_length - 1, :] / 2
    
    padded_transformed_buffer = np.zeros((padding_factor * aline_length, number_alines), dtype=complex)
    padded_transformed_buffer[:mid_length, :] = transformed_buffer[:mid_length, :]
    padded_transformed_buffer[-(mid_length - 1):, :] = transformed_buffer[-(mid_length - 1):, :]
    
    zero_padded_buffer = np.real(np.fft.ifft(padded_transformed_buffer, axis=0)) * padding_factor
    return zero_padded_buffer


def interpolationwave(
    parameters: Tuple[int, int, int, int, int, int],
    calibration_data: Optional[dict] = None
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute interpolated wavelengths for left and right channels.
    
    Parameters
    ----------
    parameters : tuple
        (PaddingFactor, PaddingLength, OriginalLineLength1, Start1, OriginalLineLength2, Start2)
    calibration_data : dict, optional
        Dictionary with keys 'mu' (position arrays) and 'wave_prime' (wavelength samples).
        If None, uses default calibration values.
        
    Returns
    -------
    wavelengths_l : np.ndarray
        Left channel wavelengths in meters
    wavelengths_r : np.ndarray
        Right channel wavelengths in meters
    interpolated_wavelengths : np.ndarray
        Interpolated wavelength array
    """
    padding_factor, padding_length, original_line_length1, start1, original_line_length2, start2 = parameters
    
    # Default calibration data (can be overridden)
    if calibration_data is None:
        # These are example values - in practice, load from calibration file
        # For now, use a simple linear mapping
        position_l = np.linspace(1, 2048, 18)
        position_r = np.linspace(1, 2048, 18)
        wave_sample = np.linspace(800, 870, 18)
    else:
        mu = calibration_data.get('mu')
        wave_prime = calibration_data.get('wave_prime')
        if mu is None or wave_prime is None:
            raise ValueError("calibration_data must contain 'mu' and 'wave_prime' keys")
        position_l = mu[:, 0] if mu.ndim > 1 else mu
        position_r = mu[:, 1] if mu.ndim > 1 and mu.shape[1] > 1 else mu
        wave_sample = wave_prime.flatten() if wave_prime.ndim > 1 else wave_prime
    
    # Ensure wave_sample is 1D and matches position_l size
    if wave_sample.ndim > 1:
        wave_sample = wave_sample.flatten()
    if len(wave_sample) != len(position_l):
        wave_sample = wave_sample.T if wave_sample.shape[0] != len(position_l) else wave_sample
    
    # Polynomial fitting (linear)
    pcoeff1 = np.polyfit(position_l, wave_sample, 1)
    pcoeff2 = np.polyfit(position_r, wave_sample, 1)
    
    x1 = np.arange(1, 2049)
    lamda_l = np.polyval(pcoeff1, x1)
    x2 = np.arange(1, 2049)
    lamda_r = np.polyval(pcoeff2, x2)
    
    w_l = lamda_l[start1 - 1:start1 + original_line_length1 - 1]
    w_r = lamda_r[start2 - 1:start2 + original_line_length2 - 1]
    
    xx1 = np.linspace(start1, start1 + original_line_length1 - 1, padding_length)
    xx2 = np.linspace(start2, start2 + original_line_length2 - 1, padding_length)
    
    wavelengths_l = 1e-9 * np.interp(xx1, np.arange(start1, start1 + original_line_length1), w_l)
    wavelengths_r = 1e-9 * np.interp(xx2, np.arange(start2, start2 + original_line_length2), w_r)
    
    min_k = 2 * np.pi / min(wavelengths_l[-1], wavelengths_r[-1])
    max_k = 2 * np.pi / max(wavelengths_l[0], wavelengths_r[0])
    
    ks = np.flipud(np.linspace(min_k, max_k, padding_length))
    interpolated_wavelengths = (2 * np.pi) / ks
    
    return wavelengths_l, wavelengths_r, interpolated_wavelengths


def spectral2complex(
    wavelength_buffers: List[Tuple[np.ndarray, np.ndarray]],
    phase_dispersion1: np.ndarray,
    phase_dispersion2: np.ndarray,
    aline: int,
    calibration_data: Optional[dict] = None,
    auto_corr_peak_cut: int = 24,
    padding_factor: int = 1,
    original_line_length1: int = 2048,
    original_line_length2: int = 2048,
    start1: int = 1,
    start2: int = 1,
) -> np.ndarray:
    """
    Convert wavelength buffer arrays to complex Jones stack.
    
    Parameters
    ----------
    wavelength_buffers : List[Tuple[np.ndarray, np.ndarray]]
        List of (wavelength_buffer1, wavelength_buffer2) tuples for each B-line.
        Each buffer should be shape (aline_length, aline) where aline_length is typically 2048.
    phase_dispersion1 : np.ndarray
        Phase dispersion correction for channel 1, shape (aline_length * padding_factor,)
    phase_dispersion2 : np.ndarray
        Phase dispersion correction for channel 2, shape (aline_length * padding_factor,)
    aline : int
        Number of A-lines
    calibration_data : dict, optional
        Calibration data for wavelength interpolation
    auto_corr_peak_cut : int
        Number of samples to cut from autocorrelation peak (default: 24)
    padding_factor : int
        Padding factor (default: 1)
    original_line_length1 : int
        Original line length for channel 1 (default: 2048)
    original_line_length2 : int
        Original line length for channel 2 (default: 2048)
    start1 : int
        Start index for channel 1 (default: 1)
    start2 : int
        Start index for channel 2 (default: 1)
        
    Returns
    -------
    np.ndarray
        Complex Jones stack, shape (4*Bline, Aline, Depth)
        Organized as [real(J1), imag(J1), real(J2), imag(J2)]
    """
    bline = len(wavelength_buffers)
    aline_length = wavelength_buffers[0][0].shape[0] if wavelength_buffers else 2048
    depth_l = 1024 - auto_corr_peak_cut
    
    padding_length = 2048 * padding_factor
    interpolation_parameters = (
        padding_factor,
        padding_length,
        original_line_length1,
        start1,
        original_line_length2,
        start2,
    )
    
    wavelengths_l, wavelengths_r, interpolated_wavelengths = interpolationwave(
        interpolation_parameters, calibration_data
    )
    
    # Prepare phase corrections
    phase_correction1 = np.exp(-1j * phase_dispersion1.reshape(aline_length * padding_factor, -1))
    phase_correction1 = np.tile(phase_correction1, (1, aline))
    
    phase_correction2 = np.exp(-1j * phase_dispersion2.reshape(aline_length * padding_factor, -1))
    phase_correction2 = np.tile(phase_correction2, (1, aline))
    
    # Stack all buffers into 3D arrays: (bline, aline_length, aline)
    wavelength_buffer1_stack = np.stack([buf[0] for buf in wavelength_buffers], axis=0)
    wavelength_buffer2_stack = np.stack([buf[1] for buf in wavelength_buffers], axis=0)
    
    # Flip channel 2 for all B-lines at once
    wavelength_buffer2_stack = np.flip(wavelength_buffer2_stack, axis=1)
    
    # Mean subtraction - vectorized across all B-lines
    refdata1 = np.mean(wavelength_buffer1_stack, axis=2, keepdims=True)  # (bline, aline_length, 1)
    refdata2 = np.mean(wavelength_buffer2_stack, axis=2, keepdims=True)  # (bline, aline_length, 1)
    mean_scan1 = wavelength_buffer1_stack - refdata1  # (bline, aline_length, aline)
    mean_scan2 = wavelength_buffer2_stack - refdata2  # (bline, aline_length, aline)
    
    # Extract original buffers - vectorized slicing
    original_buffer1 = mean_scan1[:, start1 - 1:start1 + original_line_length1 - 1, :]  # (bline, original_line_length1, aline)
    original_buffer2 = mean_scan2[:, start2 - 1:start2 + original_line_length2 - 1, :]  # (bline, original_line_length2, aline)
    
    # Zero padding - vectorized across B-lines
    # original_buffer1/2 are (bline, original_line_length, aline)
    # We can apply FFT along the depth dimension (axis=1) for all B-lines at once
    aline_length1 = original_buffer1.shape[1]
    aline_length2 = original_buffer2.shape[1]
    aline_dim = original_buffer1.shape[2]  # Number of A-lines
    mid_length1 = aline_length1 // 2 + 1
    mid_length2 = aline_length2 // 2 + 1
    
    # FFT along depth dimension (axis=1) for all B-lines
    transformed_buffer1 = np.fft.fft(original_buffer1, axis=1)  # (bline, original_line_length1, aline)
    transformed_buffer2 = np.fft.fft(original_buffer2, axis=1)  # (bline, original_line_length2, aline)
    
    # Adjust middle point
    transformed_buffer1[:, mid_length1 - 1, :] = transformed_buffer1[:, mid_length1 - 1, :] / 2
    transformed_buffer2[:, mid_length2 - 1, :] = transformed_buffer2[:, mid_length2 - 1, :] / 2
    
    # Create padded buffers
    padding_length1 = padding_factor * aline_length1
    padding_length2 = padding_factor * aline_length2
    padded_transformed_buffer1 = np.zeros(
        (bline, padding_length1, aline_dim), dtype=complex
    )
    padded_transformed_buffer2 = np.zeros(
        (bline, padding_length2, aline_dim), dtype=complex
    )
    
    # Copy FFT data to padded buffers
    padded_transformed_buffer1[:, :mid_length1, :] = transformed_buffer1[:, :mid_length1, :]
    padded_transformed_buffer1[:, -(mid_length1 - 1):, :] = transformed_buffer1[:, -(mid_length1 - 1):, :]
    
    padded_transformed_buffer2[:, :mid_length2, :] = transformed_buffer2[:, :mid_length2, :]
    padded_transformed_buffer2[:, -(mid_length2 - 1):, :] = transformed_buffer2[:, -(mid_length2 - 1):, :]
    
    # IFFT and take real part
    zero_padded_buffer1 = np.real(np.fft.ifft(padded_transformed_buffer1, axis=1)) * padding_factor
    zero_padded_buffer2 = np.real(np.fft.ifft(padded_transformed_buffer2, axis=1)) * padding_factor
    # Result: (bline, padding_length, aline)
    
    # Interpolation - vectorized using np.interp
    # Reshape for interpolation: (bline * aline, padding_length)
    bline_dim, padding_len, aline_dim = zero_padded_buffer1.shape
    interp_len = len(interpolated_wavelengths)
    
    zero_padded_buffer1_flat = zero_padded_buffer1.transpose(0, 2, 1).reshape(bline_dim * aline_dim, padding_len)
    zero_padded_buffer2_flat = zero_padded_buffer2.transpose(0, 2, 1).reshape(bline_dim * aline_dim, padding_len)
    
    # Vectorized interpolation: np.interp is fast and works well in a loop over rows
    # This is much better than the original loop since we've vectorized all other operations
    interpolated_buffer1_flat = np.array([
        np.interp(interpolated_wavelengths, wavelengths_l, zero_padded_buffer1_flat[i, :])
        for i in range(bline_dim * aline_dim)
    ])
    interpolated_buffer2_flat = np.array([
        np.interp(interpolated_wavelengths, wavelengths_r, zero_padded_buffer2_flat[i, :])
        for i in range(bline_dim * aline_dim)
    ])
    
    # Reshape back: (bline, aline, interp_len) -> (bline, interp_len, aline)
    interpolated_buffer1 = interpolated_buffer1_flat.reshape(bline_dim, aline_dim, interp_len).transpose(0, 2, 1)
    interpolated_buffer2 = interpolated_buffer2_flat.reshape(bline_dim, aline_dim, interp_len).transpose(0, 2, 1)
    
    # Median subtraction - vectorized
    median1 = np.median(interpolated_buffer1, axis=1, keepdims=True)  # (bline, 1, aline)
    median2 = np.median(interpolated_buffer2, axis=1, keepdims=True)  # (bline, 1, aline)
    interpolated_buffer1 = interpolated_buffer1 - median1
    interpolated_buffer2 = interpolated_buffer2 - median2
    
    # Apply phase correction - vectorized
    # phase_correction1 is (padding_length, aline), need to broadcast to (bline, padding_length, aline)
    phase_corr1_broadcast = phase_correction1[None, :, :]  # (1, padding_length, aline)
    phase_corr2_broadcast = phase_correction2[None, :, :]  # (1, padding_length, aline)
    interpolated_buffer1 = interpolated_buffer1 * phase_corr1_broadcast
    interpolated_buffer2 = interpolated_buffer2 * phase_corr2_broadcast
    
    # Compute Jones vectors - vectorized FFT
    # buffer2jones works on (depth, aline), we have (bline, padding_length, aline)
    # Apply FFT along axis 1 (depth dimension) for all B-lines
    aline_length_fft = interpolated_buffer1.shape[1] // (2 * padding_factor)
    jones1_all = np.fft.fft(interpolated_buffer1, axis=1)  # (bline, padding_length, aline)
    jones1_all = jones1_all[:, :aline_length_fft, :]  # (bline, aline_length_fft, aline)
    jones1_all = jones1_all[:, auto_corr_peak_cut:, :]  # (bline, depth_l, aline)
    
    jones2_all = np.fft.fft(interpolated_buffer2, axis=1)  # (bline, padding_length, aline)
    jones2_all = jones2_all[:, :aline_length_fft, :]  # (bline, aline_length_fft, aline)
    jones2_all = jones2_all[:, auto_corr_peak_cut:, :]  # (bline, depth_l, aline)
    
    # Transpose to get (bline, aline, depth_l)
    jones1_3d = jones1_all.transpose(0, 2, 1)  # (bline, aline, depth_l)
    jones2_3d = jones2_all.transpose(0, 2, 1)  # (bline, aline, depth_l)
    
    # Permute to Aline x Bline x Depth
    jones1_3d = np.transpose(jones1_3d, (1, 0, 2))
    jones2_3d = np.transpose(jones2_3d, (1, 0, 2))
    
    # Split into real/imaginary and concatenate
    j1_real = np.real(jones1_3d)
    j1_imag = np.imag(jones1_3d)
    j1_split = np.concatenate([j1_real, j1_imag], axis=0)  # (2*Bline) x Aline x Depth
    
    j2_real = np.real(jones2_3d)
    j2_imag = np.imag(jones2_3d)
    j2_split = np.concatenate([j2_real, j2_imag], axis=0)  # (2*Bline) x Aline x Depth
    
    # Final concatenation
    jstack_all = np.concatenate([j1_split, j2_split], axis=0)  # (4*Bline) x Aline x Depth
    jstack_all = np.flip(jstack_all, axis=2)
    
    return jstack_all


def read_spectral_file(
    filename: str,
    aline_length: int=2048,
    aline: int=200,
    bline: int=350,
    header_size: int = 352
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Read spectral data for a specific B-line from binary file.
    
    Parameters
    ----------
    filename : str
        Path to spectral data file
    file_ind : int
        B-line index (0-based)
    aline_length : int
        Length of each A-line (typically 2048)
    aline : int
        Number of A-lines
    header_size : int
        Size of file header in bytes (default: 352)
        
    Returns
    -------
    wavelength_buffer1 : np.ndarray
        Channel 1 data, shape (aline_length, aline) - raw, not flipped
    wavelength_buffer2 : np.ndarray
        Channel 2 data, shape (aline_length, aline) - raw, not flipped
        (s2c_array will handle the flipping)
    """
    with open(filename, 'rb') as fid:
        # Read channel 1
        offset1 = header_size
        fid.seek(offset1)
        data = np.fromfile(fid, dtype=np.uint16, count=aline_length * aline * 2 * bline)
        data = data.reshape(bline,aline,2,aline_length)
        wavelength_buffer1 = data[:,:,0,:]
        wavelength_buffer2 = data[:,:,1,:]
        # data1 = np.frombuffer(fid.read(aline_length * aline * 2), dtype=np.uint16)
        # wavelength_buffer1 = data1[:aline_length * aline].reshape(aline_length, aline)
        
        # # Read channel 2 (raw, s2c_array will flip it)
        # offset2 = offset1 + aline_length * aline * 2
        # fid.seek(offset2)
        # data2 = np.frombuffer(fid.read(aline_length * aline * 2), dtype=np.uint16)
        # wavelength_buffer2 = data2[:aline_length * aline].reshape(aline_length, aline)
    
    return wavelength_buffer1, wavelength_buffer2


def s2c_file(
    filename: str,
    file_num: int,
    disp_comp_file: str,
    aline_length: int,
    bline_length: int,
    output_path: str,
    calibration_data: Optional[dict] = None,
    auto_corr_peak_cut: int = 24,
    padding_factor: int = 1,
    original_line_length1: int = 2048,
    original_line_length2: int = 2048,
    start1: int = 1,
    start2: int = 1,
) -> str:
    """
    Process spectral file and save complex Jones stack to NIfTI.
    
    Parameters
    ----------
    filename : str
        Path to input spectral file
    file_num : int
        File number (for logging)
    disp_comp_file : str
        Path to dispersion compensation file (binary, double precision)
    aline_length : int
        Number of A-lines
    bline_length : int
        Number of B-lines
    output_path : str
        Output directory path
    calibration_data : dict, optional
        Calibration data for wavelength interpolation
    auto_corr_peak_cut : int
        Number of samples to cut from autocorrelation peak (default: 24)
    padding_factor : int
        Padding factor (default: 1)
    original_line_length1 : int
        Original line length for channel 1 (default: 2048)
    original_line_length2 : int
        Original line length for channel 2 (default: 2048)
    start1 : int
        Start index for channel 1 (default: 1)
    start2 : int
        Start index for channel 2 (default: 1)
        
    Returns
    -------
    str
        Path to output file
    """
    # Generate output filename
    input_path = Path(filename)
    base_name = input_path.stem
    
    # Replace anything starting with 'spectral' and ending before extension with 'processed_cropped'
    # Equivalent to MATLAB: regexprep(base_name, 'spectral.*$', 'processed_cropped')
    new_base = re.sub(r'spectral.*$', 'processed_cropped', base_name, flags=re.IGNORECASE)
    
    output_file = Path(output_path) / f"{new_base}.nii"
    
    if output_file.exists():
        print(f"Output file already exists: {output_file}")
        return str(output_file)
    
    print(f'aline = {aline_length}; bline = {bline_length}')
    
    # Read dispersion compensation file (same file used for both channels)
    with open(disp_comp_file, 'rb') as f:
        phase_dispersion_data = np.frombuffer(f.read(), dtype=np.float64)
    phase_dispersion1 = phase_dispersion_data
    phase_dispersion2 = phase_dispersion_data
    
    # Read all wavelength buffers from file
    aline = aline_length
    wavelength_buffers = []
    for file_ind in range(bline_length):
        if (file_ind + 1) % 50 == 0:
            print(f'Reading bline {file_ind + 1}/{bline_length}')
        wavelength_buffer1, wavelength_buffer2 = read_spectral_file(
            filename, file_ind, aline_length, aline
        )
        wavelength_buffers.append((wavelength_buffer1, wavelength_buffer2))
    
    # Process using s2c_array
    jstack_all = spectral2complex(
        wavelength_buffers=wavelength_buffers,
        phase_dispersion1=phase_dispersion1,
        phase_dispersion2=phase_dispersion2,
        aline=aline,
        calibration_data=calibration_data,
        auto_corr_peak_cut=auto_corr_peak_cut,
        padding_factor=padding_factor,
        original_line_length1=original_line_length1,
        original_line_length2=original_line_length2,
        start1=start1,
        start2=start2,
    )
    
    # Save to NIfTI
    print(f'Saving output to: {output_file}')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Create NIfTI image
    img = nib.Nifti1Image(jstack_all.astype(np.float32), affine=np.eye(4))
    nib.save(img, str(output_file))
    
    return str(output_file)


def main():
    """CLI entry point for s2c."""
    parser = argparse.ArgumentParser(
        description='Convert spectral OCT data to complex Jones stack',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'filename',
        type=str,
        help='Path to input spectral file'
    )
    parser.add_argument(
        '--disp-comp-file',
        type=str,
        required=True,
        help='Path to dispersion compensation file (binary, double precision)'
    )
    parser.add_argument(
        '--aline',
        type=int,
        required=True,
        help='Number of A-lines'
    )
    parser.add_argument(
        '--bline',
        type=int,
        required=True,
        help='Number of B-lines'
    )
    parser.add_argument(
        '--output-path',
        type=str,
        default='.',
        help='Output directory path (default: current directory)'
    )
    parser.add_argument(
        '--file-num',
        type=int,
        default=1,
        help='File number for logging (default: 1)'
    )
    parser.add_argument(
        '--auto-corr-peak-cut',
        type=int,
        default=24,
        help='Number of samples to cut from autocorrelation peak (default: 24)'
    )
    parser.add_argument(
        '--padding-factor',
        type=int,
        default=1,
        help='Padding factor (default: 1)'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.filename):
        print(f"Error: Input file not found: {args.filename}", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.exists(args.disp_comp_file):
        print(f"Error: Dispersion compensation file not found: {args.disp_comp_file}", file=sys.stderr)
        sys.exit(1)
    
    # Process file
    try:
        output_file = s2c_file(
            filename=args.filename,
            file_num=args.file_num,
            disp_comp_file=args.disp_comp_file,
            aline_length=args.aline,
            bline_length=args.bline,
            output_path=args.output_path,
            auto_corr_peak_cut=args.auto_corr_peak_cut,
            padding_factor=args.padding_factor,
        )
        print(f"Successfully processed: {output_file}")
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

