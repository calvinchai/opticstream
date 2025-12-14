"""
Tasks for slice registration processing.

These tasks wrap MATLAB functions (thruplane and RGB_3Daxis) to work with
the new project structure where mosaics are organized by mosaic_id rather
than by slice directories.
"""

import logging
import subprocess
from pathlib import Path
from typing import Optional

from prefect import task

logger = logging.getLogger(__name__)


@task(name="thruplane_registration_task")
def thruplane_registration_task(
    project_base_path: str,
    slice_number: int,
    normal_mosaic_id: int,
    tilted_mosaic_id: int,
    gamma: float = -15.0,
    mask_file: str = "",
    mask_threshold: float = 55.0,
    matlab_script_path: Optional[str] = None,
) -> Path:
    """
    Run thruplane registration for a slice.
    
    This task wraps the MATLAB thruplane function to work with the new structure.
    It creates a temporary processed directory structure that matches what the
    MATLAB function expects, then calls the MATLAB function.
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
    normal_mosaic_id : int
        Normal illumination mosaic ID (2n-1)
    tilted_mosaic_id : int
        Tilted illumination mosaic ID (2n)
    gamma : float
        Tilt angle parameter (default: -15.0)
    mask_file : str
        Mask file type ("aip", "mip", or "" for no mask)
    mask_threshold : float
        Threshold for mask generation (default: 55.0)
    matlab_script_path : str, optional
        Path to MATLAB script wrapper. If None, uses default location.
        
    Returns
    -------
    Path
        Path to the output directory containing registration results
    """
    project_path = Path(project_base_path)
    
    # Create a temporary processed directory structure for MATLAB compatibility
    # MATLAB expects: {basename}/mosaic_{id:03d}_{modality}.tif
    # New structure: {project_base_path}/mosaic-{id:03d}/stitched/mosaic_{id:03d}_{modality}.tif
    
    # Create a slice-specific processed directory
    slice_processed_dir = project_path / f"slice-{slice_number:02d}" / "processed"
    slice_processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Create symbolic links or copy files to match old structure
    # MATLAB expects files like: mosaic_001_ori.tif, mosaic_002_ori.tif, etc.
    normal_mosaic_path = project_path / f"mosaic-{normal_mosaic_id:03d}" / "stitched"
    tilted_mosaic_path = project_path / f"mosaic-{tilted_mosaic_id:03d}" / "stitched"
    
    # Required files for thruplane:
    # - mosaic_{normal_id}_ori.tif
    # - mosaic_{tilted_id}_ori.tif
    # - mosaic_{normal_id}_biref.tif
    # - mosaic_{tilted_id}_biref.tif
    # Optional: mosaic_{id}_aip.tif or mosaic_{id}_mip.tif for mask
    
    required_files = {
        f"mosaic_{normal_mosaic_id:03d}_ori.tif": normal_mosaic_path / f"mosaic_{normal_mosaic_id:03d}_ori.tif",
        f"mosaic_{tilted_mosaic_id:03d}_ori.tif": tilted_mosaic_path / f"mosaic_{tilted_mosaic_id:03d}_ori.tif",
        f"mosaic_{normal_mosaic_id:03d}_biref.tif": normal_mosaic_path / f"mosaic_{normal_mosaic_id:03d}_biref.tif",
        f"mosaic_{tilted_mosaic_id:03d}_biref.tif": tilted_mosaic_path / f"mosaic_{tilted_mosaic_id:03d}_biref.tif",
    }
    
    # Add mask files if specified
    if mask_file:
        mask_postfix = f"_{mask_file}.tif"
        required_files[f"mosaic_{normal_mosaic_id:03d}{mask_postfix}"] = (
            normal_mosaic_path / f"mosaic_{normal_mosaic_id:03d}{mask_postfix}"
        )
        required_files[f"mosaic_{tilted_mosaic_id:03d}{mask_postfix}"] = (
            tilted_mosaic_path / f"mosaic_{tilted_mosaic_id:03d}{mask_postfix}"
        )
    
    # Check that all required files exist
    missing_files = []
    for target_name, source_path in required_files.items():
        if not source_path.exists():
            missing_files.append(str(source_path))
    
    if missing_files:
        raise FileNotFoundError(
            f"Required files for thruplane registration not found:\n"
            + "\n".join(f"  - {f}" for f in missing_files)
        )
    
    # Create symbolic links in the processed directory
    for target_name, source_path in required_files.items():
        target_path = slice_processed_dir / target_name
        if target_path.exists() or target_path.is_symlink():
            target_path.unlink()
        target_path.symlink_to(source_path)
        logger.debug(f"Created symlink: {target_path} -> {source_path}")
    
    # Prepare MATLAB command
    # The MATLAB function expects: thruplane(basename, gamma, slice_numbers, mask_file, mask_threshold)
    # basename should be the directory path with trailing slash
    basename = str(slice_processed_dir) + "/"
    
    # Create temporary MATLAB script to run thruplane
    script_dir = slice_processed_dir / "scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    matlab_script_path = script_dir / f"run_thruplane_slice{slice_number}.m"
    
    # Create MATLAB wrapper script
    _create_thruplane_wrapper(matlab_script_path, basename, gamma, slice_number, mask_file, mask_threshold)
    
    # Run MATLAB script
    logger.info(
        f"Running thruplane registration for slice {slice_number} "
        f"(mosaics {normal_mosaic_id} and {tilted_mosaic_id})"
    )
    
    try:
        # Run MATLAB with the script file
        # Use -batch flag for non-interactive execution
        cmd = [
            "matlab",
            "-batch",
            f"run('{matlab_script_path}')"
        ]
        
        result = subprocess.run(
            cmd,
            cwd=str(slice_processed_dir),
            capture_output=True,
            text=True,
            check=True,
        )
        
        logger.info(f"thruplane registration completed for slice {slice_number}")
        if result.stdout:
            logger.debug(f"MATLAB stdout: {result.stdout}")
        if result.stderr:
            logger.debug(f"MATLAB stderr: {result.stderr}")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"thruplane registration failed")
        if e.stdout:
            logger.error(f"MATLAB stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"MATLAB stderr: {e.stderr}")
        raise
    
    # Check for output file
    output_file = slice_processed_dir / f"par_slice{slice_number}_data_0_100.mat"
    if not output_file.exists():
        raise FileNotFoundError(
            f"Expected output file not found: {output_file}. "
            f"Registration may have failed."
        )
    
    logger.info(f"Registration output saved to: {output_file}")
    
    return slice_processed_dir


@task(name="rgb_3daxis_task")
def rgb_3daxis_task(
    processed_dir: Path,
    slice_number: int,
    matlab_script_path: Optional[str] = None,
) -> dict:
    """
    Run RGB_3Daxis visualization for a slice.
    
    This task wraps the MATLAB RGB_3Daxis function to generate 3D axis
    visualizations from the registration results.
    
    Parameters
    ----------
    processed_dir : Path
        Path to the processed directory containing registration results
    slice_number : int
        Slice number (1-indexed)
    matlab_script_path : str, optional
        Path to MATLAB script wrapper. If None, uses default location.
        
    Returns
    -------
    dict
        Dictionary with paths to output files:
        - jpg_path: Path to JPG visualization
        - nii_path: Path to NIfTI file
    """
    # Check for input file
    input_file = processed_dir / f"par_slice{slice_number}_data_0_100.mat"
    if not input_file.exists():
        raise FileNotFoundError(
            f"Registration output file not found: {input_file}. "
            f"Run thruplane_registration_task first."
        )
    
    # Prepare MATLAB command
    # The MATLAB function expects: RGB_3Daxis(basename, slice_nums)
    # basename should be the directory path (with trailing slash for file naming)
    basename = str(processed_dir) + "/"
    
    # Create temporary MATLAB script to run RGB_3Daxis
    script_dir = processed_dir / "scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    matlab_script_path = script_dir / f"run_rgb_3daxis_slice{slice_number}.m"
    
    # Create MATLAB wrapper script
    _create_rgb_3daxis_wrapper(matlab_script_path, basename, slice_number)
    
    logger.info(f"Running RGB_3Daxis for slice {slice_number}")
    
    try:
        # Run MATLAB with the script file
        cmd = [
            "matlab",
            "-batch",
            f"run('{matlab_script_path}')"
        ]
        
        result = subprocess.run(
            cmd,
            cwd=str(processed_dir),
            capture_output=True,
            text=True,
            check=True,
        )
        
        logger.info(f"RGB_3Daxis completed for slice {slice_number}")
        if result.stdout:
            logger.debug(f"MATLAB stdout: {result.stdout}")
        if result.stderr:
            logger.debug(f"MATLAB stderr: {result.stderr}")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"RGB_3Daxis failed")
        if e.stdout:
            logger.error(f"MATLAB stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"MATLAB stderr: {e.stderr}")
        raise
    
    # Check for output files
    jpg_path = processed_dir / f"3daxis{slice_number}.jpg"
    nii_path = processed_dir / f"3daxis{slice_number}.nii"
    
    outputs = {}
    if jpg_path.exists():
        outputs["jpg_path"] = jpg_path
    if nii_path.exists():
        outputs["nii_path"] = nii_path
    
    if not outputs:
        raise FileNotFoundError(
            f"Expected output files not found for slice {slice_number}. "
            f"RGB_3Daxis may have failed."
        )
    
    logger.info(f"3D axis outputs saved: {list(outputs.keys())}")
    
    return outputs


def _create_thruplane_wrapper(
    script_path: Path,
    basename: str,
    gamma: float,
    slice_number: int,
    mask_file: str,
    mask_threshold: float,
):
    """
    Create a MATLAB wrapper script for thruplane function.
    
    This is a helper function that creates a MATLAB script to call
    the thruplane function with the correct parameters.
    """
    # Escape backslashes in path for MATLAB
    basename_escaped = basename.replace('\\', '\\\\')
    
    # Path to psoct-renew registration functions
    # Try to find it relative to common locations
    psoct_renew_paths = [
        '/space/megaera/1/users/kchai/code/psoct-renew',
        '/autofs/cluster/octdata2/users/Chao/code/psoct-renew',
    ]
    
    psoct_renew_path = None
    for path in psoct_renew_paths:
        if Path(path).exists():
            psoct_renew_path = path
            break
    
    script_content = f"""% MATLAB wrapper for thruplane function
% Auto-generated script

% Add required paths
addpath('/autofs/cluster/octdata2/users/Chao/code/demon_registration_version_8f');
addpath('/autofs/cluster/octdata2/users/Chao/code/telesto');
addpath('/space/omega/1/users/3d_axis/PAPER/scripts');
"""
    
    # Add psoct-renew path if found
    if psoct_renew_path:
        script_content += f"addpath('{psoct_renew_path}/registration');\n"
    
    script_content += f"""
% Call thruplane function
try
    thruplane('{basename_escaped}', {gamma}, [{slice_number}], '{mask_file}', {mask_threshold});
    fprintf('thruplane registration completed for slice %d\\n', {slice_number});
catch ME
    fprintf('Error in thruplane: %s\\n', ME.message);
    rethrow(ME);
end
"""
    
    script_path.write_text(script_content)
    logger.debug(f"Created MATLAB wrapper script: {script_path}")


def _create_rgb_3daxis_wrapper(
    script_path: Path,
    basename: str,
    slice_number: int,
):
    """
    Create a MATLAB wrapper script for RGB_3Daxis function.
    
    This is a helper function that creates a MATLAB script to call
    the RGB_3Daxis function with the correct parameters.
    """
    # Escape backslashes in path for MATLAB
    basename_escaped = basename.replace('\\', '\\\\')
    
    # Path to psoct-renew registration functions
    # Try to find it relative to common locations
    psoct_renew_paths = [
        '/space/megaera/1/users/kchai/code/psoct-renew',
        '/autofs/cluster/octdata2/users/Chao/code/psoct-renew',
    ]
    
    psoct_renew_path = None
    for path in psoct_renew_paths:
        if Path(path).exists():
            psoct_renew_path = path
            break
    
    script_content = f"""% MATLAB wrapper for RGB_3Daxis function
% Auto-generated script

% Add required paths (same as thruplane)
addpath('/autofs/cluster/octdata2/users/Chao/code/demon_registration_version_8f');
addpath('/autofs/cluster/octdata2/users/Chao/code/telesto');
addpath('/space/omega/1/users/3d_axis/PAPER/scripts');
"""
    
    # Add psoct-renew path if found
    if psoct_renew_path:
        script_content += f"addpath('{psoct_renew_path}/registration');\n"
    
    script_content += f"""
% Call RGB_3Daxis function
try
    RGB_3Daxis('{basename_escaped}', [{slice_number}]);
    fprintf('RGB_3Daxis completed for slice %d\\n', {slice_number});
catch ME
    fprintf('Error in RGB_3Daxis: %s\\n', ME.message);
    rethrow(ME);
end
"""
    
    script_path.write_text(script_content)
    logger.debug(f"Created MATLAB wrapper script: {script_path}")
