"""
Slice registration tasks for combining normal and tilted illumination mosaics.

This module provides Python wrappers for MATLAB functions:
- thruplane: Registers normal and tilted illuminations
- RGB_3Daxis: Generates 3D axis visualizations

These tasks adapt the new mosaic-based structure to work with the existing
MATLAB functions that expect the old slice-based structure.
"""

import subprocess
from pathlib import Path
from typing import Dict, Optional

from prefect import get_run_logger, task

from workflow.tasks.utils import get_slice_paths


def _get_matlab_engine():
    """
    Get MATLAB engine instance.
    
    Returns
    -------
    matlab.engine.MatlabEngine
        MATLAB engine instance
        
    Raises
    ------
    ImportError
        If matlab.engine is not available
    RuntimeError
        If MATLAB engine cannot be started
    """
    logger = get_run_logger()
    try:
        import matlab.engine
    except ImportError:
        raise ImportError(
            "MATLAB Engine for Python is not installed. "
            "Install it using: pip install matlabengine"
        )

    try:
        # Start MATLAB engine
        eng = matlab.engine.start_matlab()
        logger.info("MATLAB engine started successfully")
        return eng
    except Exception as e:
        raise RuntimeError(f"Failed to start MATLAB engine: {e}")


def _setup_processed_directory(
    project_base_path: str,
    slice_number: int,
    normal_mosaic_id: int,
    tilted_mosaic_id: int,
) -> Path:
    """
    Set up processed directory structure for MATLAB functions.
    
    The MATLAB functions expect a processed directory with stitched mosaic files.
    This function creates/verifies the directory structure and ensures the
    necessary files are present.
    
    Parameters
    ----------
    project_base_path : str
        Base path for the project
    slice_number : int
        Slice number (1-indexed)
    normal_mosaic_id : int
        Normal illumination mosaic ID
    tilted_mosaic_id : int
        Tilted illumination mosaic ID
        
    Returns
    -------
    Path
        Path to processed directory
    """
    logger = get_run_logger()
    # Get slice paths using new structure
    processed_path, stitched_path, _, _ = get_slice_paths(
        project_base_path, slice_number
    )

    # Create processed directory if it doesn't exist
    processed_path.mkdir(parents=True, exist_ok=True)

    # Check if stitched files exist for both mosaics
    # The MATLAB functions expect files in the processed directory
    # We may need to copy or symlink stitched files to processed directory
    # or adapt the MATLAB functions to read from stitched directory

    # For now, verify that stitched directory has the expected files
    if not stitched_path.exists():
        raise FileNotFoundError(
            f"Stitched directory not found: {stitched_path}. "
            f"Mosaics must be stitched before registration."
        )

    # Check for key stitched files (AIP is typically used for registration)
    normal_aip = stitched_path / f"mosaic_{normal_mosaic_id:03d}_aip.nii"
    tilted_aip = stitched_path / f"mosaic_{tilted_mosaic_id:03d}_aip.nii"

    if not normal_aip.exists():
        logger.warning(f"Normal mosaic AIP not found: {normal_aip}")

    if not tilted_aip.exists():
        logger.warning(f"Tilted mosaic AIP not found: {tilted_aip}")

    logger.info(
        f"Processed directory ready: {processed_path} "
        f"(stitched files in: {stitched_path})"
    )

    return stitched_path


@task(name="thruplane_registration")
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
    Perform thruplane registration of normal and tilted illumination mosaics.
    
    This is a Python wrapper for the MATLAB function `thruplane()`.
    It registers the normal and tilted illuminations to combine orientations.
    
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
        Tilt angle parameter for registration (default: -15.0)
    mask_file : str
        Mask file type ("aip", "mip", or "" for no mask)
    mask_threshold : float
        Threshold for mask generation (default: 55.0)
    matlab_script_path : str, optional
        Path to MATLAB script directory. If None, uses current directory
        or searches for thruplane.m
        
    Returns
    -------
    Path
        Path to processed directory containing registration results
        
    Notes
    -----
    The MATLAB function signature is:
        thruplane(processed_dir, gamma, slice_number)
    
    Where:
    - processed_dir: Path to processed directory (old structure: slice-XX/processed/)
    - gamma: Tilt angle parameter
    - slice_number: Slice number
    """
    logger = get_run_logger()
    logger.info(
        f"Starting thruplane registration for slice {slice_number} "
        f"(mosaics {normal_mosaic_id} and {tilted_mosaic_id})"
    )

    # Set up processed directory
    processed_path = _setup_processed_directory(
        project_base_path,
        slice_number,
        normal_mosaic_id,
        tilted_mosaic_id,
    )

    # Convert paths to absolute paths for MATLAB
    processed_dir = str(processed_path.absolute())

    try:
        # Get MATLAB engine
        eng = _get_matlab_engine()

        # Add MATLAB script path if provided
        if matlab_script_path:
            eng.addpath(matlab_script_path, nargout=0)
        else:
            # Try to find MATLAB scripts in common locations
            # This assumes MATLAB scripts are in the workspace or a known location
            current_dir = Path(__file__).parent.parent.parent
            possible_paths = [
                Path("/path/to/matlab/scripts"),  # Update with actual path
            ]
            for path in possible_paths:
                if path.exists():
                    eng.addpath(str(path), nargout=0)
                    logger.info(f"Added MATLAB path: {path}")
                    break

        # Call MATLAB function: thruplane(processed_dir, gamma, slice_number)
        logger.info(
            f"Calling MATLAB thruplane({processed_dir}, {gamma}, {slice_number})"
        )

        eng.thruplane(
            processed_dir,
            float(gamma),
            float(slice_number),
            nargout=0
        )

        logger.info("thruplane registration completed successfully")

        # Close MATLAB engine
        eng.quit()

    except ImportError:
        # Fallback: Try calling MATLAB via command line
        logger.warning(
            "MATLAB Engine not available, attempting command-line call"
        )
        _call_matlab_via_cli(
            "thruplane",
            str(processed_dir) + "/",
            gamma,
            slice_number,
            matlab_script_path=matlab_script_path,
        )
    except Exception as e:
        logger.error(f"Error in thruplane registration: {e}")
        raise

    return processed_path


@task(name="rgb_3daxis")
def rgb_3daxis_task(
    processed_dir: Path,
    slice_number: int,
    matlab_script_path: Optional[str] = None,
) -> Dict[str, Path]:
    """
    Generate RGB 3D axis visualization.
    
    This is a Python wrapper for the MATLAB function `RGB_3Daxis()`.
    It generates 3D axis visualizations from registered orientation data.
    
    Parameters
    ----------
    processed_dir : Path
        Path to processed directory containing registration results
    slice_number : int
        Slice number (1-indexed)
    matlab_script_path : str, optional
        Path to MATLAB script directory. If None, uses current directory
        or searches for RGB_3Daxis.m
        
    Returns
    -------
    Dict[str, Path]
        Dictionary with output file paths:
        - 'axis_image': Path to 3D axis image file
        
    Notes
    -----
    The MATLAB function signature is:
        RGB_3Daxis(processed_dir, slice_number)
    
    Where:
    - processed_dir: Path to processed directory
    - slice_number: Slice number
    """
    logger = get_run_logger()
    logger.info(
        f"Starting RGB_3Daxis visualization for slice {slice_number} "
        f"in {processed_dir}"
    )

    if not processed_dir.exists():
        raise FileNotFoundError(
            f"Processed directory not found: {processed_dir}. "
            f"Run thruplane registration first."
        )

    # Convert path to absolute path for MATLAB
    processed_dir_str = str(processed_dir.absolute()) + "/"

    try:
        # Get MATLAB engine
        eng = _get_matlab_engine()

        # Add MATLAB script path if provided
        if matlab_script_path:
            eng.addpath(matlab_script_path, nargout=0)
        else:
            # Try to find MATLAB scripts in common locations
            current_dir = Path(__file__).parent.parent.parent
            possible_paths = [
                current_dir,
                current_dir / "telesto",
                Path("/path/to/matlab/scripts"),  # Update with actual path
            ]
            for path in possible_paths:
                if path.exists():
                    eng.addpath(str(path), nargout=0)
                    logger.info(f"Added MATLAB path: {path}")
                    break

        # Call MATLAB function: RGB_3Daxis(processed_dir, slice_number)
        logger.info(
            f"Calling MATLAB RGB_3Daxis({processed_dir_str}, {slice_number})"
        )

        eng.RGB_3Daxis(
            processed_dir_str,
            float(slice_number),
            nargout=0
        )

        logger.info("RGB_3Daxis visualization completed successfully")

        # Close MATLAB engine
        eng.quit()

        # Find output files (MATLAB typically saves to processed_dir)
        # The exact output filename depends on the MATLAB function implementation
        # Common patterns: slice-XX_3daxis.jpg, RGB_3Daxis_slice-XX.jpg, etc.
        output_patterns = [
            processed_dir / f"slice-{slice_number:02d}_3daxis.jpg",
            processed_dir / f"RGB_3Daxis_slice-{slice_number:02d}.jpg",
            processed_dir / f"slice-{slice_number:02d}_3daxis.png",
            processed_dir / f"RGB_3Daxis_slice-{slice_number:02d}.png",
        ]

        axis_image = None
        for pattern in output_patterns:
            if pattern.exists():
                axis_image = pattern
                break

        if axis_image is None:
            logger.warning(
                f"Could not find RGB_3Daxis output file in {processed_dir}. "
                f"Expected patterns: {[str(p) for p in output_patterns]}"
            )
            # Return a placeholder path
            axis_image = processed_dir / f"slice-{slice_number:02d}_3daxis.jpg"

    except ImportError:
        # Fallback: Try calling MATLAB via command line
        logger.warning(
            "MATLAB Engine not available, attempting command-line call"
        )
        _call_matlab_via_cli(
            "RGB_3Daxis",
            processed_dir_str,
            slice_number,
            matlab_script_path=matlab_script_path,
        )

        # Try to find output file
        output_patterns = [
            processed_dir / f"slice-{slice_number:02d}_3daxis.jpg",
            processed_dir / f"RGB_3Daxis_slice-{slice_number:02d}.jpg",
        ]
        axis_image = None
        for pattern in output_patterns:
            if pattern.exists():
                axis_image = pattern
                break
        if axis_image is None:
            axis_image = processed_dir / f"slice-{slice_number:02d}_3daxis.jpg"

    except Exception as e:
        logger.error(f"Error in RGB_3Daxis visualization: {e}")
        raise

    return {
        "axis_image": axis_image,
    }


def _call_matlab_via_cli(
    function_name: str,
    *args,
    matlab_script_path: Optional[str] = None,
) -> None:
    """
    Call MATLAB function via command line as fallback.
    
    Parameters
    ----------
    function_name : str
        Name of MATLAB function to call
    *args
        Arguments to pass to MATLAB function
    matlab_script_path : str, optional
        Path to MATLAB script directory
    """
    logger = get_run_logger()
    # Build MATLAB command
    # Format: matlab -batch "function_name(arg1, arg2, ...)"

    # Convert arguments to MATLAB format
    matlab_args = []
    for arg in args:
        if isinstance(arg, (int, float)):
            matlab_args.append(str(arg))
        elif isinstance(arg, str):
            # Escape quotes and wrap in quotes
            escaped = arg.replace("'", "''")
            matlab_args.append(f"'{escaped}'")
        else:
            matlab_args.append(str(arg))

    # Add path if provided
    path_cmd = ""
    if matlab_script_path:
        path_cmd = f"addpath(genpath('{matlab_script_path}')); "

    # Build MATLAB command
    args_str = ", ".join(matlab_args)
    matlab_cmd = f"{path_cmd}{function_name}({args_str})"

    # Execute MATLAB
    cmd = ["matlab", "-batch", matlab_cmd]

    logger.info(f"Executing MATLAB command: {' '.join(cmd)}")
    print(cmd)
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info(f"MATLAB command completed: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"MATLAB command failed: {e.stderr}")
        raise
    except FileNotFoundError:
        raise RuntimeError(
            "MATLAB not found in PATH. "
            "Please install MATLAB or use MATLAB Engine for Python."
        )
