"""
MATLAB batch command execution utilities.

Centralizes MATLAB subprocess execution with consistent error handling.
Includes MATLAB engine initialization and command-line fallback utilities.
"""

import subprocess
from pathlib import Path
from typing import Optional

from prefect.logging import get_run_logger


def get_matlab_engine():
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


def call_matlab_via_cli(
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

    Raises
    ------
    RuntimeError
        If MATLAB is not found in PATH
    subprocess.CalledProcessError
        If MATLAB command fails
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
        # Also add registration subdirectory
        registration_path = Path(matlab_script_path) / "registration"
        if registration_path.exists():
            path_cmd += f"addpath('{registration_path}'); "

    # Build MATLAB command
    args_str = ", ".join(matlab_args)
    matlab_cmd = f"{path_cmd}{function_name}({args_str})"

    # Execute MATLAB
    cmd = ["matlab", "-batch", matlab_cmd]

    logger.info(f"Executing MATLAB command: {' '.join(cmd)}")
    logger.debug(f"MATLAB command string: {matlab_cmd}")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info(f"MATLAB command completed: {result.stdout}")
        if result.stderr:
            logger.warning(f"MATLAB stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logger.error(f"MATLAB command failed: {e.stderr}")
        logger.error(f"MATLAB stdout: {e.stdout}")
        raise
    except FileNotFoundError:
        raise RuntimeError(
            "MATLAB not found in PATH. "
            "Please install MATLAB or use MATLAB Engine for Python."
        )


def run_matlab_batch_command(
    command: str,
    matlab_script_path: str = "/homes/5/kc1708/localhome/code/psoct-renew/",
) -> subprocess.CompletedProcess:
    """
    Execute a MATLAB batch command.

    Builds MATLAB command string with addpath, executes subprocess.run,
    and handles errors consistently.

    Parameters
    ----------
    command : str
        MATLAB command to execute (e.g., "spectral2complex_batch(...)")
    matlab_script_path : str, optional
        Path to MATLAB script directory for addpath

    Returns
    -------
    subprocess.CompletedProcess
        Completed process result

    Raises
    ------
    ValueError
        If MATLAB command returns non-zero exit code
    """
    logger = get_run_logger()
    matlab_cmd = f"addpath(genpath('{matlab_script_path}'));{command}"
    full_command = ["matlab", "-batch", matlab_cmd]

    logger.debug(f"Executing MATLAB command: {command}")
    result = subprocess.run(full_command)

    if result.returncode != 0:
        error_msg = f"Error executing MATLAB command: {result.stderr}"
        logger.error(f"{error_msg}\nFull command: {matlab_cmd}")
        raise ValueError(error_msg)

    return result

