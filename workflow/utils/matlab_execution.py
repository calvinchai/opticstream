"""
MATLAB batch command execution utilities.

Centralizes MATLAB subprocess execution with consistent error handling.
Includes MATLAB engine initialization and command-line fallback utilities.
"""

import subprocess
from pathlib import Path
from typing import Optional

from prefect.logging import get_run_logger
from prefect_shell import ShellOperation


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
    with ShellOperation(
        commands=[cmd],
    ) as matlab_operation:
        matlab_process = matlab_operation.trigger()
        matlab_process.wait_for_completion()
        output = matlab_process.fetch_result()
        logger.info(f"MATLAB execution output: {output}")


def run_matlab_batch_command(
    command: str,
    matlab_script_path: Optional[str] = "/homes/5/kc1708/localhome/code/psoct-renew/",
    working_dir: Optional[str] = None,
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

    
    # Build full command - escape double quotes in matlab_cmd for shell
    # MATLAB commands use single quotes, so we wrap the whole thing in double quotes
    escaped_cmd = matlab_cmd.replace('"', '\\"')
    full_command = f'matlab -batch "{escaped_cmd}"'
    
    logger.info(f"Executing MATLAB command: {command}")
    logger.debug(f"Full MATLAB command: {full_command}")
    
    with ShellOperation(
        commands=[full_command],
        working_dir=working_dir if working_dir else os.getcwd(),
    ) as matlab_operation:
        matlab_process = matlab_operation.trigger()
        matlab_process.wait_for_completion()
        output = matlab_process.fetch_result()
        if matlab_process.exit_code != 0:
            logger.error(f"MATLAB command failed: {output}")
            raise RuntimeError(f"MATLAB command failed: {output}")
        logger.info(f"MATLAB execution output: {output}")


