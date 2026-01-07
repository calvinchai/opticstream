"""
MATLAB batch command execution utilities.

Centralizes MATLAB subprocess execution with consistent error handling.
"""

import subprocess
from typing import Optional

from prefect.logging import get_run_logger


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

