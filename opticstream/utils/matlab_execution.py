"""
MATLAB batch command execution utilities.

Centralizes MATLAB subprocess execution with consistent error handling.
Includes MATLAB engine initialization and command-line fallback utilities.
"""

import os
import subprocess
from numbers import Number
from pathlib import Path
from typing import Any, Optional

from prefect.logging import get_run_logger
from prefect_shell import ShellOperation

import psoct_toolbox


def matlab_literal(value: Any) -> str:
    """
    Convert a Python value into a MATLAB literal string.

    Supports scalars, strings, bools, vectors, and dict-backed structs.
    """
    if isinstance(value, dict):
        return dict_to_matlab_literal(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "''"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, Number):
        return str(value)
    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        return "[" + ", ".join(matlab_literal(v) for v in value) + "]"
    raise TypeError(f"Unsupported value type for MATLAB literal: {type(value)}")


def dict_to_matlab_literal(value: dict[str, Any]) -> str:
    """
    Convert a nested dict into a MATLAB struct literal.
    """
    if not value:
        return "struct()"
    fields: list[str] = []
    for key, field_value in value.items():
        key_lit = matlab_literal(str(key))
        value_lit = matlab_literal(field_value)
        fields.append(f"{key_lit}, {value_lit}")
    return f"struct({', '.join(fields)})"


def get_default_matlab_root() -> str:
    """
    Return the default MATLAB toolbox root from the installed psoct_toolbox.
    """
    try:
        return psoct_toolbox.get_matlab_root()
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError(
            "Failed to determine MATLAB toolbox root from psoct_toolbox. "
            "Ensure the psoct_toolbox package is installed and "
            "get_matlab_root() is available."
        ) from exc


def resolve_matlab_root(override: Optional[str] = None) -> str:
    """
    Resolve the MATLAB toolbox root, using an explicit override when provided.

    Parameters
    ----------
    override :
        Optional explicit filesystem path. When provided, it is used directly;
        otherwise the path is obtained from psoct_toolbox.get_matlab_root().
    """
    if override:
        return str(Path(override).expanduser())
    return get_default_matlab_root()


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

    # Resolve MATLAB script path (explicit override or psoct_toolbox default)
    resolved_path = resolve_matlab_root(matlab_script_path)
    path_cmd = f"addpath(genpath('{resolved_path}')); "
    # Also add registration subdirectory when present
    registration_path = Path(resolved_path) / "registration"
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
    matlab_script_path: Optional[str] = None,
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
    resolved_path = resolve_matlab_root(matlab_script_path)
    matlab_cmd = f"addpath(genpath('{resolved_path}'));{command}"

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
        if matlab_process.return_code != 0:
            logger.error(f"MATLAB command failed: {output}")
            raise RuntimeError(f"MATLAB command failed: {output}")
        logger.info(f"MATLAB execution output: {output}")
