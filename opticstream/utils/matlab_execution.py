"""
MATLAB batch command execution utilities.

Centralizes MATLAB subprocess execution with consistent error handling.
Includes MATLAB engine initialization and command-line fallback utilities.
"""

from numbers import Number
from pathlib import Path
from typing import Any, Optional

from prefect.logging import get_run_logger
from prefect_shell import ShellOperation

import psoct_toolbox


def _matlab_literal(value: Any) -> str:
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
        return "[" + ", ".join(_matlab_literal(v) for v in value) + "]"
    raise TypeError(f"Unsupported value type for MATLAB literal: {type(value)}")


def dict_to_matlab_literal(value: dict[str, Any]) -> str:
    """
    Convert a nested dict into a MATLAB struct literal.
    """
    if not value:
        return "struct()"
    fields: list[str] = []
    for key, field_value in value.items():
        key_lit = _matlab_literal(str(key))
        value_lit = _matlab_literal(field_value)
        fields.append(f"{key_lit}, {value_lit}")
    return f"struct({', '.join(fields)})"


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
    try:
        return psoct_toolbox.get_matlab_root()
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError(
            "Failed to determine MATLAB toolbox root from psoct_toolbox. "
            "Ensure the psoct_toolbox package is installed and "
            "get_matlab_root() is available."
        ) from exc


def _is_matlab_engine_available() -> bool:
    """
    Return True if the MATLAB Engine for Python package is importable.
    """
    try:
        import matlab.engine  # noqa: F401

        return True
    except ImportError:
        return False


def run_matlab_function_or_cli(
    function_name: str,
    *args: Any,
    matlab_script_path: Optional[str] = None,
    nargout: int = 0,
) -> Optional[Any]:
    """
    Run a MATLAB function via the MATLAB Engine for Python, or fall back to CLI.

    When the MATLAB Engine package is not installed, calls ``matlab -batch``
    with the same function name and arguments. Toolbox paths are resolved via
    ``addpath(genpath(root))`` in both paths.

    Parameters
    ----------
    function_name
        MATLAB function name.
    *args
        Positional arguments passed to the MATLAB function.
    matlab_script_path
        Optional toolbox root override; passed to :func:`resolve_matlab_root`.
    nargout
        Number of MATLAB outputs to return from the engine. When ``nargout > 0``,
        the CLI fallback cannot return values and raises :class:`RuntimeError`.

    Returns
    -------
    Any or None
        Engine return value(s) when ``nargout > 0``; otherwise ``None``.
    """
    logger = get_run_logger()
    resolved_path = resolve_matlab_root(matlab_script_path)

    if _is_matlab_engine_available():
        import matlab.engine

        eng = matlab.engine.start_matlab()
        eng.eval(f"addpath(genpath('{resolved_path}'));", nargout=0)
        logger.info(f"MATLAB engine started; toolbox path added: {resolved_path}")
        try:
            fn = getattr(eng, function_name)
            result = fn(*args, nargout=nargout)
            logger.info(f"{function_name} completed successfully via MATLAB engine")
            return result
        finally:
            eng.quit()

    if nargout != 0:
        raise RuntimeError(
            f"MATLAB Engine is required for {function_name} with nargout={nargout}; "
            "CLI fallback does not return MATLAB outputs."
        )

    logger.warning("MATLAB Engine not available, using CLI")
    args_str = ", ".join(_matlab_literal(a) for a in args)
    matlab_cmd = f"addpath(genpath('{resolved_path}')); {function_name}({args_str})"
    escaped_cmd = matlab_cmd.replace('"', '\\"')
    logger.info(f"Executing MATLAB CLI: {function_name}({args_str})")
    logger.debug(f"Full MATLAB command string: {matlab_cmd}")
    with ShellOperation(
        commands=[f'matlab -batch "{escaped_cmd}"'],
    ) as op:
        proc = op.trigger()
        proc.wait_for_completion()
        if proc.return_code != 0:
            raise RuntimeError(
                f"MATLAB CLI command failed for {function_name}: {proc.fetch_result()}"
            )
        logger.info(f"MATLAB output: {proc.fetch_result()}")
    return None


def run_matlab_batch_command_or_cli(
    command: str,
    matlab_script_path: Optional[str] = None,
    working_dir: Optional[str] = None,
) -> None:
    """
    Run a MATLAB batch command via the Engine when available, else ``matlab -batch``.

    Uses ``addpath(genpath(root));`` as preamble in both the engine and CLI paths.

    Parameters
    ----------
    command
        MATLAB command string to execute (e.g. ``"spectral2complex_batch(...)"``).
    matlab_script_path
        Optional toolbox root override; passed to :func:`resolve_matlab_root`.
    working_dir
        Optional working directory for the CLI subprocess.
    """
    logger = get_run_logger()
    resolved_path = resolve_matlab_root(matlab_script_path)
    matlab_cmd = f"addpath(genpath('{resolved_path}'));{command}"

    if _is_matlab_engine_available():
        import matlab.engine

        eng = matlab.engine.start_matlab()
        logger.info(f"MATLAB engine started; toolbox path added: {resolved_path}")
        try:
            eng.eval(matlab_cmd, nargout=0)
            logger.info("MATLAB batch command completed via MATLAB engine")
        finally:
            eng.quit()
        return

    logger.warning("MATLAB Engine not available, using CLI batch")
    escaped_cmd = matlab_cmd.replace('"', '\\"')
    logger.info(f"Executing MATLAB command: {command}")
    logger.debug(f"Full MATLAB command: matlab -batch \"{escaped_cmd}\"")
    with ShellOperation(
        commands=[f'matlab -batch "{escaped_cmd}"'],
        working_dir=working_dir,
    ) as op:
        proc = op.trigger()
        proc.wait_for_completion()
        if proc.return_code != 0:
            raise RuntimeError(f"MATLAB command failed: {proc.fetch_result()}")
        logger.info(f"MATLAB execution output: {proc.fetch_result()}")
