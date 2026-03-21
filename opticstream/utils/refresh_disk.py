from __future__ import annotations

import logging
import subprocess
from collections.abc import Callable

logger = logging.getLogger(__name__)

RefreshHook = Callable[[], None]


def noop_refresh() -> None:
    """
    Refresh hook that intentionally does nothing.
    """
    logger.info("Refresh hook 'none': skipping refresh")


def sas_refresh() -> None:
    """
    Refresh hook for the sas2 mount by unmounting and remounting it.
    """
    try:
        subprocess.run(["bash", "/usr/etc/sas_unmount"], check=True)
        subprocess.run(["bash", "/usr/etc/sas_mount"], check=True)
        logger.info("Refresh hook 'sas' completed")
    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
        logger.warning("Refresh hook 'sas' failed: %s. Continuing.", exc)


def sas2_refresh() -> None:
    """
    Refresh hook for the sas2 mount by unmounting and remounting it.
    """
    try:
        subprocess.run(["bash", "/usr/etc/sas2_unmount"], check=True)
        subprocess.run(["bash", "/usr/etc/sas2_mount"], check=True)
        logger.info("Refresh hook 'sas2' completed")
    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
        logger.warning("Refresh hook 'sas2' failed: %s. Continuing.", exc)

def sas3_refresh() -> None:
    """
    Refresh hook for the sas3 mount by unmounting and remounting it.
    """
    try:
        subprocess.run(["bash", "/usr/etc/sas3_unmount"], check=True)
        subprocess.run(["bash", "/usr/etc/sas3_mount"], check=True)
        logger.info("Refresh hook 'sas3' completed")
    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
        logger.warning("Refresh hook 'sas3' failed: %s. Continuing.", exc)


_REFRESH_HOOKS: dict[str, RefreshHook] = {
    "none": noop_refresh,
    "sas": sas_refresh,
    "sas2": sas2_refresh,
    "sas3": sas3_refresh,
}


def available_refresh_hooks() -> tuple[str, ...]:
    """
    Return the sorted list of registered refresh hook names.
    """
    return tuple(sorted(_REFRESH_HOOKS))


def resolve_refresh_hook(name: str | None) -> RefreshHook | None:
    """
    Resolve a refresh hook by name.

    Parameters
    ----------
    name:
        Hook name such as ``'sas2'`` or ``'none'``.
        ``None`` or empty string returns ``None``.

    Returns
    -------
    RefreshHook | None
        The resolved callable, or ``None`` when no refresh should be used.

    Raises
    ------
    ValueError
        If the hook name is unknown.
    """
    if name is None:
        return None

    normalized = name.strip().lower()
    if normalized == "":
        return None

    hook = _REFRESH_HOOKS.get(normalized)
    if hook is not None:
        return hook

    valid = ", ".join(available_refresh_hooks())
    raise ValueError(f"Unknown refresh hook {name!r}. Valid options: {valid}")


def register_refresh_hook(name: str, hook: RefreshHook, *, overwrite: bool = False) -> None:
    """
    Register a custom refresh hook.

    Parameters
    ----------
    name:
        Unique hook name.
    hook:
        Zero-argument callable to execute after direct processing.
    overwrite:
        If False, raising on duplicate names. If True, replaces existing hook.
    """
    normalized = name.strip().lower()
    if normalized == "":
        raise ValueError("Refresh hook name must not be empty")

    if not overwrite and normalized in _REFRESH_HOOKS:
        raise ValueError(f"Refresh hook {name!r} is already registered")

    _REFRESH_HOOKS[normalized] = hook