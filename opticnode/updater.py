"""GitHub Releases based auto-update for the frozen Windows OpticNode .exe."""

from __future__ import annotations

import fnmatch
import logging
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from packaging.version import InvalidVersion, Version

from . import __version__
from .redis_utils import make_redis_client
from .config import Settings

logger = logging.getLogger(__name__)

_GH_LATEST = "https://api.github.com/repos/{repo}/releases/latest"
_TAG_VERSION_RE = re.compile(r"^v?(\d+.*)$", re.IGNORECASE)


def _parse_version_from_tag(tag: str) -> Version | None:
    tag = (tag or "").strip()
    m = _TAG_VERSION_RE.match(tag)
    if not m:
        return None
    try:
        return Version(m.group(1))
    except InvalidVersion:
        return None


def _current_version() -> Version:
    try:
        return Version(__version__)
    except InvalidVersion:
        return Version("0.0.0")


def _get_frozen_paths() -> tuple[Path, Path] | None:
    """Return (dir containing the running exe, path to the exe) or None if not frozen."""
    if not getattr(sys, "frozen", False):
        return None
    exe = Path(sys.executable).resolve()
    return exe.parent, exe


def _fetch_latest_release(repo: str, timeout_s: float = 30.0) -> dict[str, Any] | None:
    url = _GH_LATEST.format(repo=repo.strip().strip("/"))
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    r = requests.get(url, headers=headers, timeout=timeout_s)
    if r.status_code == 404:
        logger.warning("No GitHub release found for %s", repo)
        return None
    r.raise_for_status()
    return r.json()


def _pick_asset(assets: list[dict[str, Any]], pattern: str) -> dict[str, Any] | None:
    for a in assets:
        name = a.get("name") or ""
        if fnmatch.fnmatch(name, pattern) and name.lower().endswith(".exe"):
            return a
    return None


def _persist_update_status(settings: Settings, mapping: dict[str, str]) -> None:
    r = make_redis_client(settings.redis_url)
    if r is None:
        return
    key = f"opticnode:{settings.node_id}:update"
    try:
        r.hset(key, mapping=mapping)
    except Exception:
        logger.exception("Failed to publish update status to Redis")


def download_asset(url: str, dest: Path, *, timeout_s: float = 600.0) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".partial")
    with requests.get(url, stream=True, timeout=60.0) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
    tmp.replace(dest)


def apply_windows_update(new_exe: Path, running_exe: Path) -> None:
    """Write helper .bat alongside the downloaded exe; bat replaces running exe and restarts."""
    bat = new_exe.parent / "apply_update.bat"
    rel_run = running_exe.name
    bat_text = (
        "@echo off\r\n"
        "timeout /t 2 /nobreak >nul\r\n"
        f'move /y "%~dp0{new_exe.name}" "%~dp0..\\{rel_run}"\r\n'
        f'start "" "%~dp0..\\{rel_run}"\r\n'
        'del "%~f0"\r\n'
    )
    bat.write_text(bat_text, encoding="utf-8")
    subprocess.Popen(["cmd", "/c", str(bat.resolve())], close_fds=True)  # noqa: S603
    time.sleep(0.3)
    os._exit(0)  # noqa: S404


def check_update_once(settings: Settings) -> str:
    """
    One synchronous check. Returns a short human-readable result.
    Does not apply updates.
    """
    if not settings.github_repo:
        return "Updater: GITHUB_REPO is not set."
    data = _fetch_latest_release(settings.github_repo)
    if not data:
        return "Updater: no release data."
    tag = data.get("tag_name") or ""
    latest_v = _parse_version_from_tag(tag)
    if latest_v is None:
        return f"Updater: could not parse version from tag {tag!r}."
    cur = _current_version()
    if latest_v <= cur:
        return f"Updater: up to date (current {cur}, latest {latest_v})."
    asset = _pick_asset(data.get("assets") or [], settings.updater_asset_pattern)
    if not asset:
        return f"Updater: newer release {tag} exists but no asset matching {settings.updater_asset_pattern!r}."
    return f"Updater: update available {cur} -> {latest_v} ({tag}), asset {asset.get('name')}."


class UpdateChecker:
    """Background loop: poll GitHub, download + replace when newer (frozen Windows only)."""

    def __init__(self, settings: Settings, stop_event: threading.Event) -> None:
        self._settings = settings
        self._stop = stop_event

    def run(self) -> None:
        interval = float(self._settings.update_check_interval_s)
        while not self._stop.is_set():
            try:
                self._check_and_maybe_apply()
            except Exception:
                logger.exception("Update check failed")
                _persist_update_status(
                    self._settings,
                    {
                        "latest": "",
                        "status": "error",
                        "message": "check failed (see logs)",
                        "checked_at": str(time.time()),
                    },
                )
            if self._stop.wait(timeout=interval):
                break

    def _check_and_maybe_apply(self) -> None:
        s = self._settings
        if not s.github_repo:
            return
        data = _fetch_latest_release(s.github_repo)
        if not data:
            _persist_update_status(
                s,
                {
                    "latest": "",
                    "status": "error",
                    "message": "no release",
                    "checked_at": str(time.time()),
                },
            )
            return
        tag = data.get("tag_name") or ""
        html_url = data.get("html_url") or ""
        latest_v = _parse_version_from_tag(tag)
        cur = _current_version()
        _persist_update_status(
            s,
            {
                "latest": tag,
                "latest_version": str(latest_v) if latest_v else "",
                "current_version": str(cur),
                "status": "up-to-date" if latest_v and latest_v <= cur else "available",
                "release_url": html_url[:500],
                "checked_at": str(time.time()),
            },
        )
        if latest_v is None or latest_v <= cur:
            return

        asset = _pick_asset(data.get("assets") or [], s.updater_asset_pattern)
        if not asset:
            logger.warning("Update available %s but no asset for pattern %s", tag, s.updater_asset_pattern)
            _persist_update_status(
                s,
                {
                    "latest": tag,
                    "status": "error",
                    "message": "no matching .exe asset",
                    "checked_at": str(time.time()),
                },
            )
            return

        url = asset.get("browser_download_url")
        if not url:
            return

        paths = _get_frozen_paths()
        if paths is None:
            logger.info(
                "Update available (%s -> %s); not frozen — restart from source or install .exe to apply.",
                cur,
                latest_v,
            )
            _persist_update_status(
                s,
                {
                    "latest": tag,
                    "status": "available",
                    "message": "not frozen; manual update required",
                    "checked_at": str(time.time()),
                },
            )
            return

        exe_dir, running_exe = paths
        if sys.platform != "win32":
            logger.info("Update available; automatic replace is only implemented on Windows.")
            _persist_update_status(
                s,
                {
                    "latest": tag,
                    "status": "available",
                    "message": "non-Windows; manual update",
                    "checked_at": str(time.time()),
                },
            )
            return

        _persist_update_status(
            s,
            {
                "latest": tag,
                "status": "applying",
                "message": "downloading",
                "checked_at": str(time.time()),
            },
        )

        update_root = exe_dir / "_update"
        update_root.mkdir(parents=True, exist_ok=True)
        dest = update_root / "opticnode-new.exe"
        try:
            logger.info("Downloading update from %s", urlparse(url).path)
            download_asset(url, dest)
        except Exception:
            logger.exception("Download failed")
            _persist_update_status(
                s,
                {
                    "latest": tag,
                    "status": "error",
                    "message": "download failed",
                    "checked_at": str(time.time()),
                },
            )
            return

        logger.info("Applying update and restarting...")
        apply_windows_update(dest, running_exe)
