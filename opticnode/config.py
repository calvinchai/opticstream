"""Load `.env` and environment variables into a single settings object."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


def load_dotenv(path: Path | None = None) -> None:
    """Merge key=value lines from a `.env` file into `os.environ` (no override)."""
    env_path = path or Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        return
    for raw in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


class Settings(BaseModel):
    """Runtime configuration for the optic node."""

    node_id: str = Field(default="opticnode-1", description="Unique node identity for registration")
    redis_url: str
    grpc_host: str = Field(default="[::]")
    grpc_port: int = Field(default=50051, ge=1, le=65535)
    heartbeat_interval_s: float = Field(default=1.0, gt=0)
    heartbeat_ttl_s: int = Field(default=10, ge=1, description="Redis TTL for last_seen (seconds)")
    log_buffer_size: int = Field(default=100, ge=1, le=10_000)
    primocache_exe: str = Field(default="rxpcc.exe", description="PrimoCache CLI executable name")
    gui_mode: bool = Field(default=False, description="Enable Tkinter log viewer + system tray")
    mgmt_iface: str | None = Field(default=None, description="Optional override for management NIC name")
    data_iface: str | None = Field(default=None, description="Optional override for data NIC name")
    advertised_host: str | None = Field(
        default=None,
        description="IPv4/hostname hub uses to reach this node; empty uses management plane IP",
    )
    github_repo: str = Field(
        default="",
        description="owner/repo for GitHub Releases update checks (e.g. myorg/opticstream)",
    )
    auto_update: bool = Field(default=True, description="Poll GitHub and apply Windows .exe updates")
    update_check_interval_s: int = Field(default=3600, ge=60, description="Seconds between update checks")
    updater_asset_pattern: str = Field(
        default="opticnode*.exe",
        description="Glob-style match for release asset name (fnmatch)",
    )
    env_file: Path | None = Field(default=None, description="Optional override path for `.env`")

    @classmethod
    def from_env(cls, *, env_file: Path | None = None) -> Settings:
        load_dotenv(env_file)
        adv = os.environ.get("ADVERTISED_HOST", "").strip()
        mgmt = os.environ.get("MGMT_IFACE", "").strip()
        data = os.environ.get("DATA_IFACE", "").strip()
        gui_raw = os.environ.get("GUI_MODE", "false").strip().lower()
        gui_mode = gui_raw in ("1", "true", "yes", "on")
        auto_raw = os.environ.get("AUTO_UPDATE", "true").strip().lower()
        auto_update = auto_raw in ("1", "true", "yes", "on")
        pat = os.environ.get("UPDATER_ASSET_PATTERN", "opticnode*.exe").strip() or "opticnode*.exe"
        return cls(
            node_id=os.environ.get("NODE_ID", "opticnode-1"),
            redis_url=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
            grpc_host=os.environ.get("GRPC_HOST", "[::]"),
            grpc_port=int(os.environ.get("GRPC_PORT", "50051")),
            heartbeat_interval_s=float(os.environ.get("HEARTBEAT_INTERVAL_S", "1.0")),
            heartbeat_ttl_s=int(os.environ.get("HEARTBEAT_TTL_S", "10")),
            log_buffer_size=int(os.environ.get("LOG_BUFFER_SIZE", "100")),
            primocache_exe=os.environ.get("PRIMOCACHE_EXE", "rxpcc.exe").strip() or "rxpcc.exe",
            gui_mode=gui_mode,
            mgmt_iface=mgmt or None,
            data_iface=data or None,
            advertised_host=adv or None,
            github_repo=os.environ.get("GITHUB_REPO", "").strip(),
            auto_update=auto_update,
            update_check_interval_s=int(os.environ.get("UPDATE_CHECK_INTERVAL_S", "3600")),
            updater_asset_pattern=pat,
            env_file=env_file,
        )
