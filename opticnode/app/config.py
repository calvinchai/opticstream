"""Settings model with JSON file persistence."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from platformdirs import user_config_dir
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(user_config_dir("opticstream"))
_DEFAULT_PATH = _CONFIG_DIR / "opticnode.json"


def default_settings_path() -> Path:
    return _DEFAULT_PATH


class Settings(BaseModel):
    """Runtime configuration for the optic node."""

    node_id: str = Field(default="opticnode-1", description="Unique node identity for registration")
    redis_url: str = Field(default="redis://127.0.0.1:6379/0")
    grpc_host: str = Field(default="[::]")
    grpc_port: int = Field(default=50051, ge=1, le=65535)
    heartbeat_interval_s: float = Field(default=1.0, gt=0)
    heartbeat_ttl_s: int = Field(default=10, ge=1, description="Redis TTL for last_seen (seconds)")
    redis_log_tail: int = Field(default=100, ge=1, le=10_000, description="Lines per module kept in Redis when node is down")
    log_dir: Path = Field(default=Path("logs"), description="Directory for per-module rotating log files")
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

    @classmethod
    def load(cls, path: Path | None = None) -> Settings:
        """Load settings from a JSON file, falling back to model defaults."""
        p = path or _DEFAULT_PATH
        if p.is_file():
            logger.info("Loading settings from %s", p)
            raw = p.read_text(encoding="utf-8")
            return cls.model_validate_json(raw)
        logger.info("No settings file at %s; creating default.", p)
        settings = cls()
        settings.save(p)
        return settings

    def save(self, path: Path | None = None) -> Path:
        """Persist settings to a JSON file, creating parent dirs as needed."""
        p = path or _DEFAULT_PATH
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(self.model_dump(mode="json"), indent=2) + "\n",
            encoding="utf-8",
        )
        logger.info("Settings saved to %s", p)
        return p


__all__ = ["Settings", "default_settings_path"]
