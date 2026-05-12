"""Hub settings model with JSON file persistence."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from platformdirs import user_config_dir
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(user_config_dir("opticstream"))
_DEFAULT_PATH = _CONFIG_DIR / "optichub.json"


def default_settings_path() -> Path:
    return _DEFAULT_PATH


def _apply_redis_url_env(settings: HubSettings) -> HubSettings:
    """Override redis_url from REDIS_URL when set (runtime only; not persisted)."""
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return settings
    return HubSettings.model_validate({**settings.model_dump(), "redis_url": url})


class HubSettings(BaseModel):
    """Runtime configuration for the optic hub (Streamlit)."""

    redis_url: str = Field(default="redis://127.0.0.1:6379/0")
    prefect_server_url: str = Field(
        default="http://127.0.0.1/",
        description="Prefect API/UI base URL (reserved for future hub features)",
    )
    grpc_ping_timeout_ms: int = Field(default=500, ge=50, le=30_000)
    online_grace_s: float = Field(default=5.0, gt=0)
    dashboard_refresh_s: int = Field(default=2, ge=1, le=60)

    @classmethod
    def load_disk(cls, path: Path | None = None) -> HubSettings:
        """Load settings from JSON only (no ``REDIS_URL`` env override)."""
        p = path or _DEFAULT_PATH
        if p.is_file():
            logger.info("Loading hub settings from %s", p)
            raw = p.read_text(encoding="utf-8")
            return cls.model_validate_json(raw)
        logger.info("No hub settings file at %s; creating default.", p)
        settings = cls()
        settings.save(p)
        return settings

    @classmethod
    def load(cls, path: Path | None = None) -> HubSettings:
        """Load from JSON, then apply ``REDIS_URL`` when set."""
        return _apply_redis_url_env(cls.load_disk(path))

    def save(self, path: Path | None = None) -> Path:
        """Persist settings to a JSON file, creating parent dirs as needed."""
        p = path or _DEFAULT_PATH
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(self.model_dump(mode="json"), indent=2) + "\n",
            encoding="utf-8",
        )
        logger.info("Hub settings saved to %s", p)
        return p
