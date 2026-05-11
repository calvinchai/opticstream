"""Hub settings model with JSON file persistence."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from platformdirs import user_config_dir
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(user_config_dir("opticstream"))
_DEFAULT_PATH = _CONFIG_DIR / "optichub.json"


def default_settings_path() -> Path:
    return _DEFAULT_PATH


class HubSettings(BaseModel):
    """Runtime configuration for the optic hub (Streamlit)."""

    redis_url: str = Field(default="redis://127.0.0.1:6379/0")
    grpc_ping_timeout_ms: int = Field(default=500, ge=50, le=30_000)
    online_grace_s: float = Field(default=5.0, gt=0)
    dashboard_refresh_s: int = Field(default=2, ge=1, le=60)

    @classmethod
    def load(cls, path: Path | None = None) -> HubSettings:
        """Load settings from a JSON file, falling back to model defaults."""
        p = path or _DEFAULT_PATH
        if p.is_file():
            logger.info("Loading hub settings from %s", p)
            raw = p.read_text(encoding="utf-8")
            return cls.model_validate_json(raw)
        logger.info("No hub settings file at %s; using defaults.", p)
        return cls()

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
