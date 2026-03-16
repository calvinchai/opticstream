"""
Pydantic models for LSM strip state and project-level state file.

Strip state is persisted as a single JSON file per project (e.g. under
scan_config.output_path) and supports load/save from file. Each strip tracks
processing state plus boolean flags: uploaded, backed_up, processed.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class LSMStripProcessingState(str, Enum):
    """Processing state of a single strip (stable JSON values)."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class LSMStripState(BaseModel):
    """
    State of a single LSM strip.

    Tracks processing state and independent boolean flags (uploaded, backed_up,
    processed) so transitions are not one-way; e.g. a strip can be running
    upload while not yet backed up.
    """

    slice_id: int = Field(..., ge=0)
    strip_id: int = Field(..., ge=0)
    channel_id: int = Field(..., ge=0)
    state: LSMStripProcessingState = LSMStripProcessingState.PENDING
    timestamp: datetime = Field(default_factory=datetime.now)
    uploaded: bool = False
    backed_up: bool = False
    processed: bool = False

    def mark_started(self) -> None:
        self.state = LSMStripProcessingState.RUNNING
        self.timestamp = datetime.now()

    def mark_completed(self) -> None:
        self.state = LSMStripProcessingState.COMPLETED
        self.timestamp = datetime.now()
        self.processed = True

    def mark_failed(self) -> None:
        self.state = LSMStripProcessingState.FAILED
        self.timestamp = datetime.now()


class LSMStripStateFile(BaseModel):
    """
    Project-level container for all strip states, persisted as one JSON file.

    File location is typically: ``{scan_config.output_path}/{project_name}_lsm_strip_state.json``.
    Use load_from_path / save_to_path for file I/O.
    """

    project_name: str = ""
    version: int = 1
    strips: dict[str, LSMStripState] = Field(default_factory=dict)

    @staticmethod
    def make_key(slice_id: int, strip_id: int, channel_id: int) -> str:
        return f"{slice_id}:{strip_id}:{channel_id}"

    @classmethod
    def load_from_path(
        cls, path: Path, project_name: str | None = None
    ) -> LSMStripStateFile:
        """Load state from JSON file; return new empty instance if file does not exist.
        When creating a new instance, pass project_name so the saved file is tagged."""
        if not path.exists():
            return cls(project_name=project_name or "")
        text = path.read_text()
        return cls.model_validate_json(text)

    def save_to_path(self, path: Path) -> None:
        """Write state to JSON file; create parent directories if needed."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2))

    def upsert_strip(self, strip_state: LSMStripState) -> None:
        """Insert or update strip state by key (slice_id, strip_id, channel_id)."""
        key = self.make_key(strip_state.slice_id, strip_state.strip_id, strip_state.channel_id)
        self.strips[key] = strip_state

    def get_strip(
        self, slice_id: int, strip_id: int, channel_id: int
    ) -> LSMStripState | None:
        """Return strip state if present."""
        key = self.make_key(slice_id, strip_id, channel_id)
        return self.strips.get(key)

    def all_strips(self) -> list[LSMStripState]:
        """Return all strip states (for introspection / dashboards)."""
        return list(self.strips.values())
