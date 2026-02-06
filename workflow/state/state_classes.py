"""
State class definitions for tracking processing state.

This module contains classes for managing state at different levels:
- BatchState: Individual batch state
- MosaicState: Mosaic-level state (contains multiple batches)
- SliceState: Slice-level state (contains two mosaics)
- ProjectState: Project-level state (contains all slices)

All state classes recover state from flag files.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from workflow.config.blocks import PSOCTScanConfig
from workflow.config.project_config import get_grid_size_x, get_project_config_block
from workflow.state.flags import (
    ARCHIVED,
    PROCESSED,
    REGISTERED,
    STARTED,
    STITCHED,
    UPLOADED,
    VOLUME_STITCHED,
    VOLUME_UPLOADED,
    get_batch_flag_path,
    get_mosaic_flag_path,
    get_slice_flag_path,
)
from workflow.utils.utils import (
    get_mosaic_paths,
    get_slice_paths,
    mosaic_id_to_slice_number,
)


class BaseState(ABC):
    """
    Abstract base class for all state classes.

    Provides common interface for serialization and state refresh operations.
    """

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert state to dictionary format for serialization.

        Returns
        -------
        Dict[str, Any]
            Dictionary representation of the state
        """
        pass

    @abstractmethod
    def refresh(self):
        """
        Refresh state by re-reading from authoritative source (flag files).
        """
        pass


@dataclass
class BatchState(BaseState):
    """
    Represents the state of a single batch, recovered from flag files.
    """

    batch_id: int
    state_path: Path
    started: bool = field(init=False)
    archived: bool = field(init=False)
    processed: bool = field(init=False)
    uploaded: bool = field(init=False)

    def __post_init__(self):
        """
        Initialize batch state from flag files.
        """
        # Recover batch state from flag files
        self._recover_state()

    def _recover_state(self):
        """Recover batch state by checking flag files."""
        self.started = get_batch_flag_path(
            self.state_path, self.batch_id, STARTED
        ).exists()
        self.archived = get_batch_flag_path(
            self.state_path, self.batch_id, ARCHIVED
        ).exists()
        self.processed = get_batch_flag_path(
            self.state_path, self.batch_id, PROCESSED
        ).exists()
        self.uploaded = get_batch_flag_path(
            self.state_path, self.batch_id, UPLOADED
        ).exists()

    def refresh(self):
        """Refresh state by re-reading flag files."""
        self._recover_state()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for serialization."""
        return {
            "batch_id": self.batch_id,
            "started": self.started,
            "archived": self.archived,
            "processed": self.processed,
            "uploaded": self.uploaded,
        }


@dataclass
class MosaicState(BaseState):
    """
    Represents the state of a mosaic, recovered from flag files.

    Total batches is determined from grid_size_x parameter or project configuration block.
    """

    project_base_path: str
    mosaic_id: int
    project_name: Optional[str] = None
    grid_size_x: Optional[int] = None
    state_path: Path = field(init=False)
    total_batches: int = field(init=False)
    started_batches: int = field(init=False, default=0)
    archived_batches: int = field(init=False, default=0)
    processed_batches: int = field(init=False, default=0)
    uploaded_batches: int = field(init=False, default=0)
    batch_states: Dict[int, BatchState] = field(init=False, default_factory=dict)
    started: bool = field(init=False, default=False)
    stitched: bool = field(init=False, default=False)
    volume_stitched: bool = field(init=False, default=False)
    volume_uploaded: bool = field(init=False, default=False)

    def __post_init__(self):
        """
        Initialize mosaic state from flag files.
        """
        # Get state path
        _, _, _, self.state_path = get_mosaic_paths(
            self.project_base_path, self.mosaic_id
        )

        # Determine total batches from grid_size_x parameter or fallback methods
        if self.grid_size_x is not None:
            self.total_batches = self.grid_size_x
        else:
            self._get_grid_size_from_block_or_flags()
        # Recover batch state from flag files
        self._recover_batch_state()
        # Recover mosaic-level flags
        self._recover_mosaic_flags()

    def _get_grid_size_from_block_or_flags(self):
        """
        Get grid_size_x from project configuration block or count from flags.

        This is a fallback method when grid_size_x is not provided as a parameter.
        """
        if self.project_name:
            try:
                # Get grid size from project configuration block
                grid_size_x = get_grid_size_x(self.project_name, self.mosaic_id)
                if grid_size_x is not None:
                    self.total_batches = int(grid_size_x)
                else:
                    # If grid_size_x is None, try to count from flag files as fallback
                    self.total_batches = self._count_batches_from_flags()
            except (ValueError, Exception):
                # If getting from block fails, count from flag files as fallback
                self.total_batches = self._count_batches_from_flags()
        else:
            # No project_name provided, count from flag files as fallback
            self.total_batches = self._count_batches_from_flags()

    @classmethod
    def from_flags(
        cls,
        project_base_path: str,
        mosaic_id: int,
        project_name: Optional[str] = None,
        grid_size_x: Optional[int] = None,
    ) -> "MosaicState":
        """
        Factory method to create MosaicState from flag files.

        Parameters
        ----------
        project_base_path : str
            Base path for the project
        mosaic_id : int
            Mosaic identifier
        project_name : str, optional
            Project name for retrieving grid size from config block
        grid_size_x : int, optional
            Grid size x (number of batches). If not provided, will be retrieved from block or flags.

        Returns
        -------
        MosaicState
            MosaicState instance initialized from flag files
        """
        return cls(
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            project_name=project_name,
            grid_size_x=grid_size_x,
        )

    @classmethod
    def from_block(
        cls,
        project_base_path: str,
        mosaic_id: int,
        project_name: str,
    ) -> "MosaicState":
        """
        Factory method to create MosaicState with grid size from config block.

        Parameters
        ----------
        project_base_path : str
            Base path for the project
        mosaic_id : int
            Mosaic identifier
        project_name : str
            Project name for retrieving grid size from config block

        Returns
        -------
        MosaicState
            MosaicState instance with grid size from config block
        """
        grid_size_x = get_grid_size_x(project_name, mosaic_id)
        return cls(
            project_base_path=project_base_path,
            mosaic_id=mosaic_id,
            project_name=project_name,
            grid_size_x=grid_size_x,
        )

    def _count_batches_from_flags(self) -> int:
        """Count total batches from flag files (fallback method)."""
        if not self.state_path.exists():
            return 0

        # Count unique batch IDs from all flag files
        # Handle both old format (batch-0.started) and new format (batch-000.started)
        all_batch_files = list(self.state_path.glob("batch-*.*"))
        batch_ids = set()
        for file in all_batch_files:
            # Extract batch ID from filename like "batch-000.started" or "batch-0.started"
            try:
                # Remove "batch-" prefix and split by "."
                name_part = file.stem  # e.g., "batch-000" or "batch-0"
                batch_id_str = name_part.split("-")[1]
                batch_id = int(batch_id_str)
                batch_ids.add(batch_id)
            except (ValueError, IndexError):
                continue

        return len(batch_ids) if batch_ids else 0

    def _recover_batch_state(self):
        """Recover batch state by scanning flag files."""
        # Initialize batch states dictionary
        self.batch_states = {}

        if not self.state_path.exists():
            self.started_batches = 0
            self.archived_batches = 0
            self.processed_batches = 0
            self.uploaded_batches = 0
            return

        # Create BatchState objects for each batch (0 to total_batches-1)
        started_count = 0
        archived_count = 0
        processed_count = 0
        uploaded_count = 0

        for batch_id in range(1, self.total_batches + 1):
            batch_state = BatchState(batch_id, self.state_path)
            self.batch_states[batch_id] = batch_state

            if batch_state.started:
                started_count += 1
            if batch_state.archived:
                archived_count += 1
            if batch_state.processed:
                processed_count += 1
            if batch_state.uploaded:
                uploaded_count += 1

        self.started_batches = started_count
        self.archived_batches = archived_count
        self.processed_batches = processed_count
        self.uploaded_batches = uploaded_count

    def _recover_mosaic_flags(self):
        """Recover mosaic-level flag files."""
        if not self.state_path.exists():
            self.started = False
            self.stitched = False
            self.volume_stitched = False
            self.volume_uploaded = False
            return

        self.started = get_mosaic_flag_path(
            self.state_path, self.mosaic_id, STARTED
        ).exists()
        self.stitched = get_mosaic_flag_path(
            self.state_path, self.mosaic_id, STITCHED
        ).exists()
        self.volume_stitched = get_mosaic_flag_path(
            self.state_path, self.mosaic_id, VOLUME_STITCHED
        ).exists()
        self.volume_uploaded = get_mosaic_flag_path(
            self.state_path, self.mosaic_id, VOLUME_UPLOADED
        ).exists()

    def is_complete(self) -> bool:
        """Check if all batches are processed."""
        if self.total_batches == 0:
            return False
        return self.processed_batches >= self.total_batches

    def get_progress_percentage(self) -> float:
        """Get processing progress percentage."""
        if self.total_batches == 0:
            return 0.0
        return (self.processed_batches / self.total_batches) * 100

    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "total_batches": self.total_batches,
            "started_batches": self.started_batches,
            "archived_batches": self.archived_batches,
            "processed_batches": self.processed_batches,
            "uploaded_batches": self.uploaded_batches,
        }

    def to_dict_full(self) -> Dict[str, Any]:
        """
        Convert to full dictionary format including batch states for serialization.

        Returns
        -------
        Dict[str, Any]
            Complete state dictionary suitable for JSON serialization
        """
        batch_states_dict = {}
        for batch_id, batch_state in self.batch_states.items():
            batch_states_dict[str(batch_id)] = batch_state.to_dict()

        return {
            "mosaic_id": self.mosaic_id,
            "total_batches": self.total_batches,
            "started_batches": self.started_batches,
            "archived_batches": self.archived_batches,
            "processed_batches": self.processed_batches,
            "uploaded_batches": self.uploaded_batches,
            "batch_states": batch_states_dict,
            "started": self.started,
            "stitched": self.stitched,
            "volume_stitched": self.volume_stitched,
            "volume_uploaded": self.volume_uploaded,
        }

    def get_batch_state(self, batch_id: int) -> Optional[BatchState]:
        """
        Get state for a specific batch.

        Parameters
        ----------
        batch_id : int
            Batch identifier

        Returns
        -------
        BatchState, optional
            Batch state object, or None if batch_id is out of range
        """
        if batch_id < 1 or batch_id > self.total_batches:
            return None
        return self.batch_states.get(batch_id)

    def refresh(self):
        """
        Refresh state by re-reading flag files.
        """
        self._recover_batch_state()
        self._recover_mosaic_flags()


@dataclass
class SliceState(BaseState):
    """
    Represents the state of a slice, recovered from flag files.

    A slice contains two mosaics: normal (2n-1) and tilted (2n).
    """

    project_base_path: str
    slice_number: int
    project_name: Optional[str] = None
    grid_size_x_normal: Optional[int] = None
    grid_size_x_tilted: Optional[int] = None
    normal_mosaic_id: int = field(init=False)
    tilted_mosaic_id: int = field(init=False)
    normal_mosaic: MosaicState = field(init=False)
    tilted_mosaic: MosaicState = field(init=False)
    started: bool = field(init=False, default=False)
    registered: bool = field(init=False, default=False)
    uploaded: bool = field(init=False, default=False)

    def __post_init__(self):
        """
        Initialize slice state from flag files.
        """
        # Calculate mosaic IDs
        self.normal_mosaic_id = 2 * self.slice_number - 1
        self.tilted_mosaic_id = 2 * self.slice_number

        # Recover state for both mosaics from flag files
        self.normal_mosaic = MosaicState(
            self.project_base_path,
            self.normal_mosaic_id,
            self.project_name,
            grid_size_x=self.grid_size_x_normal,
        )
        self.tilted_mosaic = MosaicState(
            self.project_base_path,
            self.tilted_mosaic_id,
            self.project_name,
            grid_size_x=self.grid_size_x_tilted,
        )
        # Recover slice-level flags
        self._recover_slice_flags()

    def _recover_slice_flags(self):
        """Recover slice-level flag files."""
        _, _, _, slice_state_path = get_slice_paths(
            self.project_base_path, self.slice_number
        )

        if not slice_state_path.exists():
            self.started = False
            self.registered = False
            self.uploaded = False
            return

        self.started = get_slice_flag_path(
            slice_state_path, self.slice_number, STARTED
        ).exists()
        self.registered = get_slice_flag_path(
            slice_state_path, self.slice_number, REGISTERED
        ).exists()
        self.uploaded = get_slice_flag_path(
            slice_state_path, self.slice_number, UPLOADED
        ).exists()

    def both_mosaics_complete(self) -> bool:
        """Check if both mosaics are complete."""
        return self.normal_mosaic.is_complete() and self.tilted_mosaic.is_complete()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "slice_number": self.slice_number,
            "normal_mosaic_id": self.normal_mosaic_id,
            "tilted_mosaic_id": self.tilted_mosaic_id,
            "normal_mosaic_state": self.normal_mosaic.to_dict(),
            "tilted_mosaic_state": self.tilted_mosaic.to_dict(),
            "normal_complete": self.normal_mosaic.is_complete(),
            "tilted_complete": self.tilted_mosaic.is_complete(),
            "both_complete": self.both_mosaics_complete(),
        }

    def to_dict_full(self) -> Dict[str, Any]:
        """
        Convert to full dictionary format for serialization.

        Returns
        -------
        Dict[str, Any]
            Complete state dictionary suitable for JSON serialization
        """
        return {
            "slice_number": self.slice_number,
            "normal_mosaic_id": self.normal_mosaic_id,
            "tilted_mosaic_id": self.tilted_mosaic_id,
            "normal_mosaic": self.normal_mosaic.to_dict_full(),
            "tilted_mosaic": self.tilted_mosaic.to_dict_full(),
            "started": self.started,
            "registered": self.registered,
            "uploaded": self.uploaded,
        }

    def refresh(self):
        """
        Refresh state by re-reading flag files.
        """
        self.normal_mosaic.refresh()
        self.tilted_mosaic.refresh()
        self._recover_slice_flags()

    @classmethod
    def from_flags(
        cls,
        project_base_path: str,
        slice_number: int,
        project_name: Optional[str] = None,
        grid_size_x_normal: Optional[int] = None,
        grid_size_x_tilted: Optional[int] = None,
    ) -> "SliceState":
        """
        Factory method to create SliceState from flag files.

        Parameters
        ----------
        project_base_path : str
            Base path for the project
        slice_number : int
            Slice number (1-indexed)
        project_name : str, optional
            Project name
        grid_size_x_normal : int, optional
            Grid size for normal mosaic
        grid_size_x_tilted : int, optional
            Grid size for tilted mosaic

        Returns
        -------
        SliceState
            SliceState instance initialized from flag files
        """
        return cls(
            project_base_path=project_base_path,
            slice_number=slice_number,
            project_name=project_name,
            grid_size_x_normal=grid_size_x_normal,
            grid_size_x_tilted=grid_size_x_tilted,
        )

    @classmethod
    def from_block(
        cls,
        project_base_path: str,
        slice_number: int,
        project_name: str,
    ) -> "SliceState":
        """
        Factory method to create SliceState with grid sizes from config block.

        Parameters
        ----------
        project_base_path : str
            Base path for the project
        slice_number : int
            Slice number (1-indexed)
        project_name : str
            Project name for retrieving grid sizes from config block

        Returns
        -------
        SliceState
            SliceState instance with grid sizes from config block
        """
        project_config = get_project_config_block(project_name)
        grid_size_x_normal = (
            project_config.grid_size_x_normal if project_config else None
        )
        grid_size_x_tilted = (
            project_config.grid_size_x_tilted if project_config else None
        )
        return cls(
            project_base_path=project_base_path,
            slice_number=slice_number,
            project_name=project_name,
            grid_size_x_normal=grid_size_x_normal,
            grid_size_x_tilted=grid_size_x_tilted,
        )


@dataclass
class ProjectState(BaseState):
    """
    Represents the state of an entire project, recovered from flag files.

    Contains all slices (and their mosaics and batches) in a single state object.
    """

    project_base_path: str
    project_name: str
    slice_numbers: Optional[List[int]] = None
    project_config: Optional[PSOCTScanConfig] = field(init=False, default=None)
    slices: Dict[int, SliceState] = field(init=False, default_factory=dict)

    def __post_init__(self):
        """
        Initialize project state from flag files.
        """
        # Load project configuration block to get grid sizes
        self.project_config = get_project_config_block(self.project_name)
        grid_size_x_normal = (
            self.project_config.grid_size_x_normal if self.project_config else None
        )
        grid_size_x_tilted = (
            self.project_config.grid_size_x_tilted if self.project_config else None
        )

        # Discover slices if not provided
        if self.slice_numbers is None:
            self.slice_numbers = self.discover_slices()

        # Recover state for all slices from flag files
        for slice_number in self.slice_numbers:
            self.slices[slice_number] = SliceState(
                self.project_base_path,
                slice_number,
                self.project_name,
                grid_size_x_normal=grid_size_x_normal,
                grid_size_x_tilted=grid_size_x_tilted,
            )

    def discover_slices(self) -> List[int]:
        """
        Discover available slices by scanning filesystem for mosaic directories.

        Scans {project_base_path}/mosaic-*/state/ directories and extracts
        slice numbers from mosaic IDs.

        Returns
        -------
        List[int]
            Sorted list of unique slice numbers found
        """
        project_path = Path(self.project_base_path)
        if not project_path.exists():
            return []

        # Find all mosaic directories
        mosaic_dirs = list(project_path.glob("mosaic-*/state"))
        slice_numbers = set()

        for mosaic_dir in mosaic_dirs:
            # Extract mosaic ID from directory name (e.g., "mosaic-001" -> 1)
            try:
                mosaic_id_str = mosaic_dir.parent.name.replace("mosaic-", "")
                mosaic_id = int(mosaic_id_str)
                # Convert mosaic ID to slice number
                slice_number = mosaic_id_to_slice_number(mosaic_id)
                slice_numbers.add(slice_number)
            except (ValueError, AttributeError):
                # Skip invalid directory names
                continue

        return sorted(list(slice_numbers))

    def get_slice_state(self, slice_number: int) -> Optional[SliceState]:
        """
        Get state for a specific slice.

        Parameters
        ----------
        slice_number : int
            Slice number (1-indexed)

        Returns
        -------
        SliceState, optional
            Slice state object, or None if slice_number not found
        """
        return self.slices.get(slice_number)

    def get_mosaic_state(self, mosaic_id: int) -> Optional[MosaicState]:
        """
        Get state for a specific mosaic.

        Parameters
        ----------
        mosaic_id : int
            Mosaic identifier

        Returns
        -------
        MosaicState, optional
            Mosaic state object, or None if mosaic_id not found
        """
        slice_number = mosaic_id_to_slice_number(mosaic_id)
        slice_state = self.get_slice_state(slice_number)
        if slice_state is None:
            return None

        if mosaic_id == slice_state.normal_mosaic_id:
            return slice_state.normal_mosaic
        elif mosaic_id == slice_state.tilted_mosaic_id:
            return slice_state.tilted_mosaic
        else:
            return None

    def refresh(self):
        """
        Refresh state by re-reading flag files for all slices.
        """
        # Re-discover slices in case new ones were added
        discovered_slices = self.discover_slices()
        set(self.slices.keys()) | set(discovered_slices)

        # Refresh all existing slices
        for slice_number in list(self.slices.keys()):
            self.slices[slice_number].refresh()

        # Add any newly discovered slices
        grid_size_x_normal = (
            self.project_config.grid_size_x_normal if self.project_config else None
        )
        grid_size_x_tilted = (
            self.project_config.grid_size_x_tilted if self.project_config else None
        )
        for slice_number in discovered_slices:
            if slice_number not in self.slices:
                self.slices[slice_number] = SliceState(
                    self.project_base_path,
                    slice_number,
                    self.project_name,
                    grid_size_x_normal=grid_size_x_normal,
                    grid_size_x_tilted=grid_size_x_tilted,
                )

        # Update slice_numbers
        self.slice_numbers = sorted(self.slices.keys())

    def to_dict_full(self) -> Dict[str, Any]:
        """
        Convert to full dictionary format for serialization.

        Returns
        -------
        Dict[str, Any]
            Complete state dictionary suitable for JSON serialization
        """
        slices_dict = {}
        for slice_number, slice_state in self.slices.items():
            slices_dict[str(slice_number)] = slice_state.to_dict_full()

        return {
            "project_name": self.project_name,
            "project_base_path": self.project_base_path,
            "slices": slices_dict,
        }

