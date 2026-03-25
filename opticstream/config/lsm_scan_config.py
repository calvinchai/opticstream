from typing import List, Optional
from pathlib import Path

from niizarr import ZarrConfig
from prefect.blocks.core import Block
from pydantic import BaseModel, Field, field_validator

from opticstream.config.utils import with_positions
from opticstream.utils.naming_convention import normalize_project_name

@with_positions
class LSMScanConfigModel(BaseModel):
    project_name: str = Field(..., min_length=1)
    project_base_path: Path = Field(..., description="Base filesystem path for project data")
    info_file: Path = Field(..., description="Path to the info .mat file")
    generate_zarr: bool = Field(default=True, description="Whether to generate the Zarr output")
    output_path: Path | None = Field(default=None, description="Path to the output directory for compressed strips")
    output_format: Optional[str] = Field(default=(
        "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}.ome.zarr"
    ), min_length=1)
    generate_mip: bool = Field(default=True, description="Whether to generate the MIP output")
    output_mip_format: Optional[str] = Field(default=(
        "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff"
    ), min_length=1)
    output_mip_preview_window: List[float] = Field((0,1000), description="Preview window for the MIP output. If not set, the full range of the MIP will be used.")
    generate_archive: bool = Field(default=True, description="Whether to copy the raw spooled strips to an archive")
    archive_path: Path | None = Field(default=None, description="Path to the archive directory for backup of compressed strips")

    delete_strip: bool = Field(default=False, description="Whether to delete the raw spooled strips after compression and backup")
    rename_strip: bool = Field(default=True, description="Whether to rename the raw spooled strips after compression and backup, if delete_strip is False")
    strips_per_slice: int = Field(default=100, ge=1, description="Number of strips per slice to process")

    zarr_config: ZarrConfig = Field(default_factory=ZarrConfig)

    dandi_bin: str = Field(default="dandi", min_length=1, description="Path to the DANDI CLI binary, useful for installation in a separate conda/venv environment")
    dandi_instance: str = Field(default="linc", min_length=1, description="DANDI instance to use for upload")
    dandiset_path: str = Field(default="linc://000052/", description="Path to the DANDI set to use for upload")

    # for acquisition host 
    cpu_affinity: List[int] = Field(
        default_factory=list,
        description="Range of CPU cores to use for processing. If empty, all cores will be used.",
    )
    num_workers: int = Field(default=6, ge=1, description="Number of workers to use for compression")

    stitch_volume: bool = Field(default=False, description="Whether to stitch the volume from the strips")
    channel_volume_zarr_size_threshold: int = Field(
        default=10**9,
        ge=1,
        description="Min total bytes for stitched channel volume zarr when validation runs",
    )
    skip_channel_volume_zarr_validation: bool = Field(
        default=True,
        description=(
            "If True, only ensure volume output dir exists after stitch (placeholder-friendly). "
            "Set False when volume stitch writes a real zarr to enforce size threshold."
        ),
    )

    @field_validator("output_mip_preview_window")
    @classmethod
    def validate_output_mip_preview_window(cls, value: List[float]) -> List[float]:
        if len(value) not in (0, 2):
            raise ValueError("output_mip_preview_window must have 0 or 2 values")
        if len(value) == 2 and value[0] >= value[1]:
            raise ValueError("output_mip_preview_window must be [min, max] with min < max")
        return value

    @field_validator("cpu_affinity")
    @classmethod
    def validate_cpu_affinity(cls, value: List[int]) -> List[int]:
        if any(core < 0 for core in value):
            raise ValueError("cpu_affinity values must be >= 0")
        if len(set(value)) != len(value):
            raise ValueError("cpu_affinity values must be unique")
        return value


class LSMScanConfig(LSMScanConfigModel, Block):
    """
    Project-level configuration block for LSM processing.
    Block instances should be saved with name: "{project_name}-lsm-config"

    Attributes
    ----------
    zarr_config: ZarrConfig
        Configuration for the Zarr store
    project_base_path: str
        Base filesystem path for project data
    info_file: str
        Path to the info .mat file
    output_path: str
        Path to the output directory for compressed strips
    output_mip: bool, optional
        Whether to output the MIP (default: True)
    output_format: str, optional
        Format for the compressed strip output file (default: "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}.ome.zarr")
    output_mip_format: str, optional
        Format for the MIP output file for the compressed strip (default: "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff")
    archive_path: Optional[str], optional
        Path to the archive directory for backup of compressed strips (default: None)
    delete_strip: bool, optional
        Whether to delete the original strip after compression (default: False)
    dandi_bin: str, optional
        Path to the DANDI CLI binary (default: "dandi")
    dandi_instance: Optional[str], optional
        DANDI instance to use for upload (default: None)
    """


class LSMScanConfigOverrides(BaseModel):
    project_base_path: Path | None = None
    info_file: Path | None = None
    generate_zarr: Optional[bool] = None
    output_path: Path | None = None
    output_format: Optional[str] = None
    generate_mip: Optional[bool] = None
    output_mip_format: Optional[str] = None
    generate_archive: Optional[bool] = None
    archive_path: Path | None = None
    delete_strip: Optional[bool] = None
    rename_strip: Optional[bool] = None
    strips_per_slice: Optional[int] = Field(default=None, ge=1)

    zarr_config: Optional[ZarrConfig] = None

    dandi_bin: Optional[str] = None
    dandi_instance: Optional[str] = None
    dandiset_path: Path | None = None

    cpu_affinity: Optional[List[int]] = Field(default=None)
    num_workers: Optional[int] = Field(default=None, ge=1)
    stitch_volume: Optional[bool] = None
    channel_volume_zarr_size_threshold: Optional[int] = Field(default=None, ge=1)
    skip_channel_volume_zarr_validation: Optional[bool] = None

def get_lsm_scan_config(project_name: str, override_config_name: Optional[str] = None) -> LSMScanConfig:
    """
    Get the scan configuration for a project.
    """
    return LSMScanConfig.load(override_config_name or f"{normalize_project_name(project_name)}-lsm-config")