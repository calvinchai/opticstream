from typing import List, Optional

from niizarr import ZarrConfig
from prefect.blocks.core import Block
from pydantic import BaseModel, Field, model_validator

from opticstream.config.utils import with_positions

@with_positions
class LSMScanConfigModel(BaseModel):
    project_name: str 
    project_base_path: str = Field(..., description="Base filesystem path for project data")
    info_file: str = Field(..., description="Path to the info .mat file")
    generate_zarr: bool = Field(default=True, description="Whether to generate the Zarr output")
    output_path: Optional[str] = Field(default=None, description="Path to the output directory for compressed strips")
    output_format: Optional[str] =  (
        "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}.ome.zarr"
    )
    generate_mip: bool = Field(default=True, description="Whether to generate the MIP output")
    output_mip_format: Optional[str] = (
        "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff"
    )
    output_mip_preview_window: List[float] = Field(default_factory=list, description="Preview window for the MIP output. If not set, the full range of the MIP will be used.")
    generate_archive: bool = Field(default=True, description="Whether to copy the raw spooled strips to an archive")
    archive_path: str | None = Field(default=None, description="Path to the archive directory for backup of compressed strips")

    delete_strip: bool = Field(default=False, description="Whether to delete the raw spooled strips after compression and backup")
    rename_strip: bool = Field(default=True, description="Whether to rename the raw spooled strips after compression and backup, if delete_strip is False")
    strips_per_slice: int = Field(default=100, description="Number of strips per slice to process")

    zarr_config: ZarrConfig = Field(default_factory=ZarrConfig)

    dandi_bin: str = Field(default="dandi", description="Path to the DANDI CLI binary, useful for installation in a separate conda/venv environment")
    dandi_instance: str = Field(default="linc", description="DANDI instance to use for upload")
    dandiset_path: str = Field(default="000052@draft/", description="Path to the DANDI set to use for upload")

    # for acquisition host 
    cpu_affinity: List[int] = Field(
        default_factory=list,
        description="Range of CPU cores to use for processing. If empty, all cores will be used.",
    )
    num_workers: int = Field(default=6, description="Number of workers to use for compression")

    stitch_volume: bool = Field(default=False, description="Whether to stitch the volume from the strips")


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

    # project_base_path: str
    # info_file: str
    # generate_zarr: bool = True
    # output_path: Optional[str] = None
    # output_format: Optional[str] = None
    # generate_mip: bool = True
    # output_mip_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff"
    # generate_archive: bool = True
    # archive_path: str | None = None
    # delete_strip: bool = False
    # rename_strip: bool = True
    # strips_per_slice: int = 100

    # # Zarr configuration
    # zarr_config: ZarrConfig = Field(default_factory=ZarrConfig)
    
    # dandi_bin: str = "dandi"
    # dandi_instance: str = "linc"
    # dandiset_path: str = "000052@draft/"

    # # CPU affinity configuration
    # cpu_affinity: List[int] = Field(default_factory=list, description="Range of CPU cores to use for processing. If empty, all cores will be used.")
    # num_workers: int = 6
    


class LSMScanConfigOverrides(BaseModel):
    project_base_path: Optional[str] = None
    info_file: Optional[str] = None
    generate_zarr: Optional[bool] = None
    output_path: Optional[str] = None
    output_format: Optional[str] = None
    generate_mip: Optional[bool] = None
    output_mip_format: Optional[str] = None
    generate_archive: Optional[bool] = None
    archive_path: Optional[str] = None
    delete_strip: Optional[bool] = None
    rename_strip: Optional[bool] = None
    strips_per_slice: Optional[int] = None

    zarr_config: Optional[ZarrConfig] = None

    dandi_bin: Optional[str] = None
    dandi_instance: Optional[str] = None
    dandiset_path: Optional[str] = None

    cpu_affinity: Optional[List[int]] = Field(default=None)
    num_workers: Optional[int] = None
    
def get_lsm_scan_config(project_name: str, override_config_name: Optional[str] = None) -> LSMScanConfig:
    """
    Get the scan configuration for a project.
    """
    return LSMScanConfig.load(override_config_name or f"{project_name}-lsm-config")