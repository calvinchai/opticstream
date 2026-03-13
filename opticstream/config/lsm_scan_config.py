from typing import Optional

from niizarr import ZarrConfig
from prefect.blocks.core import Block


class LSMScanConfig(Block):
    zarr_config: ZarrConfig

    project_base_path: str
    info_file: str
    output_path: str
    output_mip: bool = True
    output_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}.ome.zarr"
    output_mip_format: str = "{project_name}_sample-slice{slice_id:02d}_chunk-{strip_id:04d}_acq-{acq}_proc-mip.tiff"

    archive_path: Optional[str] = None
    delete_strip: bool = False

    dandi_bin: Optional[str] = None
    dandi_instance: Optional[str] = None
