import sys
sys.path.append('/autofs/space/zircon_001/users/prefect/oct-pipe')
from workflow.utils.utils import mosaic_id_to_slice_number 

from workflow.flows.mosaic_processing_flow import stitch_volume_modalities_flow
from pathlib import Path

project_name = "sub-I80_voi-slab2"
project_base_path = "/space/zircon/5/users/data/sub-I80_voi-slab2/"
scan_resolution_3d = [0.01, 0.01, 0.0025]
for mosaic_id in range(1, 17):
    slice_id = mosaic_id_to_slice_number(mosaic_id)
    stitched_path = Path(f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/slice-{slice_id:02d}/stitched")
    processed_path = Path(f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/slice-{slice_id:02d}/processed")
    stitch_volume_modalities_flow(project_name, project_base_path, mosaic_id, ".",".",processed_path, stitched_path, ".",scan_resolution_3d)
    