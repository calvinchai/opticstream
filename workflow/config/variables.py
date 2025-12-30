tile_archive_format  = "sub-{project_name}_sample-slice{slice_id:03d}_chunk-{tile_id:04d}_acq-{acq}_OCT.nii.gz"
mosaic_volume_format = "sub-{project_name}_sample-slice{slice_id:03d}_acq-{acq}_proc-{modality}_OCT.ome.zarr"
mosaic_enface_format = "sub-{project_name}_sample-slice{slice_id:03d}_acq-{acq}_proc-{modality}_OCT.nii.gz"
mosaic_mask_format = "sub-{project_name}_sample-slice{slice_id:03d}_acq-{acq}_OCT_mask.nii.gz"
slice_registered_format = "sub-{project_name}_sample-slice{slice_id:03d}_proc-3daxis_OCT.nii.gz"

grid_size_x_normal = 10
grid_size_x_tilted = 10
grid_size_y = 31
tile_overlap = 20.0
mask_threshold = 50.0
scan_resolution_3d = [0.01, 0.01, 0.0025]

project_base_path = "/path/to/project"