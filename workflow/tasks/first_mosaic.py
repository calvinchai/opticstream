import sys

sys.path.append("/homes/5/kc1708/localhome/code/oct-pipe/")

from data_processing.stitch import fit_coord_files, generate_mask
from data_processing.stitch.process_tile_coord import process_tile_coord
from linc_convert.modalities.psoct.mosaic2d import mosaic2d
# grid, tiles = process_tile_coord(
# "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/TileConfigurationAll.txt",
#                  "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80
#                  /TileConfigurationAll.registered.txt",
#                  "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/",
#                  "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80//tile_coords_export.yaml")
# fit_coord_files.main(input="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/tile_coords_export.yaml",
#                     output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_aip.yaml",
#                     base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                     scan_resolution=[0.01, 0.01])

# mosaic2d(tile_info_file="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_aip.yaml",
#          nifti_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_aip.nii",
#          jpeg_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_aip.jpeg")

# generate_mask.main(input="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_aip.nii",
#                   output="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_mask.nii",
#                   threshold=50)

# for modality in ["ret", "ori", "surf", "biref", "mip"]:
#     fit_coord_files.main(input="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/tile_coords_export.yaml",
#                             output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_{modality}.yaml",
#                             base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                             scan_resolution=[0.01, 0.01],
#                             replace=[f"aip:{modality}"],
#                             mask="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_mask.nii")

#     mosaic2d(tile_info_file=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_{modality}.yaml",
#             nifti_output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_{modality}.nii",
#             jpeg_output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_{modality}.jpeg",
#             tiff_output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_001_{modality}.tif",
#             circular_mean=True if modality in ["ori"] else False)
# mosaic 003
# fit_coord_files.main(input="/space/zircon/5/users/data/sub-I80_voi-slab2/tile_coords_export.yaml",
#                     output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_aip.yaml",
#                     base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/processed/",
#                     replace=["mosaic_001:mosaic_003","tile_0:image_"],
#                     scan_resolution=[0.01, 0.01])

# mosaic2d(tile_info_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_aip.yaml",
#          nifti_output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_aip.nii",
#          jpeg_output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_aip.jpeg")

# generate_mask.main(input="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_aip.nii",
#                   output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_mask.nii",
#                   threshold=50)

# for modality in ["ret", "ori", "surf", "biref", "mip"]:
#     fit_coord_files.main(input="/space/zircon/5/users/data/sub-I80_voi-slab2/tile_coords_export.yaml",
#                             output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_{modality}.yaml",
#                             base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/processed/",
#                             scan_resolution=[0.01, 0.01],
#                             replace=[f"aip:{modality}","tile_0:image_", "mosaic_001:mosaic_003"],
#                             mask="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_mask.nii")

#     mosaic2d(tile_info_file=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_{modality}.yaml",
#             nifti_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_{modality}.nii",
#             jpeg_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_{modality}.jpeg",
#             tiff_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-02/mosaic_003_{modality}.tif",
#             circular_mean=True if modality in ["ori"] else False)


# grid, tiles = process_tile_coord("/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/TileConfigurationTilted.txt",
#                  "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/TileConfigurationTilted.registered.txt",
#                  "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/",
#                  "/autofs/space/zircon_007/users/psoct-pipeline/sub-I80//tile_coords_export_tilted.yaml")
# fit_coord_files.main(input="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/tile_coords_export_tilted.yaml",
#                     output="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_aip.yaml",
#                     base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                     scan_resolution=[0.01, 0.01])
# mosaic2d(tile_info_file="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_aip.yaml",
#          nifti_output="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_aip.nii",
#          jpeg_output="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_aip.jpeg")
# generate_mask.main(input="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_aip.nii",
#                   output="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_mask.nii",
#                   threshold=50)
# for modality in ["ret", "ori", "surf", "biref", "mip"]:
#     fit_coord_files.main(input="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/tile_coords_export_tilted.yaml",
#                             output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_{modality}.yaml",
#                             base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                             scan_resolution=[0.01, 0.01],
#                             replace=[f"aip:{modality}"],
#                             mask="/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_mask.nii")

#     mosaic2d(tile_info_file=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_{modality}.yaml",
#             nifti_output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_{modality}.nii",
#             jpeg_output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_{modality}.jpeg",
#             tiff_output=f"/autofs/space/zircon_005/users/data/sub-I80_voi-slab2/mosaic_002_{modality}.tif",
#             circular_mean=True if modality in ["ori"] else False)


# mosaic2d(tile_info_file="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_002_aip.yaml",
#         nifti_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_002_aip.nii",
#         jpeg_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_002_aip.jpeg")


# fit_coord_files.main(input="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/tile_coords_export.yaml",
#                         output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_surf.yaml",
#                         base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                         scan_resolution=[0.01, 0.01],
#                         replace=["aip:surf"])

# mosaic2d(tile_info_file="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_surf.yaml",
#          nifti_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_surf.nii",
#          jpeg_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_surf.jpeg")


# fit_coord_files.main(input="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/tile_coords_export.yaml",
#                         output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_ori.yaml",
#                         base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                         scan_resolution=[0.01, 0.01],
#                         replace=["aip:ori"])

# mosaic2d(tile_info_file="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_ori.yaml",
#          nifti_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_ori.nii",
#          jpeg_output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_ori.jpeg",
#          circular_mean=True)


# fit_coord_files.main(input="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/tile_coords_export.yaml",
#                         output="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_dbi.yaml",
#                         base_dir="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80",
#                         scan_resolution=[0.01, 0.01],
#                         replace=["aip:dBI"])

# mosaic2d(tile_info_file="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_dbi.yaml",
#          out="/autofs/space/zircon_007/users/psoct-pipeline/sub-I80/mosaic_001_dbi.zarr")


from data_processing.stitch import fiji_stitch

fiji_stitch.main(
    directory="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
    file_template="mosaic_013_image_{iiii}_aip.nii",
    grid_size_x=21,
    grid_size_y=27,
)
process_tile_coord(
    ideal_coord_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/TileConfiguration.txt",
    stitched_coord_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/TileConfiguration.registered.txt",
    image_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
    export="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_013_tile_coords_export.yaml",
)

fit_coord_files.main(
    input="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_013_tile_coords_export.yaml",
    output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_aip.yaml",
    base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
    scan_resolution=[0.01, 0.01],
)
mosaic2d(
    tile_info_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_aip.yaml",
    nifti_output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_aip.nii",
    jpeg_output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_aip.jpeg",
)
generate_mask.main(
    input="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_aip.nii",
    output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_mask.nii",
    threshold=50,
)
for modality in ["ret", "ori", "biref", "mip", "surf"]:
    fit_coord_files.main(
        input="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_013_tile_coords_export.yaml",
        output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_{modality}.yaml",
        base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
        scan_resolution=[0.01, 0.01],
        replace=[f"aip:{modality}"],
        mask="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_mask.nii",
    )
    mosaic2d(
        tile_info_file=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_{modality}.yaml",
        nifti_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_{modality}.nii",
        jpeg_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_{modality}.jpeg",
        tiff_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_{modality}.tif",
        circular_mean=True if modality in ["ori"] else False,
    )
for modality in ["dBI", "R3D", "O3D"]:
    fit_coord_files.main(
        input="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_013_tile_coords_export.yaml",
        output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_{modality}.yaml",
        base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
        scan_resolution=[0.01, 0.01, 0.0025],
        replace=[f"aip:{modality}"],
    )

fiji_stitch.main(
    directory="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
    file_template="mosaic_014_image_{iiii}_aip.nii",
    grid_size_x=35,
    grid_size_y=27,
    output_textfile_name="TileConfigurationTilted.txt",
)

process_tile_coord(
    ideal_coord_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/TileConfigurationTilted.txt",
    stitched_coord_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/TileConfigurationTilted.registered.txt",
    image_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
    export="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_014_tile_coords_export.yaml",
)

fit_coord_files.main(
    input="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_014_tile_coords_export.yaml",
    output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_aip.yaml",
    base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
    scan_resolution=[0.01, 0.01],
)
mosaic2d(
    tile_info_file="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_aip.yaml",
    nifti_output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_aip.nii",
    jpeg_output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_aip.jpeg",
)
generate_mask.main(
    input="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_aip.nii",
    output="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_mask.nii",
    threshold=50,
)
for modality in ["ret", "ori", "biref", "mip", "surf"]:
    fit_coord_files.main(
        input="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_014_tile_coords_export.yaml",
        output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_{modality}.yaml",
        base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
        scan_resolution=[0.01, 0.01],
        replace=[f"aip:{modality}"],
        mask="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_mask.nii",
    )
    mosaic2d(
        tile_info_file=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_{modality}.yaml",
        nifti_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_{modality}.nii",
        jpeg_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_{modality}.jpeg",
        tiff_output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_{modality}.tif",
        circular_mean=True if modality in ["ori"] else False,
    )

for modality in ["dBI", "R3D", "O3D"]:
    fit_coord_files.main(
        input="/space/zircon/5/users/data/sub-I80_voi-slab2/mosaic_014_tile_coords_export.yaml",
        output=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_014_{modality}.yaml",
        base_dir="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/processed/",
        scan_resolution=[0.01, 0.01, 0.0025],
        replace=[f"aip:{modality}"],
    )

# mosaic2d(tile_info_file=f"/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_dBI.yaml",
#          out="/space/zircon/5/users/data/sub-I80_voi-slab2/slice-07/mosaic_013_dBI.zarr", overwrite=True, driver="tensorstore", zarr_version=3, chunk=(64,),shard=(1024,))
