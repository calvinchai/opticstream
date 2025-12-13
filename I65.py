#prefect deployment run 'process_tile_flow/process_tile' --param project_name=I55 --param project_base_path=/run/media/kc1708/New\ Volume/oct-archive/000053/rawdata/sub-I55 --param tile_path=/mnt/sas/I55_spectralraw_slice12_20251125/mosaic_015_image_225_spectral_0225.nii --param mosaic_id=15 --param tile_index=225 --param output_base_path=/run/media/kc1708/New\ Volume/octarchive/000053/rawdata/sub-I55/ --param intermediate_base_path=/local_mount/space/zircon/7/users/psoct-pipeline/ --param compressed_base_path=/run/media/kc1708/New\ Volume/oct-archive/000053/rawdata/sub-I55/ --param surface_method=find --param depth=80&




# for each file in /mnt/sas/I65_test_megatome_20251209 with spectral in name
# mosaic_001_image_519_spectral_0519.nii

import os
import glob
import time
i=0
for file in glob.glob("/mnt/sas/I65_test_megatome_20251209/*spectral*.nii"):
    # run prefect command line 
    # extract mosaic_id and tile_index from file name
    basename = os.path.basename(file)
    mosaic_id = int(basename.split("_")[1])
    tile_index = int(basename.split("_")[3])

    os.system(f"prefect deployment run 'process_tile_flow/process_tile' --param project_name=I65 --param project_base_path='/local_mount/space/zircon/7/users/psoct-pipeline/sub-I65/' --param tile_path={file} --param mosaic_id=1 --param tile_index={tile_index} --param output_base_path='/run/media/kc1708/New Volume/octarchive/000053/rawdata/sub-I65/' --param intermediate_base_path='/local_mount/space/zircon/7/users/psoct-pipeline/sub-I65/' --param compressed_base_path='/run/media/kc1708/New Volume/oct-archive/000053/rawdata/sub-I65/' --param surface_method=find --param depth=80")
    i+=1
    if i>1:
        break 
    #time.sleep(15)
   
