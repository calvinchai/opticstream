

import os
import time
import subprocess

from prefect.deployments import run_deployment

from workflow.flows.upload_flow import upload_flow


# 21 28 35 28  28 changed to 27
FOLDER_TO_WATCH = "/mnt/sas2/I80_frontal_20251210/"  # TODO: set the correct folder path here

def refresh_disk():
    subprocess.run(['bash','/usr/etc/sas2_unmount'], check=True)
    subprocess.run(['bash','/usr/etc/sas2_mount'], check=True)

def list_files(folder):
    try:
        return set(os.listdir(folder))
    except FileNotFoundError:
        return set()

def trigger_deployment(file_path):
    """
    Trigger a deployment run for the given file.
    You can replace this logic with your real deployment trigger.
    """
    # extract mosaic_id and tile_index from file name
    basename = os.path.basename(file_path)
    mosaic_id = int(basename.split("_")[1])
    tile_index = int(basename.split("_")[3])
    try:
        flow_run = run_deployment(
        name="process_tile_flow/process_tile",
        parameters={
        "project_name": "sub-I80_voi-slab2",
        "project_base_path": "/space/zircon/5/users/data/sub-I80_voi-slab2/",
        "tile_path": file_path,
        "mosaic_id": mosaic_id,
        "tile_index": tile_index,
        "output_base_path": "/space/zircon/5/users/data/sub-I80_voi-slab2/",
        "intermediate_base_path": "/local_mount/space/zircon/7/users/psoct-pipeline/sub-I80/",
        "compressed_base_path": "/run/media/kc1708/New Volume/oct-archive/000053/rawdata/sub-I80/",
        "surface_method": "find",
        "depth": 80
        },
        timeout=0
        )
        
        print(f"Triggered deployment for {file_path}")
    except Exception as e:
        print(f"Failed to trigger deployment for {file_path}: {e}")

def main():
    seen_files = set()
    while True:
        refresh_disk()
        files = list_files(FOLDER_TO_WATCH)
        new_files = files - seen_files
        for idx, file in enumerate(sorted(new_files)):
            file_path = os.path.join(FOLDER_TO_WATCH, file)
            trigger_deployment(file_path)
            seen_files.add(file)
            
            if idx < len(new_files) - 1:
                time.sleep(5)
        
        if len(seen_files) > 3136:
            break
        # Wait before the next polling interval
        time.sleep(30)


if __name__ == "__main__":
    import glob
    from collections import defaultdict
    
    folder_path = "/mnt/sas2/I80_frontal_20251210/"
    project_name = "sub-I80_voi-slab2"
    project_base_path = "/space/zircon/5/users/data/sub-I80_voi-slab2/"
    batch_size = 28
    min_mosaic_id = 3
    max_mosaic_id = 12
    
    # Get all spectral files
    spectral_files = glob.glob(os.path.join(folder_path, "*spectral*.nii"))
    
    # Group files by mosaic_id and filter for mosaic_id 3-12
    files_by_mosaic = defaultdict(list)
    for file_path in spectral_files:
        basename = os.path.basename(file_path)
        try:
            mosaic_id = int(basename.split("_")[1])
            if min_mosaic_id <= mosaic_id <= max_mosaic_id:
                files_by_mosaic[mosaic_id].append(file_path)
        except (ValueError, IndexError):
            print(f"Warning: Could not parse mosaic_id from {basename}, skipping")
            continue
    
    # Process each mosaic, grouping files into batches of 28
    batch_id_counter = 0
    for mosaic_id in sorted(files_by_mosaic.keys()):
        file_list = files_by_mosaic[mosaic_id]
        # Sort files by tile_index for consistent ordering
        file_list.sort(key=lambda f: int(os.path.basename(f).split("_")[3]))
        
        # Split into batches of 28
        for i in range(0, len(file_list), batch_size):
            batch_files = file_list[i:i+batch_size]
            batch_id = batch_id_counter
            batch_id_counter += 1
            
            try:
                flow_run = run_deployment(
                    name="process_tile_batch_flow/process_tile_batch_flow",
                    parameters={
                        "project_name": project_name,
                        "project_base_path": project_base_path,
                        "mosaic_id": mosaic_id,
                        "batch_id": batch_id,
                        "file_list": batch_files
                    }
                )
                print(f"Triggered deployment for mosaic {mosaic_id} with {len(batch_files)} files (batch {batch_id})")
            except Exception as e:
                print(f"Failed to trigger deployment for mosaic {mosaic_id}, batch {batch_id}: {e}")
            break
        break # TODO: Remove this after testing
    # upload_flow(file_path='/run/media/kc1708/New Volume/oct-archive/000053/rawdata/sub-I80/')