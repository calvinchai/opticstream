import os
import glob
from collections import defaultdict

from prefect.deployments import run_deployment


def process_folder(folder_path, project_name, project_base_path, mosaic_ranges, batch_id_counter):
    """
    Process a folder for specified mosaic ranges with their batch sizes.
    
    Args:
        folder_path: Path to the folder containing spectral files
        project_name: Project name for the deployment
        project_base_path: Base path for the project
        mosaic_ranges: List of tuples (min_mosaic_id, max_mosaic_id, batch_size)
        batch_id_counter: Starting batch_id counter
    
    Returns:
        Updated batch_id_counter
    """
    # Get all spectral files
    spectral_files = glob.glob(os.path.join(folder_path, "*spectral*.nii"))
    
    if not spectral_files:
        print(f"No spectral files found in {folder_path}")
        return batch_id_counter
    
    # Group files by mosaic_id
    files_by_mosaic = defaultdict(list)
    for file_path in spectral_files:
        basename = os.path.basename(file_path)
        try:
            mosaic_id = int(basename.split("_")[1])
            files_by_mosaic[mosaic_id].append(file_path)
        except (ValueError, IndexError):
            print(f"Warning: Could not parse mosaic_id from {basename}, skipping")
            continue
    
    # Process each mosaic range
    triggered_batches = set()
    
    for min_mosaic_id, max_mosaic_id, batch_size in mosaic_ranges:
        for mosaic_id in range(min_mosaic_id, max_mosaic_id + 1):
            if mosaic_id not in files_by_mosaic:
                print(f"No files found for mosaic {mosaic_id}")
                continue
            
            file_list = files_by_mosaic[mosaic_id]
            
            # Sort files by tile_index for consistent ordering
            file_list.sort(key=lambda f: int(os.path.basename(f).split("_")[3]))
            
            # Build batches keyed by batch_id derived from tile index
            batches = {}
            for fpath in file_list:
                basename = os.path.basename(fpath)
                try:
                    tile_index = int(basename.split("_")[3])
                except (ValueError, IndexError):
                    print(f"Warning: Could not parse tile_index from {basename}, skipping")
                    continue
                
                # Derive batch_id from tile_index: 1 -> 001-027, 2 -> 028-054, etc.
                batch_id_for_tile = (tile_index - 1) // batch_size + 1
                batches.setdefault(batch_id_for_tile, []).append(fpath)
            
            for batch_id_for_tile, batch_files in sorted(batches.items()):
                # Only trigger full batches
                batch_key = (mosaic_id, batch_id_for_tile)
                
                if len(batch_files) < batch_size:
                    print(f"Warning: Incomplete batch for mosaic {mosaic_id}, logical_batch_id {batch_id_for_tile} "
                          f"(got {len(batch_files)} files, expected {batch_size})")
                    continue
                
                if batch_key in triggered_batches:
                    # This batch for this mosaic has already been triggered
                    continue
                
                # Map the stable (mosaic_id, batch_id_for_tile) into a unique running batch_id
                batch_id = batch_id_counter
                batch_id_counter += 1
                
                try:
                    print(f"Triggering deployment for mosaic {mosaic_id} with {len(batch_files)} files "
                          f"(logical_batch_id {batch_id_for_tile}, batch_id {batch_id})")
                    flow_run = run_deployment(
                        name="process_tile_batch_flow/process_tile_batch_flow",
                        parameters={
                            "project_name": project_name,
                            "project_base_path": project_base_path,
                            "mosaic_id": mosaic_id,
                            "batch_id": batch_id,
                            "file_list": batch_files,
                            "convert": False,
                        },
                    )
                    triggered_batches.add(batch_key)
                    print(f"Successfully triggered deployment for mosaic {mosaic_id}, "
                          f"logical_batch_id {batch_id_for_tile}, batch_id {batch_id}")
                except Exception as e:
                    print(f"Failed to trigger deployment for mosaic {mosaic_id}, "
                          f"logical_batch_id {batch_id_for_tile}, batch_id {batch_id}: {e}")
    
    print(f"Triggered {len(triggered_batches)} batches from {folder_path}")
    return batch_id_counter


if __name__ == "__main__":
    project_name = "sub-I55"
    project_base_path = "/space/zircon/5/users/data/sub-I55/"
    batch_id_counter = 0
    
    folder_path_1 = "/mnt/sas/I55_spectralraw_slice5-28/"
    mosaic_ranges_1 = [
        (20, 48, 31),   # mosaics 1-10, batch_size=28
    ]
    
    print(f"Processing folder: {folder_path_1}")
    batch_id_counter = process_folder(
        folder_path_1, 
        project_name, 
        project_base_path, 
        mosaic_ranges_1, 
        batch_id_counter
    )
    
