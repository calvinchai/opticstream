import os
import subprocess
import time

from prefect.deployments import run_deployment

# 21 28 35 28  28 changed to 27
FOLDER_TO_WATCH = "/mnt/sas2/I80_frontal_slice7_20251212/"  # TODO: set the correct
# folder path here


def refresh_disk():
    subprocess.run(['bash', '/usr/etc/sas2_unmount'], check=True)
    subprocess.run(['bash', '/usr/etc/sas2_mount'], check=True)


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

    folder_path = "/mnt/sas2/I80_frontal_slice7_20251212/"
    project_name = "sub-I80_voi-slab2"
    project_base_path = "/space/zircon/5/users/data/sub-I80_voi-slab2/"
    batch_size = 27
    min_mosaic_id = 15
    max_mosaic_id = 20

    # Keep track of which (mosaic_id, batch_id) pairs we have already triggered
    triggered_batches = set()

    batch_id_counter = 0

    while True:
        refresh_disk()

        # Get all spectral files
        spectral_files = glob.glob(os.path.join(folder_path, "*spectral*.nii"))

        # Group files by mosaic_id and filter for the desired range
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

        # Process each mosaic, grouping files into batches whose IDs map to
        # specific filename index ranges. For example, with batch_size=27:
        #   batch_id 1 -> image_001 to image_027
        #   batch_id 2 -> image_028 to image_054
        # and so on, based on the tile index parsed from the filename.
        for mosaic_id in sorted(files_by_mosaic.keys()):
            file_list = files_by_mosaic[mosaic_id]

            # Sort files by tile_index for consistent ordering
            file_list.sort(key=lambda f: int(os.path.basename(f).split("_")[3]))

            # Build batches keyed by batch_id derived from tile index, so that
            # the mapping from batch_id to filenames is stable even if some
            # files are missing when we scan.
            batches = {}
            for fpath in file_list:
                basename = os.path.basename(fpath)
                try:
                    tile_index = int(basename.split("_")[3])
                except (ValueError, IndexError):
                    print(
                        f"Warning: Could not parse tile_index from {basename}, skipping")
                    continue

                # Derive batch_id from tile_index: 1 -> 001-027, 2 -> 028-054, etc.
                batch_id_for_tile = (tile_index - 1) // batch_size + 1
                batches.setdefault(batch_id_for_tile, []).append(fpath)

            for batch_id_for_tile, batch_files in sorted(batches.items()):
                # Only trigger full batches, and only once per (mosaic_id, batch_id_for_tile)
                batch_key = (mosaic_id, batch_id_for_tile)

                if len(batch_files) < batch_size:
                    # Incomplete batch; wait for more tiles to arrive
                    continue

                if batch_key in triggered_batches:
                    # This batch for this mosaic has already been triggered
                    continue

                # Map the stable (mosaic_id, batch_id_for_tile) into a unique running
                # batch_id if needed by downstream logic, but keep the logical batch
                # index visible for clarity.
                batch_id = batch_id_counter
                batch_id_counter += 1

                try:
                    print(
                        f"Triggered deployment for mosaic {mosaic_id} with {len(batch_files)} files (logical_batch_id {batch_id_for_tile}, batch_id {batch_id})")
                    flow_run = run_deployment(
                        name="process_tile_batch_flow/process_tile_batch_flow",
                        parameters={
                            "project_name": project_name,
                            "project_base_path": project_base_path,
                            "mosaic_id": mosaic_id,
                            "batch_id": batch_id,
                            "file_list": batch_files,
                        },
                    )
                    triggered_batches.add(batch_key)
                    print(
                        f"Triggered deployment for mosaic {mosaic_id} with "
                        f"{len(batch_files)} files (logical_batch_id {batch_id_for_tile}, batch_id {batch_id})"
                    )
                except Exception as e:
                    print(
                        f"Failed to trigger deployment for mosaic {mosaic_id}, "
                        f"logical_batch_id {batch_id_for_tile}, batch_id {batch_id}: {e}"
                    )
        print(f"Triggered {len(triggered_batches)} batches")

        # Wait before rescanning for new tiles / complete batches
        time.sleep(30)