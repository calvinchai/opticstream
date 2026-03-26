---
title: PS-OCT Workflow
---

# PS-OCT processing workflow

This guide outlines the PS-OCT processing pipeline driven by the
`PSOCTScanConfig` block in `opticstream.config.psoct_scan_config`.

Key concepts:

- grid and tile layout across normal / tilted illuminations
- tile overlap and masking thresholds
- 3D volume and enface mosaic formats
- optional stitching and slice-registration steps
- OCT project state backing via `opticstream.state.oct_project_state`

The PS-OCT Prefect flows use the OCT project-state service to track progress:

- Mosaic-level progress (stitching and uploads) is recorded in `OCTMosaicState`
  and exposed via `OCTMosaicStateView` (for example, `enface_stitched`,
  `volume_stitched`, and the `all_batches_done()` helper).
- Slice-level progress (registration and readiness) is recorded in
  `OCTSliceState` / `OCTSliceStateView` (for example, `registered` and the
  `all_mosaics_done()` helper, which defaults to expecting two mosaics per
  slice).

Refer to the relevant flow modules for concrete Prefect flows that consume
`PSOCTScanConfig` and update OCT project state:

- `opticstream.flows.psoct.mosaic_process_flow` (enface mosaic stitching)
- `opticstream.flows.psoct.mosaic_volume_stitch_flow`
- `opticstream.flows.psoct.slice_process_flow`

## Watching for new spectral batches

Use the **OCT watch** CLI when tiles land as `*cropped_focus*.nii` files under a folder you control and you want batches picked up automatically (either processed **in-process** or handed off via **Prefect events**).

1. Create or update the project block: **`opticstream oct setup`** (see that command’s help for paths and grid sizes).
2. Run the watcher:

   ```bash
   opticstream oct watch PROJECT_NAME /path/to/spectral/folder MOSAIC_RANGES
   ```

   Example mosaic range string: `1:2` or `1:2,5:6` (comma-separated `min:max` per segment).

   Useful options: **`--stability-seconds`** (how long each batch’s file fingerprint—paths, sizes, mtimes—must stay unchanged before that batch is dispatched), **`--poll-interval`** (seconds between full discovery passes; default is short, e.g. 5), **`--refresh`** (optional disk refresh hook after each direct-mode batch, e.g. **`sas2`** on acquisition hosts—see **`opticstream.utils.refresh_disk`**), **`--direct / --no-direct`** (run **`process_tile_batch`** locally vs emit **`BATCH_READY`** for deployments), **`--force-resend`** (retry when batch state already exists). Each iteration can process **all** complete batches that have become stable.

**In one sentence:** the watcher repeatedly scans the folder, only considers **readable** spectral files, groups them into full batches, waits until each batch’s on-disk state is **stable** for the configured window, then either runs the tile-batch flow **directly** (default) or **emits an event** for Prefect.

For **design intent** (stability vs readability, state gate, discovery patterns), see [Acquisition watchers (design layer)](../concepts/acquisition_watchers.md). For **implementation detail** (readable checks, disk refresh scripts, config loading), see [LSM and OCT acquisition watchers (design)](../developer/watcher_design_lsm_oct.md).


