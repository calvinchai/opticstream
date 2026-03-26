---
title: LSM Workflow
---

# LSM strip processing workflow

This guide describes the end-to-end workflow for processing LSM strips using
the `process_strip_flow` and related tasks in `opticstream.flows.lsm.strip_process_flow`.

At a high level, the flow:

1. Parses slice / strip / camera IDs from the strip folder name.
2. Resolves output, archive, and resource settings from an `LSMScanConfig` block.
3. Optionally compresses the strip to Zarr, generates MIP images, and backs up
   the raw strip to an archive location.
4. Performs basic validation checks before optionally renaming or deleting the
   original strip.

See the configuration and deployment guides for how to wire this into your
Prefect environment.

## Watching for new strip folders

Use the **LSM watcher** on a machine that can see the acquisition directory where the instrument writes new strip folders (immediate children whose names start with `Run`, case-insensitive).

1. Ensure an **`LSMScanConfig`** block exists for the project (same naming convention as elsewhere in OpticStream).
2. Run:

   ```bash
   opticstream lsm watch PROJECT_NAME --watch-dir /path/to/parent
   ```

   Common options: **`--stability-seconds`** (how long a folder must be unchanged before processing), **`--poll-interval`** (polling fallback if filesystem events are missed), **`--force-resend`** (emit even if strip state already exists).

The watcher waits until each new folder is **stable**, then emits **`STRIP_READY`** for Prefect and ensures strip state exists.

For **design intent** (pipeline stages, state gate, LSM vs OCT patterns), see [Acquisition watchers (design layer)](../concepts/acquisition_watchers.md). For **implementation detail** (modules, logging, scripts), see [LSM and OCT acquisition watchers (design)](../developer/watcher_design_lsm_oct.md).

