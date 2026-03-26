---
title: LSM and OCT acquisition watchers (design)
---

# LSM and OCT acquisition watchers (design)

This page is the **implementation** companion for the acquisition watchers. For the conceptual **design layer** (pipeline invariants, two discovery patterns, relation to state), read **[Acquisition watchers (design layer)](../concepts/acquisition_watchers.md)** first.

For **step-by-step commands** for lab operators:

- [LSM workflow](../user-guides/lsm_workflow.md#watching-for-new-strip-folders)
- [PS-OCT workflow](../user-guides/psoct_workflow.md#watching-for-new-spectral-batches)

---

## Split between docs

| Audience | Where to look |
|----------|----------------|
| Lab operators | User guides: prerequisites, exact CLI invocations, flags, and basic troubleshooting. |
| Architects / integrators | [Concepts: Acquisition watchers](../concepts/acquisition_watchers.md): design layer, trust boundaries, LSM vs OCT pattern. |
| Engineers | This page: modules, filesystem details, scripts, and flags. |

---

## Shared building blocks

**LSM** (`opticstream.utils.directory_watch`):

- **`wait_until_stable`**: Polls file mtimes under a directory until nothing has changed for a configured **stability window**. This reduces the chance of processing a folder or files that are still being written.
- **`NewFolderHandler`** + **`polling_scanner`** + **`run_watcher_until_interrupt`** — watchdog observes **new immediate subdirectories** under a watch root, with a polling fallback if the OS misses events. The main thread blocks until Ctrl+C; worker threads handle discovery and the consumer.

**OCT** does **not** use that stack. It uses **`PollingStableWatcher`** (`opticstream.utils.polling_watcher`) together with domain logic in **`OCTWatcherService`** (`opticstream.cli.oct.watch`):

- On each **iteration**, the service **discovers** candidate batches (glob `*cropped_focus*.nii`, group by mosaic and logical batch).
- For each candidate key `(mosaic_id, logical_batch)`, the watcher tracks a **fingerprint** (relative paths plus `mtime` and `size` for each file in the batch). A batch is **stable** only after that fingerprint stays unchanged for **`stability_seconds`**.
- After **`poll_interval`** seconds, the next iteration runs again (so there is always a fixed delay between full scan passes, in addition to per-batch stability gating).
- Within one iteration, **every** candidate that has been stable long enough is **processed** (skipped batches do not block others).

---

## LSM watcher (`opticstream lsm watch`)

**Role:** Detect new strip folders (names starting with `Run`, case-insensitive), wait until their contents are stable, parse slice/strip/channel IDs from the folder name, then emit **`STRIP_READY`** for Prefect and ensure strip state exists (see `LSM_STATE_SERVICE`).

**Skip / dedupe:** If **`peek_strip`** already has a row for that strip and **`--force-resend`** is not set, the watcher logs a skip and does not emit again.

**Logging:** Uses the standard Python `logging` package (not raw prints) so facility and log aggregation behave consistently with OCT.

---

## OCT watcher (`opticstream oct watch`)

**Role:** Poll a **known acquisition folder** for spectral NIfTI tiles matching `*cropped_focus*.nii`, group them into batches using the configured **grid_size_y** (batch size), wait until each batch’s on-disk fingerprint is **stable**, then either:

- **Direct mode (default):** run **`process_tile_batch`** in-process; or  
- **`--no-direct`:** emit **`BATCH_READY`** so a Prefect deployment runs **`process_tile_batch_event_flow`**.

**Why check that each file is readable**

Spectral files can appear in the directory **before** they are fully written or visible with correct permissions (slow writes, NFS or network filesystem quirks, or umask). Including only paths that are **regular files** (`Path.is_file()`) **and** pass **`os.access(..., os.R_OK)`** avoids:

- feeding incomplete files into MATLAB / downstream flows;
- noisy failures that are hard for operators to interpret.

This check works together with **per-batch fingerprint stability**: the fingerprint includes file size and mtime, so incomplete writes tend to reset stability; readability filters paths the process can actually open.

**Stability vs folder-wide quiet:** Unlike a single **`wait_until_stable`** over the entire directory tree, stability is tracked **per logical batch**. Unrelated activity in the same folder (other mosaics, files that are not part of a candidate’s batch) does not reset a batch’s timer unless it changes that batch’s fingerprint.

**Skip / dedupe:** If **`peek_batch`** already has state for that mosaic batch and **`--force-resend`** is not set, the watcher skips that batch.

**Direct mode and disk refresh**

Disk refresh is **optional**. Pass **`--refresh`** with a registered hook name (see **`opticstream.utils.refresh_disk`**: e.g. **`sas2`**, **`sas`**, **`sas3`**, **`none`**). When set, after each successful **`process_tile_batch`** in **direct** mode the watcher runs the corresponding unmount/mount scripts (for **`sas2`**, `/usr/etc/sas2_unmount` and `/usr/etc/sas2_mount`). If those scripts are missing or fail (e.g. on a dev laptop), the failure is logged as a warning and the loop continues. If **`--refresh`** is omitted, no refresh runs.

**Config loading:** The `PSOCTScanConfig` / project block is loaded **once** when the **`watch`** command starts, not on every loop iteration.

---

## State model alignment

Both watchers align with the rest of OpticStream by consulting **Prefect-backed project state** before doing work:

- LSM: **`peek_strip`** (and optional **`open_strip`** after emit).  
- OCT: **`peek_batch`** (and **`open_batch`** after emit in event mode).

That keeps “already submitted or completed” semantics consistent with flows that update the same state objects.

---

## Further reading

- [Flows and state models](../concepts/flows_and_state_models.md)
- [OCT events and state management](oct_events_and_state.md)
- [OCT data flow and folder layout](oct_data_flow.md)
