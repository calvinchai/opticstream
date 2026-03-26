---
title: Acquisition watchers (design layer)
---

# Acquisition watchers (design layer)

This page is the **design layer** for OpticStream’s two acquisition CLIs: **LSM strip watching** and **PS-OCT spectral batch watching**. It describes responsibilities, trust boundaries, and how each fits the rest of the system—without command-line recipes (see user guides) or module-level detail (see developer notes).

**Documentation stack**

| Layer | What it answers | Where |
|-------|------------------|--------|
| **User guides** | What do I run, in what order, with which flags? | [LSM workflow](../user-guides/lsm_workflow.md), [PS-OCT workflow](../user-guides/psoct_workflow.md) |
| **Design layer (this page)** | What problem does the watcher solve, what does it guarantee, how does it relate to state and Prefect? | Here |
| **Developer implementation** | Which modules, functions, scripts, and edge cases? | [LSM and OCT watcher design](../developer/watcher_design_lsm_oct.md) |

---

## Role in the system

Both watchers sit at the **edge between live acquisition** (folders and files on disk) and **downstream processing** (Prefect flows and project state). They are not the flows themselves; they **detect** when work is ready and either **emit** an event for a worker or **invoke** a flow in-process.

Design goals shared by both:

1. **Do not process incomplete or invisible data** — acquisition can be slower than directory listings; filesystems can cache or delay visibility.
2. **Do not duplicate work** — align with the same **Prefect-backed project state** the flows use (`peek_*` before enqueueing or running).
3. **Keep operator surface small** — a few commands and flags; complexity lives in code and docs.

---

## Trust boundaries (pipeline invariants)

Think of each watcher as a pipeline with explicit stages:

```mermaid
flowchart LR
  fs[Filesystem]
  discover[Discover]
  settle[Stable]
  ready[Readable]
  ident[Identity]
  gate[State_gate]
  out[Dispatch]

  fs --> discover
  discover --> settle
  settle --> ready
  ready --> ident
  ident --> gate
  gate --> out
```

- **Discover** — What paths are candidates? (LSM: new `Run*` subfolders; OCT: spectral NIfTI glob in a fixed folder.)
- **Stable** — Has the data settled? (LSM: **`wait_until_stable`** on the strip folder. OCT: **per-batch** fingerprint stability in **`PollingStableWatcher`**—each batch’s file paths + mtimes + sizes must stay unchanged for **`stability_seconds`** before dispatch; **`poll_interval`** is how often the loop re-scans and updates stability.)
- **Readable** — Can the process open the files for read? (OCT: `is_file` + `os.R_OK` per spectral file; LSM relies on the folder-level stability snapshot for strip content.)
- **Identity** — Parse folder/filenames into slice/strip/channel or mosaic/batch.
- **State gate** — `peek_strip` / `peek_batch`: skip if already tracked (unless forced resend).
- **Dispatch** — Emit `STRIP_READY` / `BATCH_READY`, or run `process_tile_batch` directly; then optional host-specific steps (e.g. OCT direct-mode disk refresh).

**Stability vs readability (OCT):** Per-batch fingerprint stability is about **time and content** (sizes/mtimes for that batch’s files); readability is about **permission and basic file validity**. Both are needed: a file can look stable in listing but still not readable, or pass `os.access` while another write is in flight (fingerprint changes reset the timer).

---

## Two discovery patterns

| | LSM | PS-OCT |
|---|-----|--------|
| **Trigger** | New **subdirectory** under a watch root | Repeated **rescan** of one **known** folder (`poll_interval` between iterations; each logical batch must meet **per-batch** fingerprint stability for `stability_seconds` before dispatch) |
| **Discovery** | Watchdog + polling queue of new folders | Glob `*cropped_focus*.nii` |
| **Batch unit** | One strip folder | One logical tile batch (from filename indices + `grid_size_y`) |
| **Default dispatch** | Prefect event (`STRIP_READY`) | In-process flow (`process_tile_batch`); optional event mode |

LSM is **event-driven** at the filesystem; OCT runs a **polling loop** (`PollingStableWatcher`) that rediscovers candidates each iteration, advances stability per batch, and processes **every** stable-ready batch in an iteration.

---

## Relation to state and Prefect

Watchers use the **same state store** as the flows so “already running / done” is consistent:

- **LSM:** strip identity → `peek_strip` before emit.
- **OCT:** batch identity → `peek_batch` before run or emit.

For details on events and variables, see [Flows and state models](flows_and_state_models.md) and [OCT events and state management](../developer/oct_events_and_state.md).

---

## Where to go next

- **Run the tools:** [Watching for new strip folders](../user-guides/lsm_workflow.md#watching-for-new-strip-folders), [Watching for new spectral batches](../user-guides/psoct_workflow.md#watching-for-new-spectral-batches)
- **Implementation and scripts:** [LSM and OCT watcher design](../developer/watcher_design_lsm_oct.md)
- **CLI index:** [CLI commands](../reference/cli_commands.md)
