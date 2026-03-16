---
title: LSM Workflow
---

# LSM strip processing workflow

This guide describes the end-to-end workflow for processing LSM strips using
the `process_strip_flow` and related tasks in `opticstream.flows.lsm.process_strip_flow`.

At a high level, the flow:

1. Parses slice / strip / camera IDs from the strip folder name.
2. Resolves output, archive, and resource settings from an `LSMScanConfig` block.
3. Optionally compresses the strip to Zarr, generates MIP images, and backs up
   the raw strip to an archive location.
4. Performs basic validation checks before optionally renaming or deleting the
   original strip.

See the configuration and deployment guides for how to wire this into your
Prefect environment.

