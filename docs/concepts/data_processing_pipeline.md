---
title: Data processing pipeline
---

# Data processing pipeline

The data processing pipeline combines low-level stitching and coordinate
operations with higher-level orchestration flows.

Relevant components include:

- stitching and grid utilities under `opticstream.data_processing.stitch`
  (for example, `fit_coord_files.py` and `grid.py`)
- flows that call into these utilities for volume stitching, mosaic assembly,
  and registration

See the developer and reference sections for more detailed API-level
information.

