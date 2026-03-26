---
title: Architecture
---

# Architecture

This page describes the package-level structure of OpticStream. For a
product-level introduction and major components, see the
[project overview](../concepts/project_overview.md). For details on the OCT
processing design and event-driven pipeline, see the
[design](../design.md) document and the OCT developer design docs under
`developer/oct_*.md`.

At a high level, the Python packages are:

- `opticstream.config`: configuration models and Prefect Blocks
- `opticstream.flows`: Prefect flows for LSM and PS-OCT processing
- `opticstream.cli`: command-line interface for deploying and operating flows
- `opticstream.state`: PostgreSQL-backed project state models and services
- `opticstream.events`: canonical event names and Prefect event-trigger helpers
- `opticstream.tasks`: shared Prefect tasks (archive, DANDI upload, LINC upload)
- `opticstream.artifacts`: artifact publishing hooks/tasks for flow runs
- `opticstream.data_processing`: lower-level stitching and grid utilities
- `opticstream.utils`: shared helper utilities
- `opticstream.scripts`: standalone utility scripts used by operations and debugging

For details on the PostgreSQL-backed project state design (including locks,
views, and the `open_*` / `read_*` / `peek_*` APIs), see the
[state design](prefect_state_design.md) developer document.

Subsequent pages in this section describe the code layout, CLI, and testing
strategy in more detail.
