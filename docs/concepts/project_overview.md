---
title: Project overview
---

# Project overview

OpticStream provides Prefect-based workflows and utilities for processing
large microscopy and imaging datasets, including LSM strips and PS-OCT tiles.

At a high level, OpticStream is organized into:

- configuration models and Prefect Blocks under `opticstream.config`
- Prefect flows for LSM and PS-OCT processing under `opticstream.flows`
- command-line interface for deploying and operating flows under `opticstream.cli`
- PostgreSQL-backed project state services under `opticstream.state`
- event constants and trigger helpers under `opticstream.events`
- shared upload/archive tasks under `opticstream.tasks`
- flow artifact publishing hooks under `opticstream.artifacts`
- lower-level stitching and grid utilities under `opticstream.data_processing`
- shared helper utilities under `opticstream.utils`
- standalone utility scripts under `opticstream.scripts`

For a package-level view of the code structure, see the developer
[architecture](../developer/architecture.md) page. For a deep dive into the
OCT processing design and trade-offs, see the detailed
[design](../design.md) document.