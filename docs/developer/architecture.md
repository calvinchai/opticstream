---
title: Architecture
---

# Architecture

At a high level, OpticStream is organized into:

- `opticstream.config`: configuration models and Prefect Blocks
- `opticstream.flows`: Prefect flows for LSM, PS-OCT, stitching, upload, and state management
- `opticstream.cli`: command-line interface for deploying and operating flows
- `opticstream.data_processing`: lower-level stitching and grid utilities
- `opticstream.utils`: shared helper utilities

Subsequent pages in this section describe the code layout, CLI, and testing strategy in more detail.

