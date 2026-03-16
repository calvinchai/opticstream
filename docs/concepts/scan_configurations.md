---
title: Scan configurations
---

# Scan configurations

Scan configuration models describe how OpticStream should interpret and process
input data for a given project.

- `LSMScanConfigModel` and `LSMScanConfig` (in `opticstream.config.lsm_scan_config`)
  describe paths, formats, and processing flags for LSM data.
- `PSOCTScanConfig` (in `opticstream.config.psoct_scan_config`) describes grid,
  tile, overlap, and output formats for PS-OCT data.

These models are typically stored as Prefect Blocks and loaded at runtime by
the flows that consume them.

