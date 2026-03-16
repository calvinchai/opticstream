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

Refer to the relevant flow modules for concrete Prefect flows that consume
`PSOCTScanConfig`.

