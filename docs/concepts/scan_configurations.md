---
title: Scan configurations
---

# Scan configurations

Scan configuration models describe how OpticStream should interpret and process
input data for a given project.

## LSM configuration (`opticstream.config.lsm_scan_config`)

- `LSMScanConfigModel`: Pydantic model used by flows and setup commands.
- `LSMScanConfig`: Prefect block type for persisted project configuration.

Key fields include project paths (`project_base_path`, `info_file`,
`output_path`, `archive_path`), output naming formats, `strips_per_slice`,
`zarr_config`, and resource controls (`num_workers`, `cpu_affinity`).

Raw strip post-processing is controlled by:

- `StripCleanupAction.KEEP`
- `StripCleanupAction.RENAME`
- `StripCleanupAction.DELETE`

Additional LSM options include `generate_mip`, `output_mip_format`,
`stitch_volume`, and channel-volume validation thresholds.

## PS-OCT configuration (`opticstream.config.psoct_scan_config`)

PS-OCT configuration is split into nested models:

- `PSOCTScanConfigModel` (top-level project settings)
- `PSOCTAcquisitionParams` (tile/grid geometry and acquisition constants)
- `PSOCTProcessingParams` (processing/MATLAB behavior)
- `PSOCTScanConfig` (Prefect block type)

Key enums used by the model:

- `TileSavingType`: `complex`, `spectral`, `spectral_12bit`, `complex_with_spectral`
- `EnfaceModality`: `aip`, `mip`, `ret`, `ori`, `biref`, `surf`, `mus`
- `VolumeModality`: `dBI`, `R3D`, `O3D`

Top-level fields include `mosaics_per_slice`, output naming formats, modality
selections, mask thresholds, stitch options, and a shared `zarr_config`.

## Runtime usage

These models are typically stored as Prefect Blocks and loaded at runtime by
watchers and flows. CLI setup commands (`opticstream lsm setup`,
`opticstream oct setup`) create/update project blocks using predictable names.

