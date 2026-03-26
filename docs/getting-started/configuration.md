---
title: Configuration
---

# Configuration

OpticStream uses typed configuration models and Prefect Blocks to manage
scan- and project-level settings.

## LSM scan configuration

The `LSMScanConfigModel` and `LSMScanConfig` block (in `opticstream.config.lsm_scan_config`)
control how LSM strips are processed, including:

- input paths (`project_base_path`, `info_file`, optional `archive_path`)
- output settings (`output_path`, `output_format`, `generate_mip`, `output_mip_format`)
- strip indexing (`strips_per_slice`)
- compute settings (`num_workers`, optional `cpu_affinity`)
- zarr conversion details (`zarr_config`)
- downstream stitching and validation (`stitch_volume`, `channel_volume_zarr_size_threshold`)

Strip cleanup behavior is controlled by `strip_cleanup_action`:

- `keep`: leave the raw strip folder unchanged
- `rename`: move the raw strip folder into a `processed` subdirectory
- `delete`: remove the raw strip folder after successful processing/backup checks

The strip flow emits Slack notifications (via configured Prefect blocks) for
success and failure, including strip paths and storage usage details.

## PS-OCT scan configuration

The `PSOCTScanConfig` block (in `opticstream.config.psoct_scan_config`) defines
grid layout, overlap, formats, and stitching parameters for PS-OCT processing.

It is composed from:

- `PSOCTAcquisitionParams` (tile/grid geometry and acquisition metadata)
- `PSOCTProcessingParams` (MATLAB/processing behavior and algorithm options)
- top-level output and modality settings (`enface_modalities`, `volume_modalities`,
  naming formats, masking thresholds, `zarr_config`)

See the concepts section for more detail on these configuration models.

## Using local checkouts of dev dependencies

OpticStream depends on several git-based Python packages during development, including:

- `linc-convert` (LSM features branch)
- `nifti-zarr` (port-multizarr branch)
- `psoct-toolbox`

When you install OpticStream from its git repository (for example with `pip install -e .`),
these packages are installed from the git URLs specified in `pyproject.toml`. This is the
behavior used by end users and CI.

For local development with `uv`, you can instead point these dependencies at local clones
by using the `[tool.uv.sources]` section in `pyproject.toml`. For example:

```toml
[tool.uv.sources]
linc-convert  = { path = "/path/to/your/local/linc-convert" }
nifti-zarr    = { path = "/path/to/your/local/nifti-zarr" }
psoct-toolbox = { path = "/path/to/your/local/psoct-toolbox" }
```

After updating the `path` values to match your machine, run:

```bash
uv sync
```

This causes `uv` to install `linc-convert`, `nifti-zarr`, and `psoct-toolbox` from your local
directories instead of the remote git repositories, while other users who do not modify
`[tool.uv.sources]` continue to use the default git URLs defined in `pyproject.toml`.

The exact `path = "..."` values are machine-specific. Each developer should adjust them in
their own clone as needed.

