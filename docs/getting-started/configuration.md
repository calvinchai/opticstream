---
title: Configuration
---

# Configuration

OpticStream uses typed configuration models and Prefect Blocks to manage
scan- and project-level settings.

## LSM scan configuration

The `LSMScanConfigModel` and `LSMScanConfig` block (in `opticstream.config.lsm_scan_config`)
control how LSM strips are processed, including:

- input project base path and info file
- zarr output paths and naming formats
- archive / backup paths
- resource usage (workers, CPU affinity)

Strip cleanup behavior is controlled by:

- `delete_strip` (bool): if `True`, the raw strip folder will be deleted after
  compression and backup checks succeed.
- `rename_strip` (bool): if `True` and `delete_strip` is `False`, the raw strip
  folder will be moved into a `processed` subdirectory instead of being left in
  place.

If both flags are `False`, the raw strip folder is left unchanged. If both are
`True`, **`delete_strip` takes precedence** and the strip folder is deleted
rather than renamed. The `process_strip_flow` emits Slack notifications (via the
configured Prefect `SlackWebhook` block) on both success and failure, including
the strip folder path, raw and compressed output sizes, and total disk usage for
the filesystem containing the strip.

## PS-OCT scan configuration

The `PSOCTScanConfig` block (in `opticstream.config.psoct_scan_config`) defines
grid layout, overlap, formats, and stitching parameters for PS-OCT processing.

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

---
title: Configuration
---

# Configuration

OpticStream uses typed configuration models and Prefect Blocks to manage
scan- and project-level settings.

## LSM scan configuration

The `LSMScanConfigModel` and `LSMScanConfig` block (in `opticstream.config.lsm_scan_config`)
control how LSM strips are processed, including:

- input project base path and info file
- zarr output paths and naming formats
- archive / backup paths
- resource usage (workers, CPU affinity)

## PS-OCT scan configuration

The `PSOCTScanConfig` block (in `opticstream.config.psoct_scan_config`) defines
grid layout, overlap, formats, and stitching parameters for PS-OCT processing.

See the concepts section for more detail on these configuration models.

