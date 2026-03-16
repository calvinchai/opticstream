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

## MATLAB package configuration

Some workflows invoke external MATLAB code provided by a separate GitHub
repository. OpticStream locates this MATLAB package using the following
resolution order:

1. An explicit `matlab_script_path` argument passed to a flow or task.
2. The `OPTICSTREAM_MATLAB_PACKAGE_PATH` environment variable.
3. A managed clone under:
   - `~/.opticstream/matlab-packages/<package_name>` (default: `matlab-package`).

To prepare a managed clone, use the CLI helper:

- `opticstream utils matlab-deps install https://github.com/USER/MATLAB-REPO.git`

This will clone the repo into the managed directory so that batch flows and
slice registration can find the MATLAB functions without additional
configuration. On shared clusters, prefer setting
`OPTICSTREAM_MATLAB_PACKAGE_PATH` to a centrally-installed copy instead of
letting each user clone the repo.

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

