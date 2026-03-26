---
title: Data processing pipeline
---

# Data processing pipeline

The data processing pipeline combines lower-level numeric/image utilities in
`opticstream.data_processing` with Prefect flow orchestration in
`opticstream.flows`.

## LSM processing stages

Typical strip processing stages are:

- watch/discovery (`opticstream cli lsm watch`) -> strip identity parsing
- strip conversion/compression (`opticstream.flows.lsm.strip_process_flow`)
- optional MIP generation and archive handling
- channel-level stitching (`opticstream.flows.lsm.channel_process_flow`)
- optional channel volume stitching/upload (`channel_volume_flow.py`,
  `channel_upload_flow.py`)

## PS-OCT processing stages

Typical PS-OCT processing stages are:

- batch discovery from spectral files (`opticstream cli oct watch`)
- tile-batch conversion (`opticstream.flows.psoct.tile_batch_process_flow`)
- mosaic processing and enface stitching (`mosaic_process_flow.py`)
- slice registration (`slice_process_flow.py`)
- volume stitching (`mosaic_volume_stitch_flow.py`)
- archive/upload/QC flows (`tile_batch_archive_flow.py`,
  `tile_batch_upload_flow.py`, `mosaic_*_upload_flow.py`,
  `mosaic_enface_qc_flow.py`)

## Data-processing utility modules

Key utilities currently used by flows include:

- `opticstream.data_processing.spectral_raw.spectral2complex`
- `opticstream.data_processing.volume_3d.complex2vol`
- `opticstream.data_processing.enface.vol2enface`
- `opticstream.data_processing.stitch.fiji_stitch`
- `opticstream.data_processing.stitch.fit_coord_files`
- `opticstream.data_processing.stitch.generate_mask`
- `opticstream.data_processing.convert_image`

These modules are intentionally lower-level; pipeline sequencing, retries, and
state transitions happen in Prefect flow modules.

