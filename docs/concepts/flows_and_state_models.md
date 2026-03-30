---
title: Flows and state models
---

# Flows and state models

OpticStream orchestrates both LSM and PS-OCT pipelines using Prefect flows and
hierarchical project state services.

## Flow families

### LSM flow modules (`opticstream.flows.lsm`)

Primary flow entrypoints include:

- `process_strip` / `process_strip_event` (`strip_process_flow.py`)
- `upload_strip_to_dandi_flow` / `upload_strip_to_dandi_event_flow` (`strip_upload_flow.py`)
- `process_channel` / `process_channel_event` (`channel_process_flow.py`)
- `process_channel_volume` / `process_channel_volume_event` (`channel_volume_flow.py`)
- `upload_channel_volume` / `upload_channel_volume_event` (`channel_upload_flow.py`)

### PS-OCT flow modules (`opticstream.flows.psoct`)

Primary flow entrypoints include:

- `process_tile_batch` / `process_tile_batch_event_flow` (`tile_batch_process_flow.py`)
- `process_mosaic` / `process_mosaic_event_flow` (`mosaic_process_flow.py`)
- `stitch_enface_modalities` (`mosaic_process_flow.py`)
- `register_slice_flow` / `register_slice_event_flow` (`slice_process_flow.py`)
- `stitch_volume_flow` / `stitch_volume_event_flow` (`mosaic_volume_stitch_flow.py`)
- `archive_tile_batch_flow` (`tile_batch_archive_flow.py`)
- upload flows for batch/mosaic/volume outputs in `tile_batch_upload_flow.py`,
  `mosaic_upload_flow.py`, and `mosaic_volume_upload_flow.py`

## State hierarchies

State is stored as project JSON in PostgreSQL and accessed through strongly
typed service APIs.

- LSM hierarchy: `project -> slice -> channel -> strip`
- PS-OCT hierarchy: `project -> slice -> mosaic -> batch`

Core lifecycle status uses `ProcessingState`:

- `pending`
- `running`
- `completed`
- `failed`

## Event model

Canonical event names follow:

- PS-OCT: `linc.opticstream.psoct.{batch|mosaic|slice}.{state}`
- LSM: `linc.opticstream.lsm.{strip|channel}.{state}`

Examples:

- `linc.opticstream.psoct.batch.ready`
- `linc.opticstream.psoct.mosaic.volume_stitched`
- `linc.opticstream.lsm.strip.ready`
- `linc.opticstream.lsm.channel.volume_uploaded`

