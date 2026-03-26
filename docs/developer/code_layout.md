---
title: Code layout
---

# Code layout

The main Python package is `opticstream`. The source tree is organized by
runtime role: CLI entrypoints, configuration models, Prefect flows, state and
event layers, and lower-level processing utilities.

## Top-level package map

```text
opticstream/
  cli/             # Cyclopts CLI: root app + init/lsm/oct/utils command groups
  config/          # Pydantic models + Prefect Blocks for LSM and PS-OCT
  flows/
    lsm/           # strip -> channel -> upload flow graph
    psoct/         # batch -> mosaic -> slice -> volume/upload flow graph
  state/           # PostgreSQL-backed project state models and services
  events/          # canonical event names + event trigger helpers/emitters
  tasks/           # reusable Prefect tasks (archive, DANDI, LINC uploads)
  artifacts/       # artifact publishing hooks/tasks
  data_processing/ # stitching, image conversion, spectral and volume helpers
  utils/           # shared runtime utilities (watching, naming, MATLAB, etc.)
  scripts/         # standalone scripts for operations and analysis
```

## Key responsibilities

- `opticstream.cli`: user/operator surface (`opticstream` / `ops`) for setup,
  watching, serving, deployment, and state operations.
- `opticstream.config`: schema-validated scan configuration models, including
  `LSMScanConfig` and `PSOCTScanConfig` Prefect blocks.
- `opticstream.flows`: orchestration logic and event entrypoints for both
  pipelines.
- `opticstream.state`: typed hierarchy models (`project -> ...`) and services
  (`LSM_STATE_SERVICE`, `OCT_STATE_SERVICE`) built on a PostgreSQL repository.
- `opticstream.events`: canonical event constants
  (`linc.opticstream.{pipeline}.{hierarchy}.{state}`) and helpers used by flow
  deployments and watchers.

## Navigation tips

- Start in `opticstream/cli` to understand operator workflows.
- Follow command implementations into `opticstream/flows`.
- Inspect `opticstream/state` when a flow depends on readiness checks or
  idempotency.
- Inspect `opticstream/data_processing` for algorithmic helpers invoked by
  flows/tasks.

