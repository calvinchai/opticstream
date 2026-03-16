---
title: Quickstart
---

# Quickstart

This guide walks through running an example LSM strip processing flow.

## 1. Activate your environment

```bash
cd opticstream
source .venv/bin/activate  # or your preferred environment
```

## 2. Configure an LSM project

Create and save an `LSMScanConfig` block (see the configuration page for details),
ensuring `project_base_path`, `info_file`, and `output_path` are set.

## 3. Run a strip processing flow

Once your scan configuration block is saved, you can trigger the
`process_strip_event` flow via Prefect by emitting the appropriate event
payload (see the LSM workflow user guide for a full example).

