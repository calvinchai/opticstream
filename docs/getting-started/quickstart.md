---
title: Quickstart
---

# Quickstart

This guide walks through running an example LSM strip processing flow.

## 1. Activate your environment

If you used `uv` (recommended), either activate the environment that `uv sync`
created:

```bash
cd opticstream
source .venv/bin/activate
```

or run commands via `uv run` without activating:

```bash
cd opticstream
uv run opticstream --help
```

If you installed with `pip` or `pipx`, activate your environment or ensure the
`opticstream` CLI is on your PATH.

Once your environment is ready, you can inspect the CLI with:

```bash
opticstream --help      # or: ops --help
```

## 2. Configure an LSM project

Create and save an `LSMScanConfig` block (see the configuration page for details),
ensuring `project_base_path`, `info_file`, and `output_path` are set.

## 3. Run a strip processing flow

Once your scan configuration block is saved, you can trigger the
`process_strip_event` flow via Prefect by emitting the appropriate event
payload (see the LSM workflow user guide for a full example).

