---
title: Quickstart - LSM
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

```bash
ops lsm setup --help
```

```bash
ops lsm setup testscan
```

you should be able to see a block in prefect ui named testscan-lsm-config

in there you will need to complete the detailed setup, each field will have doc in there

in one window, start 
```bash
ops lsm serve 3
```
which will start 3 local workers to process lsm flows

in another windows, start watcher
```bash
ops lsm watch testscan E:/temp/testscan
```

## 3. somethings goes wrong and restart a slice
in most case, the watcher doesn't need to be restarted if the configuration is going to stay the same
make sure the flow run for this slice are done/stopped , we dont want two flow run for the same strip running at the same time
```bash
ops lsm state reset testscan --slice SLICE_TO_RESET
```

the watcher will emit events for this slice again and reprocess them


