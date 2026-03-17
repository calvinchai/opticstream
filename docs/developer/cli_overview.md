---
title: CLI overview
---

# CLI overview

The OpticStream CLI is implemented under `opticstream.cli` and is exposed as
the `opticstream` command (with a short alias `ops`) for deploying and
operating Prefect flows.

Key entrypoints include:

- deployment helpers in `opticstream.cli.deploy_cmds`
- additional subcommands in other `opticstream.cli.*` modules

Typical invocations look like:

```bash
opticstream --help      # or: ops --help
opticstream deploy ...  # or: ops deploy ...
```

This page should provide examples of common CLI invocations and how they map
to underlying flows and deployments.

