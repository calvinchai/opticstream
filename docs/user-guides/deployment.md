---
title: Deployment
---

# Deployment

The `opticstream deploy` CLI command (or its short alias `ops deploy`) helps
you create Prefect deployments for the main OpticStream workflows.

Under the hood it uses the mapping defined in `opticstream.cli.deploy_cmds`,
which associates logical flow names (such as `mosaic`, `volume-stitching`,
`state-management`, `upload`, `slice-registration`, `slack-notification`) with
their backing Prefect deployment specs.

Use `opticstream deploy --help` (or `ops deploy --help`) for a full list of
supported flows and options.

