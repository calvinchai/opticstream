---
title: Deployment
---

# Deployment

OpticStream supports multiple deployment and serving paths depending on whether
you want to register deployments in Prefect or run local workers directly.

## Top-level deployment helper

Use `opticstream deploy` (or `ops deploy`) to deploy one logical flow group.

```bash
opticstream deploy FLOW_NAME
```

Valid `FLOW_NAME` values:

- `mosaic`
- `volume-stitching`
- `state-management`
- `upload`
- `slice-registration`
- `slack-notification`

Useful options:

- `--dry-run` to print the underlying deploy command(s)
- `--prefect-args ...` to pass through extra Prefect CLI arguments

## LSM local serving

Use `opticstream lsm serve` to run LSM deployments with `prefect.serve`:

```bash
opticstream lsm serve --concurrent-workers 2
```

This serves strip/channel processing and upload/update event flows.

## PS-OCT deployment and serving

For PS-OCT, use `opticstream oct deploy` to build deployments from source:

```bash
opticstream oct deploy --project-name all --deployment-name dynamic --work-pool-name psoct
```

For local serving paths:

```bash
opticstream oct serve register --project-name all --deployment-name local
opticstream oct serve all PROJECT_NAME --deployment-name local
```

- `oct serve register` serves registration and dependent flows.
- `oct serve all` serves the broader OCT flow set for a specific project.

## Validation step

Always run command help against your current install before deployment:

```bash
opticstream --help
opticstream deploy --help
opticstream lsm serve --help
opticstream oct deploy --help
opticstream oct serve --help
```

