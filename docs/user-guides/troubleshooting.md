---
title: Troubleshooting
---

# Troubleshooting

This page collects common issues encountered when running OpticStream flows and
suggested ways to diagnose them.

## Block load errors

Symptoms:

- `LSMScanConfig.load(...)` or `PSOCTScanConfig.load(...)` fails
- setup/watch commands fail immediately with missing block messages

Checks:

- run `opticstream init blocks`
- run `opticstream lsm setup PROJECT_NAME` or `opticstream oct setup PROJECT_NAME`
- verify block names follow project naming conventions used by setup commands

## State lock or state backend issues

Symptoms:

- flows hang while opening state contexts (`open_*`)
- state updates fail with lock/backing-store errors

Checks:

- ensure project lock exists (`opticstream lsm create-lock PROJECT_NAME` or
  `opticstream oct create-lock PROJECT_NAME`)
- verify PostgreSQL connector block used by the state repository is accessible
- confirm the state table exists (`project_state` by default)

## File discovery and permission issues

Symptoms:

- watcher runs but detects nothing
- watcher logs unreadable paths or stability never reached

Checks:

- verify watch root exists and is readable by the running user
- confirm expected file/folder patterns are present:
  - LSM: immediate `Run*` subfolders
  - OCT: spectral files matching watcher expectations
- tune `--stability-seconds` and `--poll-interval` to acquisition behavior

## MATLAB / PS-OCT processing failures

Symptoms:

- batch or slice processing fails during MATLAB invocation
- processed outputs are missing after dispatch

Checks:

- verify `matlab_root` and processing params in `PSOCTScanConfig`
- verify psoct-toolbox dependency is installed and importable
- check upstream spectral inputs and batch completeness

## Output validation and size issues

Symptoms:

- generated zarr/volume outputs are unexpectedly small or absent
- channel volume validation fails

Checks:

- verify `output_path`, naming formats, and write permissions
- inspect `zarr_config` and `channel_volume_zarr_size_threshold`
- if using placeholder volume outputs, ensure
  `skip_channel_volume_zarr_validation` is configured as intended

## Upload and external service failures

Symptoms:

- DANDI/LINC upload tasks fail
- authentication/authorization errors in upload flows

Checks:

- verify API token blocks (`dandi-api-key`, `linc-api-key`) are present
- verify configured `dandi_instance` and destination paths
- re-run with verbose Prefect logs to isolate network vs auth failures

