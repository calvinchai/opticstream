---
title: Flows and state models
---

# Flows and state models

OpticStream orchestrates its processing logic using Prefect flows.

For LSM processing, the `process_strip_flow` in
`opticstream.flows.lsm.process_strip_flow` coordinates compression, backup,
validation, and optional cleanup for a single strip.

Additional state models and event-driven flows (see `opticstream.flows.*`)
connect these building blocks into higher-level pipelines, reacting to events
such as new data availability or completed mosaics.

