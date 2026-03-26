---
title: OCT design overview
---

# OCT design overview

> Audience: advanced users and maintainers who need a detailed view of the
> distributed OCT pipeline. For a high-level introduction to OpticStream and
> its components, see the
> [project overview](../concepts/project_overview.md) and
> [architecture](architecture.md) pages.

## Overview

This document describes the design of a distributed, event-driven data
acquisition and real-time processing system for Optical Coherence
Tomography (OCT) data. The system uses **Prefect** as the workflow
orchestration engine and supports scalable, fault-tolerant processing
across tiles, mosaics, slices, and whole datasets.

The primary goals of the system are:

* Efficient ingestion and integrity validation of raw OCT data
* Near–real-time processing with minimal redundant I/O
* Clear separation of processing stages with event-based coordination
* Scalable execution across heterogeneous compute hosts
* Transparent progress monitoring, QC reporting, and data publication

