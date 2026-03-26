---
title: OCT future extensions
---

# OCT future extensions

## MATLAB to Python migration

Planned improvements include:

* **Replace MATLAB steps**: Migrate spectral-to-complex and complex-to-processed conversions to Python-native implementations
* **Eliminate batch processing**: Python can process tiles individually more efficiently, eliminating need for batch processing optimization
* **Maintainability**: Single language codebase improves maintainability
* **Performance**: Potential performance improvements by eliminating MATLAB startup overhead

## Enhanced retry policies

Possible future work:

* **Flag file-based retries**: Add retry policies that check flag-file state before retrying
* **Intelligent retries**: Retry only failed work, not completed work
* **Exponential backoff**: Implement exponential backoff for retries
* **Failure analysis**: Analyze failure patterns to improve retry strategies

## Real-time dashboarding

Ideas for richer observability beyond Prefect UI:

* **Additional dashboards**: Integrate real-time dashboarding tools (e.g., Grafana, custom dashboards)
* **Processing metrics**: Track processing metrics (throughput, latency, error rates)
* **Resource monitoring**: Monitor resource utilization (CPU, memory, disk, network)
* **Custom visualizations**: Custom visualizations for processing progress and QC results

## Summary

The OCT design provides a robust, scalable framework for distributed OCT data processing. The system is built on several key architectural principles:

* **Event-driven orchestration**: Downstream flows are triggered by events, not synchronous waits, enabling decoupling, scalability, and resilience
* **Hierarchical data organization**: Clear hierarchy of Tile → Mosaic → Slice → All-Slices, with tiles as the atomic data unit
* **MATLAB batch processing optimization**: Batch processing exists as an implementation detail for MATLAB efficiency, not as a data hierarchy level
* **Flag file-based state management**: Flag files serve as the source of truth for processing state, enabling idempotency and crash recovery
* **Hybrid host strategy**: Optimize for different workload characteristics (high I/O vs compute-intensive)

