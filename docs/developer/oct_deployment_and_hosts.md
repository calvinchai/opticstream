---
title: OCT processing hosts and deployment
---

# OCT processing hosts and deployment

## Host architecture

The system uses a hybrid host strategy to optimize for different workload characteristics:

* **Zircon (primary host)**
  * Primary data processing host
  * High I/O workflows (tile processing, mosaics, file operations)
  * Large-memory tasks (stitching, volume processing)
  * Prefect server and scheduler
  * Local high-speed SSDs for processing

* **Auxiliary hosts**
  * Compute-heavy, low-I/O tasks (e.g., registration)
  * Deployments connected to Zircon Prefect server
  * Can offload compute-intensive work from Zircon
  * Useful for tasks that don't require high I/O bandwidth

This hybrid strategy maximizes throughput while minimizing contention on shared storage and network resources.

## Work pool strategy

Different work pools are configured for different task types to optimize resource allocation:

* **Tile processing pool**: High parallelism, CPU-intensive, moderate memory
  * Handles MATLAB batch processing, spectral-to-complex conversion
  * Many concurrent workers for parallel tile processing
* **Stitching pool**: Moderate parallelism, memory-intensive
  * Handles mosaic stitching operations
  * Fewer workers due to memory requirements
* **Registration pool**: Low parallelism, CPU-intensive, high memory
  * Handles slice registration (compute-heavy)
  * Can run on auxiliary hosts
* **Upload pool**: High parallelism, I/O-bound
  * Handles cloud storage uploads
  * Many concurrent workers for parallel uploads

Work pools enable fine-grained control over resource allocation and can be tuned based on workload characteristics.

## Agent deployment

Prefect agents run on processing hosts and pull work from work pools:

* **Zircon agent**: Pulls from tile processing, stitching, and upload pools
* **Auxiliary host agents**: Pull from registration pool and other compute-heavy pools
* **Connection**: All agents connect to Zircon Prefect server
* **Reliability**: Agents run as systemd services or Docker containers for auto-restart

Agents automatically pull work from their assigned pools and execute flows on the local host, enabling distributed execution while maintaining centralized orchestration.

