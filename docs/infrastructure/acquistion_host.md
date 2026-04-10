---
title: LSM Acquisition Host
---

# LSM acquisition host

The LSM acquisition host is the machine that runs both the LSM microscope acquisition software and the OpticStream real-time processing pipeline simultaneously. Because the acquisition software is latency-sensitive — a backlogged acquisition process will drop frames — the host must be tuned to ensure sufficient CPU headroom is reserved exclusively for acquisition.

## Host responsibilities

- Runs the vendor acquisition software, which writes raw strip folders to a local or network path.
- Runs the OpticStream LSM watcher (`opticstream lsm watch`) and Prefect worker, which detect new strips and trigger processing flows in real time.
- Performs CPU-intensive work such as Zarr compression, MIP generation, and optional strip archival while acquisition is ongoing.

## Performance optimisations

### 1. Disable hyperthreading (BIOS)

Hyperthreading should be disabled in the server BIOS. With hyperthreading enabled, two logical cores share one physical core's execution resources, which causes unpredictable latency spikes under the mixed real-time / batch load typical of an acquisition session. Disabling it gives each logical core exclusive access to its physical core and makes CPU scheduling more deterministic.

This setting is found under the processor or advanced CPU configuration section of the BIOS/UEFI, often labelled **Hyper-Threading**, **Simultaneous Multithreading (SMT)**, or similar. It must be set before the OS boots; no OS-level change is needed.

### 2. Limit CPU affinity for processing workers

Even with hyperthreading disabled, the processing flows must not be allowed to compete with the acquisition software for CPU time. OpticStream enforces this by pinning its Prefect worker processes to a subset of CPU cores using the `cpu_affinity` field in the `LSMScanConfig` block.

**How it works:** the `cpu_affinity` setting is passed directly to the underlying worker processes (e.g. via `os.sched_setaffinity` or equivalent), restricting them to the listed cores. The acquisition software runs on the remaining cores and is never preempted by processing work.

**Example — host Willow (28 physical cores, 0-indexed):**

Cores 0–13 are reserved for the acquisition software. The processing workers are pinned to cores 14–27:

```yaml
cpu_affinity: [14, 28]
```

Set this in the project's `LSMScanConfig` Prefect block via the Prefect UI:

1. Open the Prefect dashboard at `http://<HOST_IP>:4200`.
2. Navigate to **Blocks** and open the `LSMScanConfig` block for the project.
3. Set the `cpu_affinity` field to the list of core indices the processing workers should use, e.g. `[14, 28]`.
4. Save the block.

As a rule of thumb, give the acquisition software roughly half the physical cores and let the processing pipeline use the other half. Adjust the split based on observed acquisition CPU usage — if the acquisition process starts queueing or dropping frames, expand the reserved range.

## Monitoring

Use **Windows Task Manager** to verify CPU load is distributed as expected during an active acquisition session:

1. Open Task Manager (`Ctrl+Shift+Esc`) and go to the **Performance** tab.
2. Click **CPU**, then right-click the CPU graph and select **Change graph to → Logical processors**. Each core is shown as a separate tile.
3. Confirm that the acquisition software cores (0–13 on Willow) stay at low-to-moderate utilisation, while the processing cores (14–27 on Willow) are free to spike to 100 % during Zarr compression without affecting acquisition.

If frame drops are observed despite correct affinity settings, check whether any other system process (e.g. Windows Update, antivirus, background services) is also running on the cores reserved for acquisition and reschedule or disable those tasks if possible.
8