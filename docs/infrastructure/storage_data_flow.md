# Storage and data flow

Acquisition hosts **Willow** and **Cedar**, and processing host **Zircon**, connect to the storage system over **SAS** (host HBAs to the storage server or expander fabric).

```
                         ┌──────────────────────────┐
                         │      Storage server       │
                         └───────────┬──────────────┘
                                     │
                        SAS (HBAs / multipath as deployed)
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
              │ SAS                  │ SAS                  │ SAS
              ▼                      ▼                      ▼
     ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
     │ Willow          │   │ Cedar           │   │ Zircon          │
     │ (acquisition)   │   │ (acquisition)   │   │ (processing)    │
     └─────────────────┘   └─────────────────┘   └─────────────────┘
```

## Host networking (25G vSwitch on Zircon, 10Gb wall)

**Willow**, **Cedar**, **Zircon**, and **…** (fourth host, such as Maple) all **connect to Zircon** for east–west traffic: a **25 Gbps virtual switch** runs **on Zircon**, and every host has a **25 Gbps** leg into that vSwitch (Zircon’s own participation is on-box). Separately, **each host** has its own **10 Gbps** link to the building **wall** (patch / upstream).

```
                         ┌──────────────────────────────────┐
                         │   10Gb wall (building uplink)   │
                         └───┬────────┬────────┬────────┬───┘
                             │        │        │        │
                          10Gb    10Gb    10Gb    10Gb
                             │        │        │        │
                          Willow   Cedar   Zircon    …
                             │        │        │        │
                             └──25G───┴──25G───┴──25G───┴──25G──┘
                                         │
                                ┌────────┴────────┐
                                │ Zircon          │
                                │ vSwitch 25 Gbps │
                                └─────────────────┘
```

East–west traffic between hosts uses the **25 Gbps vSwitch on Zircon** (hub: all hosts → Zircon). North–south: **10 Gbps** to the **wall** per host, independent of the vSwitch path (exact VLANs or ports per your network plan).

## SMB file sharing (Zircon reads data on other hosts)

**Zircon** reaches filesystems on the other hosts **over the same east–west path** (the **25 Gbps** fabric to Zircon’s vSwitch, not over SAS). Hosts that hold data **publish folders as SMB shares** (Windows file sharing: SMB/CIFS). **Zircon** mounts or accesses those shares as an **SMB client** so pipelines and services on Zircon can read (or write, if permitted) files as if they were local or UNC paths, without moving bulk data off the acquisition machines first.

Typical pattern: **Willow** / **Cedar** (and other data hosts as needed) run an **SMB server** and share the relevant disk or directory; **Zircon** connects with credentials and uses Windows UNC paths (for example `\\Willow\Acquisition`) or Linux `//Willow/Acquisition` mounts (`cifs`, `smbclient`) over **25 Gbps**, which keeps latency and throughput suitable for large sequential reads compared to routing that traffic out the **10 Gbps wall** uplink.

Operational notes: align **firewall** rules and **SMB signing** policy with your site; for Linux servers use **Samba** for SMB shares; ensure **service accounts** and **share permissions** match what processing jobs on Zircon expect.

## Shared volume on acquisition and processing (dual attach, storage-mapped RO on Zircon)

A less common option is to **present the same volume** (same LUN or equivalent) to **both** an acquisition host (**Willow** / **Cedar**) and **Zircon**. In many environments that is **discouraged or disallowed** when two hosts could **both write** the same filesystem.

Here the **storage server** defines two **mappings** for that volume: the **acquisition host** sees it **read–write** so capture can write **NTFS** (or the host OS) normally. **Zircon** is given a **read-only** mapping to the **same** volume—enforced **below the OS** by the **array / presentation** (SCSI permissions, LUN mask, or equivalent), so **Zircon cannot write** even if a client tried. Only **one** host mutates data; **Zircon** is a **reader**.

**Upside:** Reads on **Zircon** go over **SAS/block** with **lower overhead** and often **better throughput** than **SMB** over the 25 Gbps network.

**Downside:** **Zircon** does **not** see changes in **real time** the way a network share can; metadata and cache may lag. Operators sometimes need to **unmount and remount** on **Zircon** (or refresh the mount) after new data lands on the acquisition side.

**Protect acquisition:** Even read-heavy workloads on **Zircon** can **contend** with the array or paths the acquisition host relies on. **Throttle** sequential readers, **limit parallelism**, and **cap IOPS/bandwidth** on the processing host so **Zircon** does not **read fast enough to interfere** with ongoing capture (latency spikes, dropped frames, or storage saturation). Treat this as a **shared resource budget**, not “read as fast as possible.”
