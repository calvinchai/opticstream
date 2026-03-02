from __future__ import annotations

import threading
import time
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

from cyclopts import App
from niizarr.multizarr import ZarrConfig
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from opticstream.cli.watch.cli import watch_cli
from opticstream.config.blocks import LSMScanConfig
from opticstream.flows.lsm.process_strip_flow import process_strip_flow


def wait_until_stable(folder: Path, stable_seconds: int) -> None:
    """
    Block until the given folder has had no file modifications for
    ``stable_seconds``.
    """
    last_change = time.time()

    def snapshot() -> list[tuple[str, float]]:
        return [
            (p.name, p.stat().st_mtime)
            for p in folder.rglob("*")
            if p.is_file()
        ]

    prev = snapshot()

    while True:
        time.sleep(2)
        curr = snapshot()
        if curr != prev:
            last_change = time.time()
            prev = curr
        elif time.time() - last_change >= stable_seconds:
            return


class NewFolderHandler(FileSystemEventHandler):
    """
    Watchdog handler that enqueues newly created subdirectories exactly once.
    """

    def __init__(self, queue: Queue, seen: set[Path], lock: threading.Lock) -> None:
        self.queue = queue
        self.seen = seen
        self.lock = lock

    def on_created(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            path = Path(event.src_path)
            with self.lock:
                if path not in self.seen:
                    print(f"[NEW ] (watchdog) {path.name}")
                    self.seen.add(path)
                    self.queue.put(path)


def polling_scanner(
    queue: Queue,
    seen: set[Path],
    lock: threading.Lock,
    base_dir: Path,
    poll_interval: int,
) -> None:
    """
    Polling-based fallback for environments where watchdog misses events.
    """
    while True:
        try:
            for d in base_dir.iterdir():
                if not d.is_dir():
                    continue
                with lock:
                    if d not in seen:
                        print(f"[NEW ] (polling ) {d.name}")
                        seen.add(d)
                        queue.put(d)
            time.sleep(poll_interval)
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"[WARN] Polling error: {exc}")
            time.sleep(poll_interval)


def _consumer_loop(
    queue: Queue,
    project_name: str,
    slice_number: int,
    scan_config: LSMScanConfig,
    stability_time: int,
    strip_start: int,
) -> None:
    """
    Consume folders from the queue and dispatch Prefect flows.
    """
    next_strip_number = strip_start

    base_output = Path(scan_config.output_path)
    info_file = Path(scan_config.info_file)
    archive_path: Optional[Path] = (
        Path(scan_config.archive_path) if scan_config.archive_path is not None else None
    )

    while True:
        try:
            folder: Path = queue.get(timeout=1)
        except Empty:
            continue

        try:
            print(f"[WAIT] Stability check: {folder.name}")
            wait_until_stable(folder, stability_time)

            strip_number = next_strip_number
            next_strip_number += 1

            print(
                f"[FLOW] Starting process_strip_flow for "
                f"project={project_name}, slice={slice_number}, strip={strip_number}, "
                f"path={folder}"
            )
            start = time.perf_counter()

            process_strip_flow(
                project_name=project_name,
                slice_number=slice_number,
                strip_number=strip_number,
                strip_path=str(folder),
                output_path=str(base_output),
                info_file=str(info_file),
                zarr_config=scan_config.zarr_config,
                output_format=scan_config.output_format,
                output_mip_format=scan_config.output_mip_format,
                archive_path=str(archive_path) if archive_path is not None else None,
                output_mip=scan_config.output_mip,
                delete_strip=scan_config.delete_strip,
            )

            elapsed = time.perf_counter() - start
            print(
                f"[FLOW] Completed process_strip_flow for strip={strip_number} "
                f"in {elapsed:.2f} s"
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"[ERROR] {folder.name}: {exc}")


def watch_lsm(
    project_name: str,
    slice_number: int,
    *,
    scan_config: LSMScanConfig,
    strip_start: int = 1,
    stability_time: int = 15,
    poll_interval: int = 30,
) -> None:
    """
    Watch an LSM directory for new strip folders and dispatch Prefect flows.

    Parameters
    ----------
    project_name:
        Project identifier (used by Prefect flows and config blocks).
    slice_number:
        Slice index for which strips are being acquired.
    scan_config:
        `LSMScanConfig` block instance providing base paths and zarr configuration.
    strip_start:
        Starting strip number; incremented for each new folder discovered.
    stability_time:
        Seconds a folder must be unchanged before processing.
    poll_interval:
        Seconds between polling scans for new folders.
    """
    base_dir = Path(scan_config.project_base_path)

    queue: Queue = Queue()
    seen: set[Path] = set()
    lock = threading.Lock()

    # Seed existing folders so we process anything already on disk.
    for d in base_dir.iterdir():
        if d.is_dir():
            seen.add(d)
            queue.put(d)

    # Watchdog observer.
    observer = Observer()
    observer.schedule(
        NewFolderHandler(queue, seen, lock),
        str(base_dir),
        recursive=False,
    )
    observer.start()

    # Polling fallback.
    polling_thread = threading.Thread(
        target=polling_scanner,
        args=(queue, seen, lock, base_dir, poll_interval),
        daemon=True,
    )
    polling_thread.start()

    # Consumer loop in its own thread so the main thread can handle KeyboardInterrupt.
    consumer_thread = threading.Thread(
        target=_consumer_loop,
        args=(queue, project_name, slice_number, scan_config, stability_time, strip_start),
        daemon=True,
    )
    consumer_thread.start()

    print("[INFO] LSM watcher running (watchdog + polling)")
    print("[INFO] Press Ctrl+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down LSM watcher...")

    observer.stop()
    observer.join()


lsm_cli = watch_cli.command(App(name="lsm"))


@lsm_cli.command
def run(
    project_name: str,
    slice_number: int,
    *,
    config_block_name: Optional[str] = None,
    strip_start: int = 1,
    stability_seconds: int = 15,
    poll_interval: int = 30,
) -> None:
    """
    Watch an LSM acquisition directory and dispatch Prefect strip-processing flows.

    This command relies on an `LSMScanConfig` Prefect Block to provide paths and
    zarr configuration. By default, it loads a block named
    `\"{project_name}-lsm-config\"` unless `config_block_name` is provided.
    """
    scan_config = LSMScanConfig.load(f"{project_name}-lsm-config")

    base_dir = Path(scan_config.project_base_path)
    if not base_dir.exists():
        raise ValueError(
            f"Project base path '{base_dir}' does not exist. "
            "Run 'opticstream watch lsm setup-dirs' first or create it manually."
        )

    watch_lsm(
        project_name=project_name,
        slice_number=slice_number,
        scan_config=scan_config,
        strip_start=strip_start,
        stability_time=stability_seconds,
        poll_interval=poll_interval,
    )


@lsm_cli.command
def setup(
    project_name: str,
    *,
    config_block_name: Optional[str] = None,
    zarr_config: ZarrConfig,
    project_base_path: str,
    info_file: str,
    output_path: str,
    output_mip: Optional[bool] = None,
    output_format: Optional[str] = None,
    output_mip_format: Optional[str] = None,
    archive_path: Optional[str] = None,
    delete_strip: Optional[bool] = None,
    dandi_bin: Optional[str] = None,
    dandi_instance: Optional[str] = None
) -> None:
    """
    Create and verify directories used by the LSM watcher.

    Uses `LSMScanConfig` to determine:
    - `project_base_path` (directory to watch for strip folders)
    - `output_path` (compressed strip outputs)
    - `archive_path` (optional backup location)
    - parent directory of `info_file`
    """
    block_name = config_block_name or f"{project_name}-lsm-config"
    LSMScanConfig.register_type_and_schema()
    config = {
        "project_base_path": project_base_path,
        "info_file": info_file,
        "output_path": output_path,
        "output_mip": output_mip,
        "output_format": output_format,
        "output_mip_format": output_mip_format,
        "archive_path": archive_path,
        "delete_strip": delete_strip,
        "dandi_bin": dandi_bin,
        "dandi_instance": dandi_instance,
        "zarr_config": zarr_config,
    }
    for key, value in config.items():
        if value is None:
            del config[key]
    scan_config = LSMScanConfig(**config)
    scan_config.save(f"{project_name}-lsm-config")

    created: list[Path] = []
    verified: list[Path] = []

    def _ensure_dir(path: Optional[str]) -> None:
        if not path:
            return
        p = Path(path)
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(p)
        else:
            verified.append(p)

    _ensure_dir(scan_config.project_base_path)
    _ensure_dir(scan_config.output_path)
    _ensure_dir(scan_config.archive_path)

    # Ensure parent directory of the info file exists so users can place it there.
    if scan_config.info_file:
        info_parent = Path(scan_config.info_file).parent
        _ensure_dir(str(info_parent))

    if created:
        print("Created directories:")
        for p in created:
            print(f"  - {p}")
    if verified:
        print("Verified existing directories:")
        for p in verified:
            print(f"  - {p}")
    if not created and not verified:
        print("No directories to create or verify from LSMScanConfig.")
