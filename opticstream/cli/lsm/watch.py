from __future__ import annotations
from pathlib import Path
import time
import threading
from queue import Empty, Queue

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from opticstream.config import LSMScanConfig
from opticstream.cli.lsm.cli import lsm_cli
from opticstream.events.lsm_events import STRIP_READY
from opticstream.events.lsm_event_emitters import emit_strip_lsm_event
from opticstream.state.lsm_project_state import LSMStripId, LSM_STATE_SERVICE
from opticstream.utils.filename_utils import parse_lsm_run_folder_name, parse_lsm_strip_index


def _should_process_folder(path: Path) -> bool:
    """
    Return True if the folder should be processed by the watcher.

    By default, folders must have a name that starts with "Run" (case-insensitive).
    """
    return path.name.lower().startswith("run")


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

    def __init__(
        self,
        queue: Queue,
        seen: set[Path],
        lock: threading.Lock,
    ) -> None:
        self.queue = queue
        self.seen = seen
        self.lock = lock

    def on_created(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            path = Path(event.src_path)
            if not _should_process_folder(path):
                return
            with self.lock:
                if path not in self.seen:
                    print(f"[NEW ] (watchdog) {path.name}")
                    self.seen.add(path)
                    self.queue.put(path)


def polling_scanner(
    queue: Queue,
    seen: set[Path],
    lock: threading.Lock,
    watch_dir: Path,
    poll_interval: int,
) -> None:
    """
    Polling-based fallback for environments where watchdog misses events.
    """
    while True:
        try:
            for d in watch_dir.iterdir():
                if not d.is_dir() or not _should_process_folder(d):
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
    scan_config: LSMScanConfig,
    stability_time: int,
    slice_offset: int,
    force_resend: bool,
) -> None:
    """
    Consume folders from the queue, emit ``STRIP_READY``, and ensure a PENDING strip row exists.

    Flow lifecycle (RUNNING / COMPLETED / FAILED) is handled inside ``process_strip``, not here.
    """
    while True:
        try:
            folder: Path = queue.get(timeout=1)
        except Empty:
            continue

        try:
            print(f"[WAIT] Stability check: {folder.name}")
            wait_until_stable(folder, stability_time)

            try:
                slice_index, strip_id, channel_index = parse_lsm_strip_index(
                    *parse_lsm_run_folder_name(folder.name)[1:],
                    scan_config.strips_per_slice,
                )
            except Exception as exc:
                print(f"[WARN] Skipping folder {folder.name!r}: cannot parse indices ({exc})")
                continue

            slice_id = slice_index + slice_offset
            strip_ident = LSMStripId(
                project_name=project_name,
                slice_id=slice_id,
                strip_id=strip_id,
                channel_id=channel_index,
            )

            existing = LSM_STATE_SERVICE.peek_strip(strip_ident=strip_ident)
            if existing is not None and not force_resend:
                print(
                    f"[SKIP] Strip state already exists for {strip_ident}; "
                    "not emitting STRIP_READY (use --force-resend to override)"
                )
                continue

            print(
                "[FLOW] Parsed indices for folder "
                f"name={folder.name}, slice={slice_id} (base={slice_index}, offset={slice_offset}), "
                f"strip={strip_id}, channel={channel_index}"
            )

            extra_payload: dict = {"strip_path": str(folder)}
            if force_resend:
                extra_payload["force_rerun"] = True

            print(
                f"[FLOW] Emitting STRIP_READY for project={project_name}, "
                f"slice={slice_id}, strip={strip_id}, path={folder}"
            )
            start = time.perf_counter()

            emit_strip_lsm_event(STRIP_READY, strip_ident, extra_payload=extra_payload)

            with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident):
                pass # open it so the state is created 

            elapsed = time.perf_counter() - start
            print(
                f"[FLOW] Emitted STRIP_READY for strip={strip_id} in {elapsed:.2f} s"
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"[ERROR] {folder.name}: {exc}")


def watch_lsm(
    project_name: str,
    *,
    scan_config: LSMScanConfig,
    watch_dir: Path,
    slice_offset: int = 0,
    stability_time: int = 15,
    poll_interval: int = 30,
    force_resend: bool = False,
) -> None:
    """
    Watch an LSM directory for new strip folders and emit ``STRIP_READY`` for Prefect.

    Strip lifecycle is updated by ``process_strip`` when the event-driven flow runs.

    Parameters
    ----------
    project_name:
        Project identifier (used by Prefect flows and config blocks).
    scan_config:
        `LSMScanConfig` block instance providing output/info/archive paths and zarr configuration.
    watch_dir:
        Base directory to watch for new strip folders. Only immediate subdirectories whose
        names start with "Run" (case-insensitive) are processed.
    slice_offset:
        Integer offset added to the slice index parsed from each folder name.
    stability_time:
        Seconds a folder must be unchanged before processing.
    poll_interval:
        Seconds between polling scans for new folders.
    force_resend:
        If True, emit ``STRIP_READY`` even when strip state already exists, with ``force_rerun``.
    """
    queue: Queue = Queue()
    seen: set[Path] = set()
    lock = threading.Lock()

    # Seed existing folders so we process anything already on disk.
    for d in watch_dir.iterdir():
        if d.is_dir() and _should_process_folder(d):
            seen.add(d)
            queue.put(d)

    # Watchdog observer.
    observer = Observer()
    observer.schedule(
        NewFolderHandler(queue, seen, lock),
        str(watch_dir),
        recursive=False,
    )
    observer.start()

    # Polling fallback.
    polling_thread = threading.Thread(
        target=polling_scanner,
        args=(queue, seen, lock, watch_dir, poll_interval),
        daemon=True,
    )
    polling_thread.start()

    # Consumer loop in its own thread so the main thread can handle KeyboardInterrupt.
    consumer_thread = threading.Thread(
        target=_consumer_loop,
        args=(
            queue,
            project_name,
            scan_config,
            stability_time,
            slice_offset,
            force_resend,
        ),
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


@lsm_cli.command
def watch(
    project_name: str,
    watch_dir: str = ".",
    *,
    config_block_name: str | None = None,
    slice_offset: int = 0,
    stability_seconds: int = 15,
    poll_interval: int = 30,
    force_resend: bool = False,
) -> None:

    scan_config = LSMScanConfig.load(config_block_name or f"{project_name}-lsm-config")

    watch_dir = Path(watch_dir)

    if not watch_dir.exists():
        raise ValueError(f"Watch directory {watch_dir} does not exist")

    watch_lsm(
        project_name=project_name,
        scan_config=scan_config,
        watch_dir=watch_dir,
        slice_offset=slice_offset,
        stability_time=stability_seconds,
        poll_interval=poll_interval,
        force_resend=force_resend,
    )
