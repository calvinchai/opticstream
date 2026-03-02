import subprocess
import time
import threading
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from queue import Queue, Empty
import os

# ===================== API KEYS =====================
os.environ["DANDI_API_KEY"] = "b725989a1915babea90beb65fa56ab272eb313c7"
os.environ["LINC_API_KEY"] = "b725989a1915babea90beb65fa56ab272eb313c7"
# ===================== CONFIG =====================

BASE_DIR = Path(r"E:\temp\2025_02_19_Compress_Test")
INFO_FILE = Path(r"E:\temp\2025_02_19_Compress_Test_info.mat")

OUTPUT_DIR = Path(r"E:\temp\2025_02_19_Compress_Test_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAX_WORKERS = 4
START_DELAY = 20
STABILITY_TIME = 15
POLL_INTERVAL = 30

# ===================== PROCESSING =====================

def run_linc_convert(run_dir: Path):
    start_time = time.perf_counter()

    output_zarr = OUTPUT_DIR / f"{run_dir.name}.ome.zarr"
    if output_zarr.exists():
        print(f"[SKIP ] {run_dir.name} already converted")
        return run_dir.name, 0.0

    cmd = [
        "linc-convert", "lsm", "strip",
        "--inp", str(run_dir),
        "--info_file", str(INFO_FILE),
        "--chunk", "128",
        "--out", str(output_zarr),
        "--overwrite",
        "--driver", "tensorstore",
        "--shard", "1024",
    ]

    print(f"[START] {run_dir.name}")
    subprocess.run(cmd, check=True)
    # ---------- Upload ----------
    upload_cmd = [
        "dandi", "upload", "-i", "linc",
        "-J", "15:15",
        str(output_zarr),
    ]

    print(f"[UPLOAD] {output_zarr.name}")
    subprocess.run(upload_cmd, check=True)

    elapsed = time.perf_counter() - start_time
    print(f"[DONE ] {run_dir.name} — convert + upload — {elapsed:.2f} s")

    return run_dir.name, elapsed
    elapsed = time.perf_counter() - start_time
    print(f"[DONE ] {run_dir.name} — {elapsed:.2f} s")

    return run_dir.name, elapsed

# ===================== STABILITY CHECK =====================

def wait_until_stable(folder: Path, stable_seconds: int):
    last_change = time.time()

    def snapshot():
        return [(p.name, p.stat().st_mtime) for p in folder.rglob("*") if p.is_file()]

    prev = snapshot()

    while True:
        time.sleep(2)
        curr = snapshot()
        if curr != prev:
            last_change = time.time()
            prev = curr
        elif time.time() - last_change >= stable_seconds:
            return

# ===================== WATCHDOG HANDLER =====================

class NewFolderHandler(FileSystemEventHandler):
    def __init__(self, queue, seen, lock):
        self.queue = queue
        self.seen = seen
        self.lock = lock

    def on_created(self, event):
        if event.is_directory:
            path = Path(event.src_path)
            with self.lock:
                if path not in self.seen:
                    print(f"[NEW ] (watchdog) {path.name}")
                    self.seen.add(path)
                    self.queue.put(path)

# ===================== POLLING FALLBACK =====================

def polling_scanner(queue, seen, lock):
    while True:
        try:
            for d in BASE_DIR.iterdir():
                if not d.is_dir():
                    continue
                with lock:
                    if d not in seen:
                        print(f"[NEW ] (polling ) {d.name}")
                        seen.add(d)
                        queue.put(d)
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            print(f"[WARN] Polling error: {e}")
            time.sleep(POLL_INTERVAL)

# ===================== CONSUMER =====================

def consumer(queue: Queue, executor: ProcessPoolExecutor):
    while True:
        try:
            folder = queue.get(timeout=1)
        except Empty:
            continue

        try:
            print(f"[WAIT] Stability check: {folder.name}")
            wait_until_stable(folder, STABILITY_TIME)

            executor.submit(run_linc_convert, folder)
            time.sleep(START_DELAY)

        except Exception as e:
            print(f"[ERROR] {folder.name}: {e}")

# ===================== MAIN =====================

def main():
    queue = Queue()
    seen = set()
    lock = threading.Lock()

    # Seed existing folders
    for d in BASE_DIR.iterdir():
        if d.is_dir():
            seen.add(d)
            queue.put(d)

    executor = ProcessPoolExecutor(max_workers=MAX_WORKERS)

    # Watchdog
    observer = Observer()
    observer.schedule(
        NewFolderHandler(queue, seen, lock),
        str(BASE_DIR),
        recursive=False,
    )
    observer.start()

    # Polling fallback
    polling_thread = threading.Thread(
        target=polling_scanner,
        args=(queue, seen, lock),
        daemon=True,
    )
    polling_thread.start()

    # Consumer
    consumer_thread = threading.Thread(
        target=consumer,
        args=(queue, executor),
        daemon=True,
    )
    consumer_thread.start()

    print("[INFO] Real-time conversion running (watchdog + polling)")
    print("[INFO] Press Ctrl+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down...")

    observer.stop()
    observer.join()
    executor.shutdown(wait=True)

def watch_lsm(
    base_dir: Path,
    info_file: Path,
    output_dir: Path,
    max_workers: int = 4,
    start_delay: int = 20,
    stability_time: int = 15,
    poll_interval: int = 30,
) -> None:
    """
    Watch an LSM directory and process the strips.
    """
    queue = Queue()
    seen = set()
    lock = threading.Lock()

    # Seed existing folders
    for d in base_dir.iterdir():
        if d.is_dir():
            seen.add(d)
            queue.put(d)