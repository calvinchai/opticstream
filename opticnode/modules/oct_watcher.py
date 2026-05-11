"""OCT polling watcher: stable batches enqueue RQ jobs (not Prefect events)."""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import Field
from redis import Redis
from rq import Queue

from opticstream.cli.oct.watch import OCTBatchCandidate, OCTWatcherService
from opticstream.config.psoct_scan_config import PSOCTScanConfigModel, get_psoct_scan_config
from opticstream.state import OCT_STATE_SERVICE
from opticapi.project_state.oct_models import OCTBatchId
from opticstream.utils.polling_watcher import PollingStableWatcher

from opticnode.modules.base import ModuleConfig, NodeModule
from opticnode.modules.worker import OctBatchTask, WorkerConfig, _PROCESS_FN

logger = logging.getLogger(__name__)


def _parse_mosaic_ranges(mosaic_ranges_str: str) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for range_str in mosaic_ranges_str.split(","):
        parts = range_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid mosaic range format: {range_str!r}. Expected 'min:max'"
            )
        out.append((int(parts[0]), int(parts[1])))
    return out


class RedisOCTWatcherService(OCTWatcherService):
    def __init__(self, *, rq_queue: Queue, worker_config: WorkerConfig, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._rq_queue = rq_queue
        self._worker_config = worker_config

    def _process_event(self, candidate: OCTBatchCandidate, batch_ident: Any) -> int:
        bid = (
            batch_ident
            if isinstance(batch_ident, OCTBatchId)
            else OCTBatchId.model_validate(batch_ident)
        )
        batch_mtime = max(p.stat().st_mtime for p in candidate.files)
        task = OctBatchTask(
            batch_id=bid,
            file_list=[str(x) for x in candidate.files],
            force_rerun=self.force_resend,
            timestamp=datetime.fromtimestamp(batch_mtime),
        )
        self._rq_queue.enqueue(
            _PROCESS_FN, task.model_dump(mode="python"), self._worker_config.model_dump()
        )
        with OCT_STATE_SERVICE.open_batch(batch_ident=bid):
            pass
        logger.info("Enqueued OCT batch job for %s (%s files)", bid, len(task.file_list))
        return 1


class OCTWatcherConfig(ModuleConfig):
    watch_path: str = Field(default="")
    poll_interval: int = Field(default=5, ge=1)
    stability_seconds: int = Field(default=15, ge=0)
    force_resend: bool = False
    project_name: str = Field(default="")
    slice_offset: int = Field(default=0)
    mosaic_ranges: str = Field(default="1:999999")
    project_base_path: str = Field(default="")
    min_complex_file_size_bytes: int = Field(default=1, ge=0)
    prefer_spectral_for_complex_with_spectral: bool = True
    prefect_deployment: str = Field(default="")
    allowed_window_minutes: float = Field(default=10, ge=0)


class OCTWatcherModule(NodeModule):
    """Polls for stable OCT batches and enqueues RQ jobs."""

    name = "oct_watcher"
    Config = OCTWatcherConfig

    def __init__(self, redis_url: str) -> None:
        super().__init__()
        self._redis_url = redis_url
        self._thread: threading.Thread | None = None
        self._poll_stop = threading.Event()

    def _make_rq_queue(self, queue_name: str) -> Queue:
        conn = Redis.from_url(self._redis_url, decode_responses=False)
        return Queue(queue_name, connection=conn)

    def _poll_loop(self, watcher: PollingStableWatcher[Any, Any], poll_interval: int) -> None:
        iteration = 0
        while not self._poll_stop.is_set():
            iteration += 1
            logger.info("--- oct_watcher iteration %s ---", iteration)
            try:
                watcher._run_iteration()
            except Exception:
                logger.exception("OCT watcher iteration failed")
            if self._poll_stop.wait(timeout=float(poll_interval)):
                break
        logger.info("OCT watcher poll loop exited after %s iterations", iteration)

    def _launch(self, config: OCTWatcherConfig) -> None:
        watch_path = config.watch_path.strip()
        if not watch_path:
            raise ValueError("OCTWatcherModule requires a non-empty 'watch_path'.")
        pn = config.project_name.strip()
        if not pn:
            raise ValueError("OCTWatcherModule requires a non-empty 'project_name'.")

        p = Path(watch_path)
        if not p.is_dir():
            raise ValueError(f"OCTWatcherModule: watch_path is not a directory: {watch_path}")

        pbp = config.project_base_path.strip()
        if not pbp:
            raise ValueError("OCTWatcherModule requires a non-empty 'project_base_path'.")

        worker_config = WorkerConfig(
            project_name=pn,
            deployment_name=config.prefect_deployment,
            queue_kind="oct",
            allowed_window_minutes=config.allowed_window_minutes,
            redis_url=self._redis_url,
        )
        rq_queue = self._make_rq_queue(worker_config.queue_name)
        self._poll_stop.clear()
        project_config = get_psoct_scan_config(pn)
        scan_config = PSOCTScanConfigModel.model_validate(project_config.model_dump())
        batch_size = project_config.acquisition.grid_size_y
        service = RedisOCTWatcherService(
            rq_queue=rq_queue,
            worker_config=worker_config,
            project_name=pn,
            folder_path=p,
            project_base_path=pbp,
            mosaic_ranges=_parse_mosaic_ranges(config.mosaic_ranges),
            slice_offset=config.slice_offset,
            batch_size=batch_size,
            scan_config=scan_config,
            direct=False,
            force_resend=config.force_resend,
            refresh_hook=None,
            min_complex_file_size_bytes=config.min_complex_file_size_bytes,
            prefer_spectral_for_complex_with_spectral=config.prefer_spectral_for_complex_with_spectral,
        )
        watcher = PollingStableWatcher(
            discover_candidates=service.discover_candidates,
            candidate_key=service.candidate_key,
            fingerprint=service.fingerprint,
            process=service.process,
            poll_interval=config.poll_interval,
            stability_seconds=config.stability_seconds,
            running_message="OCT Redis watcher (polling)",
        )

        t = threading.Thread(
            target=self._poll_loop,
            args=(watcher, config.poll_interval),
            daemon=True,
            name="oct-watcher-poll",
        )
        self._thread = t
        t.start()
        logger.info(
            "OCTWatcherModule: watch=%s queue=%s poll=%ss stability=%ss",
            watch_path,
            worker_config.queue_name,
            config.poll_interval,
            config.stability_seconds,
        )

    def _teardown(self) -> None:
        self._poll_stop.set()
        th, self._thread = self._thread, None
        if th is not None and th.is_alive():
            th.join(timeout=30.0)

    def _is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
