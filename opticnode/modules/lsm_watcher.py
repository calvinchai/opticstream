"""LSM polling watcher: stable strip folders enqueue RQ jobs (not Prefect events)."""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import Field
from redis import Redis
from rq import Queue

from opticstream.cli.lsm.watch import LSMWatcherService
from opticstream.config.lsm_scan_config import LSMScanConfig, get_lsm_scan_config
from opticstream.state import LSM_STATE_SERVICE
from opticapi.project_state.lsm_models import LSMStripId
from opticstream.utils.polling_watcher import PollingStableWatcher

from opticnode.modules.base import ModuleConfig, NodeModule
from opticnode.modules.worker import StripTask, WorkerConfig, _PROCESS_FN

logger = logging.getLogger(__name__)


class RedisLSMWatcherService(LSMWatcherService):
    def __init__(self, *, rq_queue: Queue, worker_config: WorkerConfig, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._rq_queue = rq_queue
        self._worker_config = worker_config

    def _process_event(self, *, strip_ident: LSMStripId, folder: Path) -> int:
        task = StripTask(
            lsm_strip_id=strip_ident,
            strip_path=str(folder),
            force_rerun=self.force_resend,
            timestamp=datetime.fromtimestamp(folder.stat().st_mtime),
        )
        self._rq_queue.enqueue(
            _PROCESS_FN, task.model_dump(mode="python"), self._worker_config.model_dump()
        )
        with LSM_STATE_SERVICE.open_strip(strip_ident=strip_ident):
            pass
        logger.info("Enqueued LSM strip job for %s from %s", strip_ident, folder)
        return 1


class LSMWatcherConfig(ModuleConfig):
    watch_path: str = Field(default="")
    poll_interval: int = Field(default=5, ge=1)
    stability_seconds: int = Field(default=15, ge=0)
    force_resend: bool = False
    project_name: str = Field(default="")
    slice_offset: int = Field(default=0)
    prefect_deployment: str = Field(default="")
    allowed_window_minutes: float = Field(default=10, ge=0)


class LSMWatcherModule(NodeModule):
    """Polls for stable LSM folders and enqueues RQ jobs."""

    name = "lsm_watcher"
    Config = LSMWatcherConfig

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
            logger.info("--- lsm_watcher iteration %s ---", iteration)
            try:
                watcher._run_iteration()
            except Exception:
                logger.exception("LSM watcher iteration failed")
            if self._poll_stop.wait(timeout=float(poll_interval)):
                break
        logger.info("LSM watcher poll loop exited after %s iterations", iteration)

    def _launch(self, config: LSMWatcherConfig) -> None:
        watch_path = config.watch_path.strip()
        if not watch_path:
            raise ValueError("LSMWatcherModule requires a non-empty 'watch_path'.")
        pn = config.project_name.strip()
        if not pn:
            raise ValueError("LSMWatcherModule requires a non-empty 'project_name'.")

        p = Path(watch_path)
        if not p.is_dir():
            raise ValueError(f"LSMWatcherModule: watch_path is not a directory: {watch_path}")

        worker_config = WorkerConfig(
            project_name=pn,
            deployment_name=config.prefect_deployment,
            queue_kind="lsm",
            allowed_window_minutes=config.allowed_window_minutes,
            redis_url=self._redis_url,
        )
        rq_queue = self._make_rq_queue(worker_config.queue_name)
        self._poll_stop.clear()
        scan_config: LSMScanConfig = get_lsm_scan_config(pn)
        service = RedisLSMWatcherService(
            rq_queue=rq_queue,
            worker_config=worker_config,
            project_name=pn,
            scan_config=scan_config,
            watch_dir=p,
            slice_offset=config.slice_offset,
            direct=False,
            force_resend=config.force_resend,
        )
        watcher: PollingStableWatcher[Any, Any] = PollingStableWatcher(
            discover_candidates=service.discover_candidates,
            candidate_key=service.candidate_key,
            fingerprint=service.fingerprint,
            process=service.process,
            poll_interval=config.poll_interval,
            stability_seconds=config.stability_seconds,
            running_message="LSM Redis watcher (polling)",
        )

        t = threading.Thread(
            target=self._poll_loop,
            args=(watcher, config.poll_interval),
            daemon=True,
            name="lsm-watcher-poll",
        )
        self._thread = t
        t.start()
        logger.info(
            "LSMWatcherModule: watch=%s queue=%s poll=%ss stability=%ss",
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
