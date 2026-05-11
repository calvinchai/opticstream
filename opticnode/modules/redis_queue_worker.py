"""RedisQueueWorkerModule: runs RQ worker subprocess(es) for a named queue."""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
from pydantic import Field

from opticnode.modules.base import ModuleConfig, ModuleRegistry, NodeModule

logger = logging.getLogger(__name__)


class RedisQueueWorkerConfig(ModuleConfig):
    queue_name: str = Field(default="")
    num_workers: int = Field(default=1, ge=1)


class RedisQueueWorkerModule(NodeModule):
    """Runs `rq worker` against Settings.redis_url for a single queue."""

    name = "redis_queue_worker"
    Config = RedisQueueWorkerConfig

    def __init__(self, redis_url: str) -> None:
        super().__init__()
        self._redis_url = redis_url
        self._procs: list[subprocess.Popen[str]] = []
        self._proc_lock = threading.Lock()

    def _rq_command(self, config: RedisQueueWorkerConfig) -> list[str]:
        qn = config.queue_name.strip()
        if not qn:
            raise ValueError("RedisQueueWorkerModule requires a non-empty 'queue_name'.")
        return ["rq", "worker", "-u", self._redis_url, qn]

    def _worker_count(self, config: RedisQueueWorkerConfig) -> int:
        return config.num_workers

    def _on_workers_started(self) -> None:
        pass

    def _launch(self, config: RedisQueueWorkerConfig) -> None:
        cmd = self._rq_command(config)
        n = self._worker_count(config)
        logger.info(
            "%s: starting %d x %s",
            type(self).__name__,
            n,
            shlex.join(cmd),
        )
        for _ in range(n):
            self._start_subprocess(cmd)
        self._on_workers_started()

    def _start_subprocess(self, cmd: list[str]) -> subprocess.Popen[str]:
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError:
            raise RuntimeError("rq CLI not found on PATH")

        with self._proc_lock:
            self._procs.append(proc)

        threading.Thread(
            target=self._read_logs,
            args=(proc,),
            daemon=True,
            name=f"rq-worker-logs-{proc.pid}",
        ).start()
        logger.info("%s: subprocess started (pid=%d).", type(self).__name__, proc.pid)
        return proc

    def _teardown(self) -> None:
        with self._proc_lock:
            procs, self._procs = list(self._procs), []
        for proc in procs:
            if proc.poll() is not None:
                continue
            proc.terminate()
            try:
                proc.wait(timeout=15.0)
            except subprocess.TimeoutExpired:
                proc.kill()

    def _is_alive(self) -> bool:
        with self._proc_lock:
            procs = list(self._procs)
        return len(procs) > 0 and all(p.poll() is None for p in procs)

    def _read_logs(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            logger.info("%s", line.rstrip())


class RedisQueueBurstWorkerModule(RedisQueueWorkerModule):
    name = "redis_queue_burst_worker"

    def __init__(self, redis_url: str, registry: ModuleRegistry) -> None:
        super().__init__(redis_url)
        self._registry = registry

    def _rq_command(self, config: RedisQueueWorkerConfig) -> list[str]:
        cmd = super()._rq_command(config)
        return cmd[:-1] + ["--burst", cmd[-1]]

    def _on_workers_started(self) -> None:
        with self._proc_lock:
            procs = list(self._procs)
        threading.Thread(
            target=self._after_all_burst_exit,
            args=(procs,),
            daemon=True,
            name="rq-burst-exit",
        ).start()

    def stop(self) -> None:
        self._teardown()
        super().stop()

    def _after_all_burst_exit(self, procs: list[subprocess.Popen[str]]) -> None:
        for proc in procs:
            try:
                proc.wait()
            except Exception:
                logger.exception("RedisQueueBurstWorkerModule: waiting for worker process failed.")
        with self._lock:
            if self._stop_event.is_set():
                return
        try:
            self._registry.stop(self.name)
        except Exception:
            logger.exception("RedisQueueBurstWorkerModule: registry.stop after burst failed.")
