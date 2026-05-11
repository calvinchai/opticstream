"""RedisQueueWorkerModule: runs an RQ worker subprocess for a named queue."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import threading
from typing import Any

from pydantic import Field

from .base import ModuleConfig, ModuleRegistry, NodeModule
from .worker import (
    ENV_ALLOWED_WINDOW_MINUTES,
    ENV_BACKLOG_QUEUE,
    ENV_PREFECT_DEPLOYMENT,
    ENV_PREFECT_DEPLOYMENT_LSM,
    ENV_PREFECT_DEPLOYMENT_OCT,
    ENV_REDIS_URL,
)

logger = logging.getLogger(__name__)


class RedisQueueWorkerConfig(ModuleConfig):
    queue_name: str = Field(default="")
    prefect_deployment: str = Field(default="")
    prefect_deployment_lsm: str = Field(default="")
    prefect_deployment_oct: str = Field(default="")
    allowed_window_minutes: float = Field(default=10.0)
    backlog_queue_name: str = Field(default="")


class RedisQueueWorkerModule(NodeModule):
    """Runs `rq worker` against Settings.redis_url for a single queue."""

    name = "redis_queue_worker"
    Config = RedisQueueWorkerConfig

    def __init__(self, redis_url: str, log_buffer: Any) -> None:
        super().__init__()
        self._redis_url = redis_url
        self._log_buffer = log_buffer
        self._proc: subprocess.Popen[str] | None = None
        self._proc_lock = threading.Lock()

    def _log_bucket(self) -> str:
        return "redis_queue_worker"

    def _rq_command(self, config: RedisQueueWorkerConfig) -> list[str]:
        qn = config.queue_name.strip()
        if not qn:
            raise ValueError("RedisQueueWorkerModule requires a non-empty 'queue_name'.")
        return ["rq", "worker", "-u", self._redis_url, qn]

    def _rq_worker_env(self, config: RedisQueueWorkerConfig) -> dict[str, str]:
        env: dict[str, str] = {ENV_REDIS_URL: self._redis_url}
        if config.prefect_deployment.strip():
            env[ENV_PREFECT_DEPLOYMENT] = config.prefect_deployment.strip()
        if config.prefect_deployment_lsm.strip():
            env[ENV_PREFECT_DEPLOYMENT_LSM] = config.prefect_deployment_lsm.strip()
        if config.prefect_deployment_oct.strip():
            env[ENV_PREFECT_DEPLOYMENT_OCT] = config.prefect_deployment_oct.strip()
        env[ENV_ALLOWED_WINDOW_MINUTES] = str(config.allowed_window_minutes)
        if config.backlog_queue_name.strip():
            env[ENV_BACKLOG_QUEUE] = config.backlog_queue_name.strip()
        return env

    def _launch(self, config: RedisQueueWorkerConfig) -> None:
        cmd = self._rq_command(config)
        logger.info("RedisQueueWorkerModule: starting %s", shlex.join(cmd))
        self._start_subprocess(cmd, extra_env=self._rq_worker_env(config))

    def _start_subprocess(
        self,
        cmd: list[str],
        *,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
        except FileNotFoundError:
            raise RuntimeError("rq CLI not found on PATH")

        with self._proc_lock:
            self._proc = proc

        bucket = self._log_bucket()
        threading.Thread(
            target=self._read_logs,
            args=(proc, bucket),
            daemon=True,
            name="rq-worker-logs",
        ).start()
        logger.info("%s: subprocess started (pid=%d).", type(self).__name__, proc.pid)
        return proc

    def _teardown(self) -> None:
        with self._proc_lock:
            proc, self._proc = self._proc, None
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=15.0)
        except subprocess.TimeoutExpired:
            proc.kill()

    def _is_alive(self) -> bool:
        with self._proc_lock:
            proc = self._proc
        return proc is not None and proc.poll() is None

    def _read_logs(self, proc: subprocess.Popen[str], bucket: str) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            self._log_buffer.append(bucket, line.rstrip())


class RedisQueueBurstWorkerModule(RedisQueueWorkerModule):
    """Runs `rq worker --burst`: drains the queue then stops (disables persisted config)."""

    name = "redis_queue_burst_worker"

    def __init__(self, redis_url: str, log_buffer: Any, registry: ModuleRegistry) -> None:
        super().__init__(redis_url, log_buffer)
        self._registry = registry

    def _log_bucket(self) -> str:
        return "redis_queue_burst_worker"

    def _rq_command(self, config: RedisQueueWorkerConfig) -> list[str]:
        cmd = super()._rq_command(config)
        return cmd[:-1] + ["--burst", cmd[-1]]

    def _launch(self, config: RedisQueueWorkerConfig) -> None:
        cmd = self._rq_command(config)
        logger.info("RedisQueueBurstWorkerModule: starting %s", shlex.join(cmd))
        proc = self._start_subprocess(cmd, extra_env=self._rq_worker_env(config))
        threading.Thread(
            target=self._after_worker_exit,
            args=(proc,),
            daemon=True,
            name="rq-burst-exit",
        ).start()

    def stop(self) -> None:
        # Base class skips _teardown when state is already ERROR (supervisor race). Always reap
        # the subprocess handle so _proc is cleared after burst exit or manual stop.
        self._teardown()
        super().stop()

    def _after_worker_exit(self, proc: subprocess.Popen[str]) -> None:
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
