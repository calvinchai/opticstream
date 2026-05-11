"""PrefectWorkerModule: manages a prefect worker subprocess."""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
from typing import Any

from pydantic import Field

from .base import ModuleConfig, NodeModule

logger = logging.getLogger(__name__)


class PrefectWorkerConfig(ModuleConfig):
    work_pool: str = Field(default="default")
    worker_count: int = Field(default=1, ge=1)
    extra_args: list[str] = Field(default_factory=list)


class PrefectWorkerModule(NodeModule):
    """Runs `prefect worker start` as a subprocess with supervision."""

    name = "prefect_worker"
    Config = PrefectWorkerConfig

    def __init__(self, log_buffer: Any) -> None:
        super().__init__()
        self._log_buffer = log_buffer
        self._proc: subprocess.Popen[str] | None = None
        self._proc_lock = threading.Lock()

    def _launch(self, config: PrefectWorkerConfig) -> None:
        pool = config.work_pool.strip() or "default"
        cmd = ["prefect", "worker", "start", "--pool", pool, "--limit", str(config.worker_count), *config.extra_args]
        logger.info("PrefectWorkerModule: starting %s", shlex.join(cmd))
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError:
            raise RuntimeError("prefect CLI not found on PATH")

        with self._proc_lock:
            self._proc = proc

        threading.Thread(
            target=self._read_logs,
            args=(proc,),
            daemon=True,
            name="prefect-logs",
        ).start()
        logger.info("PrefectWorkerModule: subprocess started (pid=%d).", proc.pid)

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

    def _read_logs(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            self._log_buffer.append("prefect_worker", line.rstrip())
