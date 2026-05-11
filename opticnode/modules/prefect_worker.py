"""PrefectWorkerModule: manages a prefect worker subprocess."""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
import time

from pydantic import Field

from .base import ModuleConfig, ModuleState, ModuleStatus, NodeModule

# Use the module-specific logger so output goes to prefect_worker.log via ModuleLog.
logger = logging.getLogger(__name__)

_RESTART_RESET_AFTER_S = 30.0


class PrefectWorkerConfig(ModuleConfig):
    work_pool: str = Field(default="default")
    worker_count: int = Field(default=1, ge=1)
    extra_args: list[str] = Field(default_factory=list)
    auto_restart: bool = True


class PrefectWorkerModule(NodeModule):
    """Runs `prefect worker start` as a subprocess with supervision."""

    name = "prefect_worker"
    Config = PrefectWorkerConfig

    def __init__(self) -> None:
        self._config: PrefectWorkerConfig = PrefectWorkerConfig()
        self._state = ModuleState.STOPPED
        self._started_at: float | None = None
        self._error: str = ""
        self._proc: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()
        self._restart_count = 0

    def _build_cmd(self, config: PrefectWorkerConfig) -> list[str]:
        pool = config.work_pool.strip() or "default"
        return ["prefect", "worker", "start", "--pool", pool, "--limit", str(config.worker_count), *config.extra_args]

    def _do_start(self, config: PrefectWorkerConfig) -> None:
        cmd = self._build_cmd(config)
        logger.info("PrefectWorkerModule: starting %s", shlex.join(cmd))
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError:
            with self._lock:
                self._state = ModuleState.ERROR
                self._error = "prefect CLI not found on PATH"
            logger.error("PrefectWorkerModule: %s", self._error)
            return

        with self._lock:
            self._proc = proc
            self._state = ModuleState.RUNNING
            self._started_at = time.time()
            self._error = ""

        log_thread = threading.Thread(target=self._read_logs, args=(proc,), daemon=True)
        log_thread.start()

        supervisor = threading.Thread(target=self._supervise, args=(proc, config), daemon=True)
        supervisor.start()

    def _read_logs(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            logger.info("%s", line.rstrip())

    def _supervise(self, proc: subprocess.Popen[str], config: PrefectWorkerConfig) -> None:
        start_time = time.time()
        proc.wait()
        elapsed = time.time() - start_time

        with self._lock:
            if self._state == ModuleState.STOPPING:
                self._state = ModuleState.STOPPED
                self._proc = None
                return
            if self._state != ModuleState.RESTARTING:
                self._state = ModuleState.ERROR
                self._error = f"exited unexpectedly (code {proc.returncode})"
            proc_ref = self._proc
            if proc_ref is proc:
                self._proc = None

        logger.warning("PrefectWorkerModule: %s", self._error)

        if elapsed > _RESTART_RESET_AFTER_S:
            self._restart_count = 0

        if not config.auto_restart:
            return

        delay = min(2 ** self._restart_count, 60)
        self._restart_count += 1
        logger.info("PrefectWorkerModule: restarting in %ds (attempt %d).", delay, self._restart_count)

        def _delayed_restart() -> None:
            time.sleep(delay)
            with self._lock:
                if self._state not in (ModuleState.ERROR, ModuleState.RESTARTING):
                    return
                self._state = ModuleState.STARTING
            self._do_start(config)

        threading.Thread(target=_delayed_restart, daemon=True).start()

    def start(self, config: PrefectWorkerConfig) -> None:
        with self._lock:
            if self._state in (ModuleState.RUNNING, ModuleState.STARTING):
                raise RuntimeError("PrefectWorkerModule is already running.")
            self._state = ModuleState.STARTING
            self._config = config
        self._do_start(config)

    def stop(self) -> None:
        with self._lock:
            proc = self._proc
            self._state = ModuleState.STOPPING
        if proc is None or proc.poll() is not None:
            with self._lock:
                self._state = ModuleState.STOPPED
            return
        proc.terminate()
        try:
            proc.wait(timeout=15.0)
        except subprocess.TimeoutExpired:
            proc.kill()
        with self._lock:
            self._proc = None
            self._state = ModuleState.STOPPED
            self._started_at = None
        logger.info("PrefectWorkerModule: stopped.")

    def reconfigure(self, patch: dict) -> None:
        new_config = PrefectWorkerConfig.model_validate({**self._config.model_dump(), **patch})
        with self._lock:
            was_running = self._state == ModuleState.RUNNING
            self._config = new_config
            if was_running:
                self._state = ModuleState.RESTARTING

        if was_running:
            self.stop()
            self.start(new_config)

    def status(self) -> ModuleStatus:
        with self._lock:
            return ModuleStatus(
                name=self.name,
                state=self._state,
                config=self._config.model_dump(),
                started_at=self._started_at,
                error=self._error,
            )
