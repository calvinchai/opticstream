"""CommandRunnerModule: runs queued commands sequentially, recording results."""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
import time
import uuid
from collections import OrderedDict, deque
from typing import Any

from pydantic import Field, model_validator

from opticnode.modules.base import ModuleConfig, ModuleState, LoopModule

logger = logging.getLogger(__name__)


class CommandRunnerConfig(ModuleConfig):
    max_results: int = Field(default=100, gt=0)
    default_timeout_s: float = Field(default=30.0, gt=0)
    max_timeout_s: float = Field(default=600.0, gt=0)
    max_output_chars: int = Field(default=200_000, gt=0)

    @model_validator(mode="after")
    def _timeout_order(self) -> CommandRunnerConfig:
        if self.default_timeout_s > self.max_timeout_s:
            raise ValueError("default_timeout_s must be <= max_timeout_s")
        return self


class CommandRunnerModule(LoopModule):
    """Drains a command queue in a background loop, recording each result."""

    name = "command_runner"
    Config = CommandRunnerConfig
    _thread_join_timeout = 5.0

    def __init__(self) -> None:
        super().__init__()
        self._queue: deque[dict[str, Any]] = deque()
        self._results: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._job_lock = threading.Lock()

    # ---------- public job API ----------

    def submit_job(self, payload: dict) -> str:
        """Append a command to the queue and return its job_id."""
        command = (payload.get("command") or "").strip()
        if not command:
            raise ValueError("payload must include non-empty 'command'")

        with self._lock:
            if self._state != ModuleState.RUNNING:
                raise RuntimeError("CommandRunnerModule is not running.")

        cfg: CommandRunnerConfig = self._config  # type: ignore[assignment]
        timeout_s = float(payload.get("timeout_s") or cfg.default_timeout_s)
        if timeout_s <= 0:
            raise ValueError("payload 'timeout_s' must be > 0")
        if timeout_s > cfg.max_timeout_s:
            raise ValueError(f"payload 'timeout_s' exceeds max_timeout_s ({cfg.max_timeout_s})")

        job_id = uuid.uuid4().hex
        job: dict[str, Any] = {
            "job_id": job_id,
            "command": command,
            "shell": bool(payload.get("shell", True)),
            "timeout_s": timeout_s,
            "submitted_at_unix": time.time(),
            "status": "queued",
            "started_at_unix": 0.0,
            "finished_at_unix": 0.0,
            "exit_code": 0,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "error": "",
        }

        with self._job_lock:
            self._results[job_id] = job
            self._trim_locked()
            self._queue.append(job_id)

        return job_id

    def get_job_result(self, job_id: str) -> dict | None:
        with self._job_lock:
            r = self._results.get(job_id)
            return dict(r) if r is not None else None

    def list_job_results(self, limit: int | None = None) -> list[dict]:
        with self._job_lock:
            results = [dict(r) for r in reversed(self._results.values())]
        if limit is not None and limit > 0:
            return results[:limit]
        return results

    # ---------- loop ----------

    def _run_loop(self) -> None:
        logger.info("CommandRunnerModule: ready (config=%s).", self._config)
        while not self._stop_event.is_set():
            with self._job_lock:
                job_id = self._queue.popleft() if self._queue else None
            if job_id is None:
                time.sleep(0.1)
                continue
            self._run_job(job_id)

    # ---------- internal ----------

    def _run_job(self, job_id: str) -> None:
        cfg: CommandRunnerConfig = self._config  # type: ignore[assignment]

        with self._job_lock:
            r = self._results.get(job_id)
            if r is None:
                return
            command = r["command"]
            shell_mode = r["shell"]
            timeout_s = r["timeout_s"]
            r["status"] = "running"
            r["started_at_unix"] = time.time()

        cmd: str | list[str] = command if shell_mode else shlex.split(command)
        logger.info("CommandRunnerModule: running job %s: %s", job_id, command)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=shell_mode,
            )
            stdout, stderr = proc.communicate(timeout=timeout_s)
            self._record(job_id, status="finished", exit_code=proc.returncode,
                         stdout=stdout, stderr=stderr, cfg=cfg)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            self._record(job_id, status="timed_out", exit_code=0,
                         stdout=stdout or "", stderr=stderr or "",
                         timed_out=True, error=f"Command timed out after {timeout_s}s.", cfg=cfg)
        except Exception as exc:
            self._record(job_id, status="failed", exit_code=0, error=str(exc), cfg=cfg)

    def _record(self, job_id: str, *, status: str, exit_code: int,
                stdout: str = "", stderr: str = "", timed_out: bool = False,
                error: str = "", cfg: CommandRunnerConfig) -> None:
        with self._job_lock:
            r = self._results.get(job_id)
            if r is None:
                return
            r.update({
                "status": status,
                "finished_at_unix": time.time(),
                "exit_code": exit_code,
                "stdout": _truncate(stdout, cfg.max_output_chars),
                "stderr": _truncate(stderr, cfg.max_output_chars),
                "timed_out": timed_out,
                "error": error,
            })

    def _trim_locked(self) -> None:
        cfg: CommandRunnerConfig = self._config  # type: ignore[assignment]
        while len(self._results) > cfg.max_results:
            self._results.popitem(last=False)


def _truncate(value: str | bytes | None, max_chars: int) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    if len(value) <= max_chars:
        return value
    omitted = len(value) - max_chars
    return f"{value[:max_chars]}\n... truncated {omitted} chars ..."
