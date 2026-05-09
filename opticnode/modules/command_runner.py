"""CommandRunnerModule: runs command jobs and stores recent results in memory."""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any

from pydantic import Field, model_validator

from .base import ModuleConfig, ModuleState, ModuleStatus, NodeModule

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


class CommandRunnerModule(NodeModule):
    """Runs submitted shell or argv commands asynchronously."""

    name = "command_runner"
    Config = CommandRunnerConfig

    def __init__(self) -> None:
        self._config: CommandRunnerConfig = CommandRunnerConfig()
        self._state = ModuleState.STOPPED
        self._started_at: float | None = None
        self._error = ""
        self._results: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._procs: dict[str, subprocess.Popen[str]] = {}
        self._lock = threading.Lock()

    def start(self, config: CommandRunnerConfig) -> None:
        with self._lock:
            if self._state == ModuleState.RUNNING:
                raise RuntimeError("CommandRunnerModule is already running.")
            self._config = config
            self._state = ModuleState.RUNNING
            self._started_at = time.time()
            self._error = ""
            self._trim_locked()
        logger.info("CommandRunnerModule: started with config %s.", config)

    def stop(self) -> None:
        with self._lock:
            self._state = ModuleState.STOPPING
            procs = dict(self._procs)
        for job_id, proc in procs.items():
            if proc.poll() is None:
                logger.info("CommandRunnerModule: terminating running job %s.", job_id)
                proc.terminate()
        with self._lock:
            self._state = ModuleState.STOPPED
            self._started_at = None
        logger.info("CommandRunnerModule: stopped.")

    def reconfigure(self, patch: dict) -> None:
        new_config = CommandRunnerConfig.model_validate({**self._config.model_dump(), **patch})
        with self._lock:
            self._config = new_config
            self._trim_locked()

    def submit_job(self, payload: dict) -> str:
        command = (payload.get("command") or "").strip()
        if not command:
            raise ValueError("payload must include non-empty 'command'")

        raw_args = payload.get("args") or []
        if not isinstance(raw_args, list) or not all(isinstance(arg, str) for arg in raw_args):
            raise ValueError("payload 'args' must be a list of strings")
        args = list(raw_args)

        shell_mode = bool(payload.get("shell", False))
        with self._lock:
            if self._state != ModuleState.RUNNING:
                raise RuntimeError("CommandRunnerModule is not running.")
            config = self._config

        timeout_s = float(payload.get("timeout_s") or config.default_timeout_s)
        if timeout_s <= 0:
            raise ValueError("payload 'timeout_s' must be > 0")
        if timeout_s > config.max_timeout_s:
            raise ValueError(f"payload 'timeout_s' exceeds max_timeout_s ({config.max_timeout_s})")

        job_id = uuid.uuid4().hex
        now = time.time()
        result = {
            "job_id": job_id,
            "status": "queued",
            "command": command,
            "args": args,
            "shell": shell_mode,
            "timeout_s": timeout_s,
            "submitted_at_unix": now,
            "started_at_unix": 0.0,
            "finished_at_unix": 0.0,
            "exit_code": 0,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "error": "",
        }
        with self._lock:
            self._results[job_id] = result
            self._trim_locked()

        thread = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
        thread.start()
        return job_id

    def get_job_result(self, job_id: str) -> dict | None:
        with self._lock:
            result = self._results.get(job_id)
            return dict(result) if result is not None else None

    def list_job_results(self, limit: int | None = None) -> list[dict]:
        with self._lock:
            results = [dict(r) for r in reversed(self._results.values())]
        if limit is not None and limit > 0:
            return results[:limit]
        return results

    def status(self) -> ModuleStatus:
        with self._lock:
            return ModuleStatus(
                name=self.name,
                state=self._state,
                config=self._config.model_dump(),
                started_at=self._started_at,
                error=self._error,
            )

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            result = self._results.get(job_id)
            if result is None:
                return
            command = result["command"]
            args = list(result["args"])
            shell_mode = bool(result["shell"])
            timeout_s = float(result["timeout_s"])
            max_output_chars = self._config.max_output_chars
            result["status"] = "running"
            result["started_at_unix"] = time.time()

        cmd: str | list[str] = command if shell_mode else [command, *args]
        logger.info("CommandRunnerModule: running job %s: %s", job_id, cmd if shell_mode else shlex.join(cmd))
        proc: subprocess.Popen[str] | None = None
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=shell_mode,
            )
            with self._lock:
                self._procs[job_id] = proc
            stdout, stderr = proc.communicate(timeout=timeout_s)
            self._finish_job(
                job_id,
                status="finished",
                exit_code=proc.returncode,
                stdout=stdout,
                stderr=stderr,
                max_output_chars=max_output_chars,
            )
        except subprocess.TimeoutExpired as exc:
            if proc is not None:
                proc.kill()
                stdout, stderr = proc.communicate()
            else:
                stdout = exc.stdout or ""
                stderr = exc.stderr or ""
            self._finish_job(
                job_id,
                status="timed_out",
                exit_code=0,
                stdout=stdout,
                stderr=stderr,
                timed_out=True,
                error=f"Command timed out after {timeout_s} seconds.",
                max_output_chars=max_output_chars,
            )
        except Exception as exc:
            self._finish_job(
                job_id,
                status="failed",
                exit_code=0,
                error=str(exc),
                max_output_chars=max_output_chars,
            )
        finally:
            with self._lock:
                self._procs.pop(job_id, None)

    def _finish_job(
        self,
        job_id: str,
        *,
        status: str,
        exit_code: int,
        stdout: str = "",
        stderr: str = "",
        timed_out: bool = False,
        error: str = "",
        max_output_chars: int,
    ) -> None:
        with self._lock:
            result = self._results.get(job_id)
            if result is None:
                return
            result.update(
                {
                    "status": status,
                    "finished_at_unix": time.time(),
                    "exit_code": int(exit_code),
                    "stdout": self._truncate(stdout, max_output_chars),
                    "stderr": self._truncate(stderr, max_output_chars),
                    "timed_out": timed_out,
                    "error": error,
                }
            )

    def _trim_locked(self) -> None:
        while len(self._results) > self._config.max_results:
            self._results.popitem(last=False)

    @staticmethod
    def _truncate(value: str | bytes | None, max_chars: int) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            value = value.decode(errors="replace")
        if len(value) <= max_chars:
            return value
        omitted = len(value) - max_chars
        return f"{value[:max_chars]}\n... truncated {omitted} chars ..."
