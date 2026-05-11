"""OCTProcessServerModule: runs ``opticstream oct serve all`` as a supervised subprocess."""

from __future__ import annotations

import logging
import shlex
import shutil
import subprocess
import threading

from pydantic import Field

from opticnode.modules.base import ModuleConfig, NodeModule

logger = logging.getLogger(__name__)


def _find_cli() -> str:
    for name in ("opticstream", "ops"):
        path = shutil.which(name)
        if path:
            return path
    raise RuntimeError("opticstream/ops CLI not found on PATH")


class OCTProcessServerConfig(ModuleConfig):
    project_name: str = Field(default="")
    deployment_name: str = Field(default="local")
    concurrency_limit: int = Field(default=1, ge=1)
    exclude: list[str] = Field(default_factory=list)
    extra_args: list[str] = Field(default_factory=list)


class OCTProcessServerModule(NodeModule):
    """Runs ``opticstream oct serve all`` as a subprocess with supervision."""

    name = "oct_process_server"
    Config = OCTProcessServerConfig

    def __init__(self) -> None:
        super().__init__()
        self._proc: subprocess.Popen[str] | None = None
        self._proc_lock = threading.Lock()

    def _launch(self, config: OCTProcessServerConfig) -> None:
        pn = config.project_name.strip()
        if not pn:
            raise ValueError("OCTProcessServerModule requires a non-empty 'project_name'.")

        cli = _find_cli()
        cmd = [
            cli, "oct", "serve", "all",
            "--project-name", pn,
            "--deployment-name", config.deployment_name.strip() or "local",
            "--concurrency-limit", str(config.concurrency_limit),
        ]
        for flow in config.exclude:
            cmd.extend(["--exclude", flow])
        cmd.extend(config.extra_args)

        logger.info("OCTProcessServerModule: starting %s", shlex.join(cmd))
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError:
            raise RuntimeError("opticstream CLI not found on PATH")

        with self._proc_lock:
            self._proc = proc

        threading.Thread(
            target=self._read_logs,
            args=(proc,),
            daemon=True,
            name="oct-process-server-logs",
        ).start()
        logger.info("OCTProcessServerModule: subprocess started (pid=%d).", proc.pid)

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
            logger.info("%s", line.rstrip())
