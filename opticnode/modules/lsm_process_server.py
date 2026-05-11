"""LSMProcessServerModule: runs ``opticstream lsm serve process`` as a supervised subprocess."""

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


class LSMProcessServerConfig(ModuleConfig):
    concurrent_workers: int = Field(default=2, ge=1)
    extra_args: list[str] = Field(default_factory=list)


class LSMProcessServerModule(NodeModule):
    """Runs ``opticstream lsm serve process`` as a subprocess with supervision."""

    name = "lsm_process_server"
    Config = LSMProcessServerConfig

    def __init__(self) -> None:
        super().__init__()
        self._proc: subprocess.Popen[str] | None = None
        self._proc_lock = threading.Lock()

    def _launch(self, config: LSMProcessServerConfig) -> None:
        cli = _find_cli()
        cmd = [
            cli, "lsm", "serve", "process",
            "--concurrent-workers", str(config.concurrent_workers),
            *config.extra_args,
        ]
        logger.info("LSMProcessServerModule: starting %s", shlex.join(cmd))
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
            name="lsm-process-server-logs",
        ).start()
        logger.info("LSMProcessServerModule: subprocess started (pid=%d).", proc.pid)

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
