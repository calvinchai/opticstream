"""gRPC method handlers (ExecuteCommand, telemetry, role tasks, work queue)."""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .generated import opticnode_pb2 as pb2
from .generated.opticnode_pb2 import ExecuteCommandResponse, PingResponse
from .generated.opticnode_pb2_grpc import OpticNodeServicer as OpticNodeBaseServicer
from .telemetry import TelemetryEngine, TelemetrySnapshot
from .work_queue import WorkQueue

logger = logging.getLogger(__name__)


class _WatchLogHandler(FileSystemEventHandler):
    def on_any_event(self, event: Any) -> None:
        logger.info("watcher: %s %s", event.event_type, getattr(event, "src_path", ""))


class OpticNodeServicer(OpticNodeBaseServicer):
    """Application logic for opticnode RPCs."""

    def __init__(
        self,
        settings: Any,
        telemetry: TelemetryEngine,
        work_queue: WorkQueue,
    ) -> None:
        self._settings = settings
        self._telemetry = telemetry
        self._work_queue = work_queue
        self._observer: Observer | None = None
        self._prefect_proc: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()

    def _snapshot_to_pb(self, snap: TelemetrySnapshot) -> pb2.TelemetryResponse:
        net_msgs = [
            pb2.NetIfaceThroughput(name=n.name, bytes_sent=n.bytes_sent, bytes_recv=n.bytes_recv)
            for n in snap.net
        ]
        pc = pb2.PrimoCacheTelemetry(
            is_active=snap.is_primocache_active,
            binary_present=snap.primocache_binary_present,
        )
        if snap.primocache is not None:
            s = snap.primocache
            pc.l1_hit_rate_current = float(s.l1_hit_rate_current or 0.0)
            pc.l1_hit_rate_cumulative = float(s.l1_hit_rate_cumulative or 0.0)
            pc.l2_hit_rate_current = float(s.l2_hit_rate_current or 0.0)
            pc.l2_hit_rate_cumulative = float(s.l2_hit_rate_cumulative or 0.0)
            pc.cache_used_mb = float(s.cache_used_mb or 0.0)
            pc.cache_free_mb = float(s.cache_free_mb or 0.0)
            pc.write_buffer_deferred_blocks = int(s.write_buffer_deferred_blocks or 0)
            pc.write_buffer_urgent_writes = int(s.write_buffer_urgent_writes or 0)
            pc.io_trimmed_blocks = int(s.io_trimmed_blocks or 0)
            pc.io_read_bytes = int(s.io_read_bytes or 0)
            pc.io_written_bytes = int(s.io_written_bytes or 0)
        return pb2.TelemetryResponse(
            collected_at_unix=snap.collected_at_unix,
            cpu_pct=snap.cpu_pct,
            ram_used_pct=snap.ram_used_pct,
            net=net_msgs,
            primocache=pc,
            primocache_error=snap.primocache_raw_error or "",
        )

    def GetTelemetry(self, request: Any, context: Any) -> pb2.TelemetryResponse:
        snap = self._telemetry.collect()
        return self._snapshot_to_pb(snap)

    def StartWatcher(self, request: Any, context: Any) -> pb2.RoleTaskResponse:
        import grpc  # type: ignore[reportMissingModuleSource]

        path = (request.watch_path or "").strip()
        if not path:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return pb2.RoleTaskResponse(ok=False, message="watch_path is required")
        p = Path(path)
        if not p.is_dir():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return pb2.RoleTaskResponse(ok=False, message=f"not a directory: {path}")

        with self._lock:
            if self._observer is not None:
                return pb2.RoleTaskResponse(ok=False, message="watcher already running")
            obs = Observer()
            obs.schedule(_WatchLogHandler(), str(p.resolve()), recursive=bool(request.recursive))
            obs.start()
            self._observer = obs
        logger.info("Directory watcher started on %s", path)
        return pb2.RoleTaskResponse(ok=True, message="watcher started")

    def StopWatcher(self, request: Any, context: Any) -> pb2.RoleTaskResponse:
        with self._lock:
            obs = self._observer
            self._observer = None
        if obs is None:
            return pb2.RoleTaskResponse(ok=False, message="watcher not running")
        obs.stop()
        obs.join(timeout=10.0)
        logger.info("Directory watcher stopped")
        return pb2.RoleTaskResponse(ok=True, message="watcher stopped")

    def StartPrefectWorker(self, request: Any, context: Any) -> pb2.RoleTaskResponse:
        import grpc  # type: ignore[reportMissingModuleSource]

        pool = (request.work_pool or "default").strip() or "default"
        extra = list(request.extra_args)

        with self._lock:
            if self._prefect_proc is not None and self._prefect_proc.poll() is None:
                return pb2.RoleTaskResponse(ok=False, message="prefect worker already running")
            cmd = ["prefect", "worker", "start", "--pool", pool, *extra]
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            except FileNotFoundError:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return pb2.RoleTaskResponse(ok=False, message="prefect CLI not found on PATH")
            self._prefect_proc = proc
        logger.info("Started Prefect worker: %s", shlex.join(cmd))
        return pb2.RoleTaskResponse(ok=True, message="prefect worker started")

    def StopPrefectWorker(self, request: Any, context: Any) -> pb2.RoleTaskResponse:
        with self._lock:
            proc = self._prefect_proc
            self._prefect_proc = None
        if proc is None or proc.poll() is not None:
            return pb2.RoleTaskResponse(ok=False, message="prefect worker not running")
        proc.terminate()
        try:
            proc.wait(timeout=15.0)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info("Prefect worker stopped")
        return pb2.RoleTaskResponse(ok=True, message="prefect worker stopped")

    def QueueCopyJob(self, request: Any, context: Any) -> pb2.CopyJobResponse:
        import grpc  # type: ignore[reportMissingModuleSource]

        src = (request.src_path or "").strip()
        dst = (request.dst_path or "").strip()
        if not src or not dst:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return pb2.CopyJobResponse(
                job_id="",
                status=pb2.COPY_JOB_STATUS_FAILED,
                error="src_path and dst_path are required",
            )
        job_id = self._work_queue.enqueue(src, dst, move_mode=bool(request.move_mode))
        return pb2.CopyJobResponse(job_id=job_id, status=pb2.COPY_JOB_STATUS_QUEUED, error="")

    def PauseCopyQueue(self, request: Any, context: Any) -> pb2.StatusResponse:
        self._work_queue.pause()
        return pb2.StatusResponse(ok=True, message="queue paused")

    def ResumeCopyQueue(self, request: Any, context: Any) -> pb2.StatusResponse:
        self._work_queue.resume()
        return pb2.StatusResponse(ok=True, message="queue resumed")

    def ExecuteCommand(self, request: Any, context: Any) -> ExecuteCommandResponse:
        """Execute a controller-issued command and return typed protobuf response."""
        import grpc  # type: ignore[reportMissingModuleSource]

        command = request.command or ""
        args = list(request.args)
        timeout_s = request.timeout_s or 30.0
        shell_mode = request.shell

        if not command.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Field `command` must be a non-empty string.")
            return ExecuteCommandResponse(ok=False, error="invalid command")
        if timeout_s <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Field `timeout_s` must be > 0.")
            return ExecuteCommandResponse(ok=False, error="invalid timeout_s")

        if shell_mode:
            cmd: str | list[str] = command
        else:
            cmd = [command, *args]

        logger.info("ExecuteCommand received: %s", cmd if shell_mode else shlex.join(cmd))
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_s,
                shell=shell_mode,
                check=False,
            )
            return ExecuteCommandResponse(
                ok=True,
                command=command,
                args=args,
                shell=shell_mode,
                timeout_s=timeout_s,
                exit_code=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
                timed_out=False,
                error="",
            )
        except subprocess.TimeoutExpired as exc:
            return ExecuteCommandResponse(
                ok=False,
                command=command,
                args=args,
                shell=shell_mode,
                timeout_s=timeout_s,
                exit_code=0,
                stdout=exc.stdout or "",
                stderr=exc.stderr or "",
                timed_out=True,
                error=f"Command timed out after {timeout_s} seconds.",
            )
        except Exception as exc:  # pragma: no cover - defensive fallback
            return ExecuteCommandResponse(
                ok=False,
                command=command,
                args=args,
                shell=shell_mode,
                timeout_s=timeout_s,
                exit_code=0,
                stdout="",
                stderr="",
                timed_out=False,
                error=str(exc),
            )

    def Ping(self, request: Any, context: Any) -> PingResponse:
        """Lightweight RTT probe for the hub dashboard."""
        now = time.time_ns()
        return PingResponse(server_recv_unix_ns=now, server_send_unix_ns=now)
