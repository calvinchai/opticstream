"""CoreMixin: Ping, GetTelemetry, ExecuteCommand RPC handlers."""

from __future__ import annotations

import logging
import shlex
import subprocess
import time
from typing import Any

import grpc  # type: ignore[reportMissingModuleSource]

from opticapi.generated import telemetry_pb2 as tpb2
from opticnode.app.telemetry import TelemetryEngine, TelemetrySnapshot

logger = logging.getLogger(__name__)


def _snapshot_to_pb(snap: TelemetrySnapshot) -> tpb2.TelemetryResponse:
    net_msgs = [
        tpb2.NetIfaceThroughput(name=n.name, bytes_sent=n.bytes_sent, bytes_recv=n.bytes_recv)
        for n in snap.net
    ]
    return tpb2.TelemetryResponse(
        collected_at_unix=snap.collected_at_unix,
        cpu_pct=snap.cpu_pct,
        ram_used_pct=snap.ram_used_pct,
        net=net_msgs,
    )


class CoreMixin:
    """Handles core node RPCs: Ping, GetTelemetry, ExecuteCommand."""

    def __init__(self, *, telemetry: TelemetryEngine, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._telemetry = telemetry

    def Ping(self, request: Any, context: Any) -> tpb2.PingResponse:
        now = time.time_ns()
        return tpb2.PingResponse(server_recv_unix_ns=now, server_send_unix_ns=now)

    def GetTelemetry(self, request: Any, context: Any) -> tpb2.TelemetryResponse:
        return _snapshot_to_pb(self._telemetry.collect())

    def ExecuteCommand(self, request: Any, context: Any) -> tpb2.ExecuteCommandResponse:
        command = request.command or ""
        args = list(request.args)
        timeout_s = request.timeout_s or 30.0
        shell_mode = request.shell

        if not command.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Field `command` must be a non-empty string.")
            return tpb2.ExecuteCommandResponse(ok=False, error="invalid command")
        if timeout_s <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Field `timeout_s` must be > 0.")
            return tpb2.ExecuteCommandResponse(ok=False, error="invalid timeout_s")

        cmd: str | list[str] = command if shell_mode else [command, *args]
        logger.info("ExecuteCommand: %s", cmd if shell_mode else shlex.join(cmd))
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_s,
                shell=shell_mode,
                check=False,
            )
            return tpb2.ExecuteCommandResponse(
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
            return tpb2.ExecuteCommandResponse(
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
        except Exception as exc:
            return tpb2.ExecuteCommandResponse(
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
