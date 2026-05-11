"""gRPC helpers for the hub: ping RTT and remote command execution."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import grpc  # type: ignore[reportMissingModuleSource]


@dataclass(frozen=True)
class ExecResult:
    """Outcome of `ExecuteCommand` (RPC success) or transport failure."""

    ok: bool
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    error: str
    rpc_error: str | None


@dataclass(frozen=True)
class ModuleInfo:
    name: str
    state: str
    config: dict[str, Any]
    started_at_unix: float
    error: str


def ping_node(host: str, port: int, *, timeout_ms: int) -> float | None:
    """Return round-trip time in milliseconds, or None on failure."""
    if not host or port <= 0:
        return None
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub
    from opticnode.generated.telemetry_pb2 import PingRequest

    target = f"{host}:{port}"
    deadline = timeout_ms / 1000.0
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        t0 = time.perf_counter_ns()
        stub.Ping(PingRequest(client_send_unix_ns=time.time_ns()), timeout=deadline)
        t1 = time.perf_counter_ns()
        return (t1 - t0) / 1_000_000.0
    except Exception:
        return None
    finally:
        if channel is not None:
            channel.close()


def execute_command(
    host: str,
    port: int,
    *,
    command: str,
    args: list[str],
    shell: bool,
    timeout_s: float,
    rpc_timeout_s: float | None = None,
) -> ExecResult:
    """Call `ExecuteCommand` on the node; return structured result or RPC-layer error."""
    if not host or port <= 0:
        return ExecResult(
            ok=False,
            exit_code=0,
            stdout="",
            stderr="",
            timed_out=False,
            error="",
            rpc_error="invalid host or port",
        )
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub
    from opticnode.generated.telemetry_pb2 import ExecuteCommandRequest

    deadline_s = float(rpc_timeout_s if rpc_timeout_s is not None else timeout_s) + 5.0
    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        req = ExecuteCommandRequest(
            command=command,
            args=args,
            timeout_s=timeout_s,
            shell=shell,
        )
        resp = stub.ExecuteCommand(req, timeout=deadline_s)
        return ExecResult(
            ok=resp.ok,
            exit_code=int(resp.exit_code),
            stdout=resp.stdout or "",
            stderr=resp.stderr or "",
            timed_out=resp.timed_out,
            error=resp.error or "",
            rpc_error=None,
        )
    except grpc.RpcError as exc:
        return ExecResult(
            ok=False,
            exit_code=0,
            stdout="",
            stderr="",
            timed_out=False,
            error="",
            rpc_error=exc.details() or str(exc),
        )
    except Exception as exc:  # pragma: no cover - defensive
        return ExecResult(
            ok=False,
            exit_code=0,
            stdout="",
            stderr="",
            timed_out=False,
            error="",
            rpc_error=str(exc),
        )
    finally:
        if channel is not None:
            channel.close()


def get_module_logs(
    host: str,
    port: int,
    *,
    module: str,
    tail_lines: int = 100,
    entire_buffer: bool = False,
    timeout_s: float = 60.0,
) -> tuple[list[str], str | None]:
    """Call GetModuleLogs; return (lines, rpc_error)."""
    if not host or port <= 0:
        return [], "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.modules_pb2 import ModuleLogsRequest
    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        req = ModuleLogsRequest(
            name=module,
            tail_lines=tail_lines,
            entire_buffer=entire_buffer,
        )
        resp = stub.GetModuleLogs(req, timeout=timeout_s)
        return list(resp.lines), None
    except grpc.RpcError as exc:
        return [], exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return [], str(exc)
    finally:
        if channel is not None:
            channel.close()


def list_modules(
    host: str,
    port: int,
    *,
    timeout_s: float = 30.0,
) -> tuple[list[ModuleInfo], str | None]:
    """Call ListModules; return (modules, rpc_error)."""
    if not host or port <= 0:
        return [], "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.common_pb2 import EmptyRequest
    from opticnode.generated.modules_pb2 import ModuleState
    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub

    state_names = {value: name.replace("MODULE_STATE_", "").lower() for name, value in ModuleState.items()}
    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        resp = stub.ListModules(EmptyRequest(), timeout=timeout_s)
        modules = []
        for module in resp.modules:
            try:
                config = json.loads(module.config_json) if module.config_json else {}
            except json.JSONDecodeError:
                config = {}
            modules.append(
                ModuleInfo(
                    name=module.name,
                    state=state_names.get(module.state, "unknown"),
                    config=config,
                    started_at_unix=float(module.started_at_unix),
                    error=module.error or "",
                )
            )
        return modules, None
    except grpc.RpcError as exc:
        return [], exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return [], str(exc)
    finally:
        if channel is not None:
            channel.close()


def start_module(
    host: str,
    port: int,
    *,
    module: str,
    config: dict[str, Any] | None = None,
    timeout_s: float = 30.0,
) -> tuple[bool, str]:
    """Call StartModule; return (ok, message_or_error)."""
    if not host or port <= 0:
        return False, "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.modules_pb2 import StartModuleRequest
    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        resp = stub.StartModule(
            StartModuleRequest(name=module, config_json=json.dumps(config or {})),
            timeout=timeout_s,
        )
        return bool(resp.ok), resp.message or ""
    except grpc.RpcError as exc:
        return False, exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return False, str(exc)
    finally:
        if channel is not None:
            channel.close()


def stop_module(
    host: str,
    port: int,
    *,
    module: str,
    timeout_s: float = 30.0,
) -> tuple[bool, str]:
    """Call StopModule; return (ok, message_or_error)."""
    if not host or port <= 0:
        return False, "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.modules_pb2 import StopModuleRequest
    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        resp = stub.StopModule(StopModuleRequest(name=module), timeout=timeout_s)
        return bool(resp.ok), resp.message or ""
    except grpc.RpcError as exc:
        return False, exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return False, str(exc)
    finally:
        if channel is not None:
            channel.close()


def configure_module(
    host: str,
    port: int,
    *,
    module: str,
    config: dict[str, Any] | None = None,
    timeout_s: float = 30.0,
) -> tuple[bool, str]:
    """Call ConfigureModule (merge patch into running config); return (ok, message_or_error)."""
    if not host or port <= 0:
        return False, "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.modules_pb2 import ConfigureModuleRequest
    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        resp = stub.ConfigureModule(
            ConfigureModuleRequest(name=module, config_json=json.dumps(config or {})),
            timeout=timeout_s,
        )
        return bool(resp.ok), resp.message or ""
    except grpc.RpcError as exc:
        return False, exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return False, str(exc)
    finally:
        if channel is not None:
            channel.close()


def submit_module_job(
    host: str,
    port: int,
    *,
    module: str,
    payload: dict[str, Any],
    timeout_s: float = 30.0,
) -> tuple[str, str | None]:
    """Call SubmitModuleJob; return (job_id, error)."""
    if not host or port <= 0:
        return "", "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.modules_pb2 import SubmitModuleJobRequest
    from opticnode.generated.opticnode_pb2_grpc import OpticNodeStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = OpticNodeStub(channel)
        resp = stub.SubmitModuleJob(
            SubmitModuleJobRequest(module_name=module, payload_json=json.dumps(payload)),
            timeout=timeout_s,
        )
        return resp.job_id or "", None if resp.ok else (resp.error or "job submission failed")
    except grpc.RpcError as exc:
        return "", exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return "", str(exc)
    finally:
        if channel is not None:
            channel.close()



def _command_result_to_dict(result: Any) -> dict[str, Any]:
    return {
        "job_id": result.job_id,
        "status": result.status,
        "command": result.command,
        "args": list(result.args),
        "shell": result.shell,
        "timeout_s": result.timeout_s,
        "submitted_at_unix": result.submitted_at_unix,
        "started_at_unix": result.started_at_unix,
        "finished_at_unix": result.finished_at_unix,
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "timed_out": result.timed_out,
        "error": result.error,
    }


def submit_command(
    host: str,
    port: int,
    *,
    command: str,
    args: list[str] | None = None,
    shell: bool = False,
    timeout_s: float = 0.0,
    rpc_timeout_s: float = 30.0,
) -> tuple[str, str | None]:
    """Call CommandRunner.SubmitCommand; return (job_id, error)."""
    if not host or port <= 0:
        return "", "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.command_runner_pb2 import SubmitCommandRequest
    from opticnode.generated.command_runner_pb2_grpc import CommandRunnerStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = CommandRunnerStub(channel)
        resp = stub.SubmitCommand(
            SubmitCommandRequest(
                command=command,
                args=list(args or []),
                shell=bool(shell),
                timeout_s=float(timeout_s),
            ),
            timeout=rpc_timeout_s,
        )
        return resp.job_id or "", None if resp.ok else (resp.error or "submit failed")
    except grpc.RpcError as exc:
        return "", exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return "", str(exc)
    finally:
        if channel is not None:
            channel.close()


def get_command_result(
    host: str,
    port: int,
    *,
    job_id: str,
    timeout_s: float = 30.0,
) -> tuple[dict[str, Any] | None, str | None]:
    """Call CommandRunner.GetCommandResult; return (result, error)."""
    if not host or port <= 0:
        return None, "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.command_runner_pb2 import GetCommandResultRequest
    from opticnode.generated.command_runner_pb2_grpc import CommandRunnerStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = CommandRunnerStub(channel)
        resp = stub.GetCommandResult(GetCommandResultRequest(job_id=job_id), timeout=timeout_s)
        if not resp.ok:
            return None, resp.error or "result lookup failed"
        return _command_result_to_dict(resp.result), None
    except grpc.RpcError as exc:
        return None, exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return None, str(exc)
    finally:
        if channel is not None:
            channel.close()


def list_command_results(
    host: str,
    port: int,
    *,
    limit: int = 20,
    timeout_s: float = 30.0,
) -> tuple[list[dict[str, Any]], str | None]:
    """Call CommandRunner.ListCommandResults; return (results, error)."""
    if not host or port <= 0:
        return [], "invalid host or port"
    import grpc  # type: ignore[reportMissingModuleSource]

    from opticnode.generated.command_runner_pb2 import ListCommandResultsRequest
    from opticnode.generated.command_runner_pb2_grpc import CommandRunnerStub

    target = f"{host}:{port}"
    channel: grpc.Channel | None = None
    try:
        channel = grpc.insecure_channel(target)
        stub = CommandRunnerStub(channel)
        resp = stub.ListCommandResults(
            ListCommandResultsRequest(limit=int(limit)),
            timeout=timeout_s,
        )
        return [_command_result_to_dict(r) for r in resp.results], None
    except grpc.RpcError as exc:
        return [], exc.details() or str(exc)
    except Exception as exc:  # pragma: no cover
        return [], str(exc)
    finally:
        if channel is not None:
            channel.close()
