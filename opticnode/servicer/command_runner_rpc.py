"""CommandRunnerServicer: typed gRPC service for the command_runner module."""

from __future__ import annotations

import logging
from typing import Any

import grpc  # type: ignore[reportMissingModuleSource]

from opticapi.generated import command_runner_pb2 as crpb2
from opticapi.generated.command_runner_pb2_grpc import CommandRunnerServicer as _BaseServicer
from ..modules.base import ModuleRegistry

logger = logging.getLogger(__name__)

_MODULE_NAME = "command_runner"


def _result_to_pb(result: dict[str, Any]) -> crpb2.CommandJobResult:
    return crpb2.CommandJobResult(
        job_id=result.get("job_id", ""),
        status=result.get("status", ""),
        command=result.get("command", ""),
        args=list(result.get("args") or []),
        shell=bool(result.get("shell", False)),
        timeout_s=float(result.get("timeout_s", 0.0)),
        submitted_at_unix=float(result.get("submitted_at_unix", 0.0)),
        started_at_unix=float(result.get("started_at_unix", 0.0)),
        finished_at_unix=float(result.get("finished_at_unix", 0.0)),
        exit_code=int(result.get("exit_code", 0)),
        stdout=result.get("stdout", "") or "",
        stderr=result.get("stderr", "") or "",
        timed_out=bool(result.get("timed_out", False)),
        error=result.get("error", "") or "",
    )


class CommandRunnerServicer(_BaseServicer):
    """gRPC servicer that routes typed CommandRunner RPCs to the module registry."""

    def __init__(self, registry: ModuleRegistry) -> None:
        self._registry = registry

    def SubmitCommand(self, request: Any, context: Any) -> crpb2.SubmitCommandResponse:
        command = (request.command or "").strip()
        if not command:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return crpb2.SubmitCommandResponse(job_id="", ok=False, error="command is required")

        payload: dict[str, Any] = {
            "command": command,
            "args": list(request.args),
            "shell": bool(request.shell),
        }
        if request.timeout_s and request.timeout_s > 0:
            payload["timeout_s"] = float(request.timeout_s)

        try:
            job_id = self._registry.submit_job(_MODULE_NAME, payload)
            return crpb2.SubmitCommandResponse(job_id=job_id, ok=True, error="")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return crpb2.SubmitCommandResponse(job_id="", ok=False, error=str(exc))
        except (ValueError, RuntimeError) as exc:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return crpb2.SubmitCommandResponse(job_id="", ok=False, error=str(exc))
        except Exception as exc:
            logger.exception("SubmitCommand failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return crpb2.SubmitCommandResponse(job_id="", ok=False, error=str(exc))

    def GetCommandResult(self, request: Any, context: Any) -> crpb2.GetCommandResultResponse:
        job_id = (request.job_id or "").strip()
        if not job_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return crpb2.GetCommandResultResponse(ok=False, error="job_id is required")

        try:
            result = self._registry.get_job_result(_MODULE_NAME, job_id)
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return crpb2.GetCommandResultResponse(ok=False, error=str(exc))
        except Exception as exc:
            logger.exception("GetCommandResult failed for '%s'", job_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return crpb2.GetCommandResultResponse(ok=False, error=str(exc))

        if result is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return crpb2.GetCommandResultResponse(ok=False, error=f"job '{job_id}' not found")
        return crpb2.GetCommandResultResponse(ok=True, error="", result=_result_to_pb(result))

    def ListCommandResults(self, request: Any, context: Any) -> crpb2.ListCommandResultsResponse:
        limit = int(request.limit) if request.limit > 0 else None
        try:
            results = self._registry.list_job_results(_MODULE_NAME, limit=limit)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return crpb2.ListCommandResultsResponse(results=[])
        except Exception:
            logger.exception("ListCommandResults failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return crpb2.ListCommandResultsResponse(results=[])
        return crpb2.ListCommandResultsResponse(results=[_result_to_pb(r) for r in results])
