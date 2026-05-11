"""ModulesMixin: module lifecycle and job submission RPC handlers."""

from __future__ import annotations

import json
import logging
from typing import Any

import grpc

from opticapi.generated import common_pb2 as cpb2
from opticapi.generated import modules_pb2 as mpb2
from ..modules.base import ModuleRegistry, ModuleState

logger = logging.getLogger(__name__)

_STATE_TO_PB = {
    ModuleState.STOPPED: mpb2.MODULE_STATE_STOPPED,
    ModuleState.STARTING: mpb2.MODULE_STATE_STARTING,
    ModuleState.RUNNING: mpb2.MODULE_STATE_RUNNING,
    ModuleState.STOPPING: mpb2.MODULE_STATE_STOPPING,
    ModuleState.ERROR: mpb2.MODULE_STATE_ERROR,
    ModuleState.RESTARTING: mpb2.MODULE_STATE_RESTARTING,
}


def _status_to_pb(status: Any) -> mpb2.ModuleStatus:
    return mpb2.ModuleStatus(
        name=status.name,
        state=_STATE_TO_PB.get(status.state, mpb2.MODULE_STATE_STOPPED),
        config_json=json.dumps(status.config),
        started_at_unix=status.started_at or 0.0,
        error=status.error or "",
    )


class ModulesMixin:
    """Handles module management and job submission RPCs."""

    def __init__(self, *, registry: ModuleRegistry, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._registry = registry

    def ListModules(self, request: Any, context: Any) -> mpb2.ModuleListResponse:
        statuses = self._registry.list_all()
        return mpb2.ModuleListResponse(modules=[_status_to_pb(s) for s in statuses])

    def StartModule(self, request: Any, context: Any) -> cpb2.RoleTaskResponse:
        name = (request.name or "").strip()
        if not name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cpb2.RoleTaskResponse(ok=False, message="module name is required")
        try:
            config = json.loads(request.config_json) if request.config_json else {}
        except json.JSONDecodeError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cpb2.RoleTaskResponse(ok=False, message=f"invalid config_json: {exc}")
        try:
            self._registry.start(name, config)
            return cpb2.RoleTaskResponse(ok=True, message=f"module '{name}' started")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except Exception as exc:
            logger.exception("StartModule failed for '%s'", name)
            context.set_code(grpc.StatusCode.INTERNAL)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))

    def StopModule(self, request: Any, context: Any) -> cpb2.RoleTaskResponse:
        name = (request.name or "").strip()
        if not name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cpb2.RoleTaskResponse(ok=False, message="module name is required")
        try:
            self._registry.stop(name)
            return cpb2.RoleTaskResponse(ok=True, message=f"module '{name}' stopped")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except Exception as exc:
            logger.exception("StopModule failed for '%s'", name)
            context.set_code(grpc.StatusCode.INTERNAL)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))

    def ConfigureModule(self, request: Any, context: Any) -> cpb2.RoleTaskResponse:
        name = (request.name or "").strip()
        if not name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cpb2.RoleTaskResponse(ok=False, message="module name is required")
        try:
            patch = json.loads(request.config_json) if request.config_json else {}
        except json.JSONDecodeError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cpb2.RoleTaskResponse(ok=False, message=f"invalid config_json: {exc}")
        try:
            self._registry.reconfigure(name, patch)
            return cpb2.RoleTaskResponse(ok=True, message=f"module '{name}' reconfigured")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except Exception as exc:
            logger.exception("ConfigureModule failed for '%s'", name)
            context.set_code(grpc.StatusCode.INTERNAL)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))

    def GetModuleLogs(self, request: Any, context: Any) -> mpb2.ModuleLogsResponse:
        name = (request.name or "").strip()
        # tail=0 means "return all available lines from the in-memory deque"
        tail = 0 if request.entire_buffer else (int(request.tail_lines) if request.tail_lines > 0 else 100)
        if not name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return mpb2.ModuleLogsResponse(lines=[])
        try:
            lines = self._registry.get_logs(name, tail=tail)
            return mpb2.ModuleLogsResponse(lines=lines)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return mpb2.ModuleLogsResponse(lines=[])
        except Exception:
            logger.exception("GetModuleLogs failed for '%s'", name)
            context.set_code(grpc.StatusCode.INTERNAL)
            return mpb2.ModuleLogsResponse(lines=[])

    def SubmitModuleJob(self, request: Any, context: Any) -> mpb2.SubmitModuleJobResponse:
        name = (request.module_name or "").strip()
        if not name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return mpb2.SubmitModuleJobResponse(job_id="", ok=False, error="module_name is required")
        try:
            payload = json.loads(request.payload_json) if request.payload_json else {}
        except json.JSONDecodeError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return mpb2.SubmitModuleJobResponse(job_id="", ok=False, error=f"invalid payload_json: {exc}")
        try:
            job_id = self._registry.submit_job(name, payload)
            return mpb2.SubmitModuleJobResponse(job_id=job_id, ok=True, error="")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return mpb2.SubmitModuleJobResponse(job_id="", ok=False, error=str(exc))
        except NotImplementedError as exc:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            return mpb2.SubmitModuleJobResponse(job_id="", ok=False, error=str(exc))
        except Exception as exc:
            logger.exception("SubmitModuleJob failed for '%s'", name)
            context.set_code(grpc.StatusCode.INTERNAL)
            return mpb2.SubmitModuleJobResponse(job_id="", ok=False, error=str(exc))

