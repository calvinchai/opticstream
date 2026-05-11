"""PrefectWorkerServicer: typed gRPC service for the prefect_worker module."""

from __future__ import annotations

import logging
from typing import Any

import grpc  # type: ignore[reportMissingModuleSource]

from opticapi.generated import common_pb2 as cpb2
from opticapi.generated.prefect_worker_pb2_grpc import PrefectWorkerServicer as _BaseServicer
from ..modules.base import ModuleRegistry

logger = logging.getLogger(__name__)

_MODULE_NAME = "prefect_worker"


class PrefectWorkerServicer(_BaseServicer):
    """gRPC servicer that routes typed PrefectWorker RPCs to the module registry."""

    def __init__(self, registry: ModuleRegistry) -> None:
        self._registry = registry

    def Start(self, request: Any, context: Any) -> cpb2.RoleTaskResponse:
        config: dict[str, Any] = {"auto_restart": bool(request.auto_restart)}
        if request.work_pool:
            config["work_pool"] = request.work_pool.strip()
        if request.worker_count > 0:
            config["worker_count"] = int(request.worker_count)
        if request.extra_args:
            config["extra_args"] = list(request.extra_args)

        try:
            self._registry.start(_MODULE_NAME, config)
            pool = config.get("work_pool", "default")
            return cpb2.RoleTaskResponse(ok=True, message=f"prefect_worker started on pool '{pool}'")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except (ValueError, RuntimeError) as exc:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except Exception as exc:
            logger.exception("PrefectWorker.Start failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
