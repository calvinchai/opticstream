"""WatcherServicer: typed gRPC service; Start targets the lsm_watcher node module."""

from __future__ import annotations

import logging
from typing import Any

import grpc  # type: ignore[reportMissingModuleSource]

from ..generated import common_pb2 as cpb2
from ..generated.watcher_pb2_grpc import WatcherServicer as _BaseServicer
from ..modules.base import ModuleRegistry

logger = logging.getLogger(__name__)

_MODULE_NAME = "lsm_watcher"


class WatcherServicer(_BaseServicer):
    """gRPC servicer that routes typed Watcher RPCs to the module registry."""

    def __init__(self, registry: ModuleRegistry) -> None:
        self._registry = registry

    def Start(self, request: Any, context: Any) -> cpb2.RoleTaskResponse:
        watch_path = (request.watch_path or "").strip()
        if not watch_path:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cpb2.RoleTaskResponse(ok=False, message="watch_path is required")

        try:
            self._registry.start(
                _MODULE_NAME,
                {"watch_path": watch_path, "recursive": bool(request.recursive)},
            )
            return cpb2.RoleTaskResponse(ok=True, message=f"lsm_watcher started on {watch_path!r}")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except (ValueError, RuntimeError) as exc:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except Exception as exc:
            logger.exception("Watcher.Start failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
