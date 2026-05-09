"""CopyQueueServicer: typed gRPC service for the copy_queue module."""

from __future__ import annotations

import logging
from typing import Any

from ..generated import common_pb2 as cpb2
from ..generated import copy_queue_pb2 as cqpb2
from ..generated.copy_queue_pb2_grpc import CopyQueueServicer as _BaseServicer
from ..modules.base import ModuleRegistry

logger = logging.getLogger(__name__)

_MODULE_NAME = "copy_queue"


class CopyQueueServicer(_BaseServicer):
    """gRPC servicer that routes typed CopyQueue RPCs to the module registry."""

    def __init__(self, registry: ModuleRegistry) -> None:
        self._registry = registry

    def Start(self, request: Any, context: Any) -> cpb2.RoleTaskResponse:
        import grpc  # type: ignore[reportMissingModuleSource]

        config: dict[str, Any] = {}
        if request.worker_count > 0:
            config["worker_count"] = int(request.worker_count)
        config["paused"] = bool(request.paused)

        try:
            self._registry.start(_MODULE_NAME, config)
            return cpb2.RoleTaskResponse(ok=True, message="copy_queue started")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except RuntimeError as exc:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))
        except Exception as exc:
            logger.exception("CopyQueue.Start failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return cpb2.RoleTaskResponse(ok=False, message=str(exc))

    def SubmitCopyJob(self, request: Any, context: Any) -> cqpb2.SubmitCopyJobResponse:
        import grpc  # type: ignore[reportMissingModuleSource]

        src = (request.src_path or "").strip()
        dst = (request.dst_path or "").strip()
        if not src or not dst:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return cqpb2.SubmitCopyJobResponse(ok=False, error="src_path and dst_path are required")

        try:
            job_id = self._registry.submit_job(
                _MODULE_NAME,
                {"src_path": src, "dst_path": dst, "move_mode": bool(request.move_mode)},
            )
            return cqpb2.SubmitCopyJobResponse(job_id=job_id, ok=True, error="")
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return cqpb2.SubmitCopyJobResponse(ok=False, error=str(exc))
        except (ValueError, RuntimeError) as exc:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return cqpb2.SubmitCopyJobResponse(ok=False, error=str(exc))
        except Exception as exc:
            logger.exception("CopyQueue.SubmitCopyJob failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return cqpb2.SubmitCopyJobResponse(ok=False, error=str(exc))
