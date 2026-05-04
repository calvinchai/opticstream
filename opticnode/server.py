"""gRPC server construction, registration, and graceful shutdown."""

from __future__ import annotations

import logging
from concurrent import futures
from typing import Any

from .config import Settings
from .generated.opticnode_pb2_grpc import add_OpticNodeServicer_to_server

logger = logging.getLogger(__name__)


def create_server(settings: Settings, servicer: Any) -> Any:
    """Build a threaded gRPC server and attach the given servicer implementation."""
    import grpc  # type: ignore[reportMissingModuleSource]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_OpticNodeServicer_to_server(servicer, server)
    listen = f"{settings.grpc_host}:{settings.grpc_port}"
    server.add_insecure_port(listen)
    logger.info("gRPC server prepared on %s (service: opticnode.OpticNode)", listen)
    return server


def serve_blocking(server: Any) -> None:
    """Start the server and block until termination."""
    server.start()
    server.wait_for_termination()
