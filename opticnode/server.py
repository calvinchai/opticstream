"""gRPC server construction, registration, and graceful shutdown."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable
from concurrent import futures
from typing import Any

from .config import Settings
from .generated.opticnode_pb2_grpc import add_OpticNodeServicer_to_server

logger = logging.getLogger(__name__)


ServiceRegistration = tuple[Callable[[Any, Any], None], Any]


def create_server(
    settings: Settings,
    servicer: Any | None = None,
    *,
    extra_services: Iterable[ServiceRegistration] | None = None,
) -> Any:
    """Build a threaded gRPC server and register one or more service implementations.

    `servicer`, if provided, is registered as the OpticNode service for backwards
    compatibility. `extra_services` is an iterable of (register_fn, servicer) pairs;
    each module that ships its own gRPC service should add itself here.
    """
    import grpc  # type: ignore[reportMissingModuleSource]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    registered: list[str] = []
    if servicer is not None:
        add_OpticNodeServicer_to_server(servicer, server)
        registered.append("opticnode.OpticNode")
    for register_fn, impl in extra_services or ():
        register_fn(impl, server)
        registered.append(getattr(impl, "service_name", impl.__class__.__name__))

    listen = f"{settings.grpc_host}:{settings.grpc_port}"
    server.add_insecure_port(listen)
    logger.info("gRPC server prepared on %s (services: %s)", listen, ", ".join(registered))
    return server


def serve_blocking(server: Any, stop_event: threading.Event, *, grace: float = 5.0) -> None:
    """Start the server and block until stop_event is set or the server terminates."""
    server.start()

    def _watch() -> None:
        stop_event.wait()
        try:
            server.stop(grace)
        except Exception:
            logger.exception("gRPC server stop failed")

    threading.Thread(target=_watch, name="grpc-stop-watcher", daemon=True).start()
    server.wait_for_termination()
