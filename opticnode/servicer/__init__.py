"""OpticNodeServicer: composed from CoreMixin and ModulesMixin."""

from __future__ import annotations

from typing import Any

from ..generated.opticnode_pb2_grpc import OpticNodeServicer as _BaseServicer
from ..modules.base import ModuleRegistry
from ..telemetry import TelemetryEngine
from .core import CoreMixin
from .modules_rpc import ModulesMixin


class OpticNodeServicer(CoreMixin, ModulesMixin, _BaseServicer):
    """gRPC servicer implementation composed from CoreMixin and ModulesMixin."""

    def __init__(
        self,
        settings: Any,
        telemetry: TelemetryEngine,
        registry: ModuleRegistry,
    ) -> None:
        super().__init__(telemetry=telemetry, registry=registry)
        self._settings = settings
