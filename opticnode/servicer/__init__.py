"""OpticNodeServicer: composed from CoreMixin and ModulesMixin."""

from __future__ import annotations

from typing import Any

from opticnode.generated.opticnode_pb2_grpc import OpticNodeServicer as _BaseServicer
from opticnode.modules.base import ModuleRegistry
from opticnode.app.telemetry import TelemetryEngine
from opticnode.servicer.core import CoreMixin
from opticnode.servicer.modules_rpc import ModulesMixin


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
