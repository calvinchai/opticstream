"""NodeRuntime: owns the agent's lifecycle — services, threads, and shutdown."""

from __future__ import annotations

import logging
import logging.handlers
import queue
import signal
import threading
from pathlib import Path
from typing import Any

from opticnode.app.config import Settings, default_settings_path
from opticnode.app.heartbeat import HeartbeatLoop
from opticnode.app.server import create_server
from opticnode.app.telemetry import TelemetryEngine, TelemetrySnapshot
from opticapi.generated.command_runner_pb2_grpc import add_CommandRunnerServicer_to_server
from opticapi.generated.prefect_worker_pb2_grpc import add_PrefectWorkerServicer_to_server
from opticapi.generated.watcher_pb2_grpc import add_WatcherServicer_to_server
from opticnode.modules import ModuleRegistry
from opticnode.modules.command_runner import CommandRunnerModule
from opticnode.modules.prefect_worker import PrefectWorkerModule
from opticnode.modules.primocache_monitor import PrimoCacheMonitorModule
from opticnode.modules.redis_queue_worker import RedisQueueBurstWorkerModule, RedisQueueWorkerModule
from opticnode.modules.lsm_process_server import LSMProcessServerModule
from opticnode.modules.lsm_watcher import LSMWatcherModule
from opticnode.modules.oct_process_server import OCTProcessServerModule
from opticnode.modules.oct_watcher import OCTWatcherModule
from opticnode.servicer import OpticNodeServicer
from opticnode.servicer.command_runner_rpc import CommandRunnerServicer
from opticnode.servicer.prefect_worker_rpc import PrefectWorkerServicer
from opticnode.servicer.watcher_rpc import WatcherServicer
from opticnode.utils.network import classify_interfaces

logger = logging.getLogger(__name__)


class NodeRuntime:
    """Owns all agent services and their lifecycle.

    Callers (CLI or GUI) start the runtime, wait on it or hand it to the GUI
    layer, and call stop() when they want a clean shutdown.  The GUI should
    never touch the underlying server handle or stop event directly.
    """

    def __init__(self, settings: Settings, *, gui_mode: bool = False) -> None:
        self._settings = settings
        self._gui_mode = gui_mode
        self._stop = threading.Event()
        self._server: Any = None
        self._registry: ModuleRegistry | None = None
        self._telemetry: TelemetryEngine | None = None
        self._core_queue: queue.Queue[logging.LogRecord] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Initialise all services and start background threads.

        Must be called once before wait() or get_log_queues().
        """
        settings = self._settings
        gui_mode = self._gui_mode

        planes = classify_interfaces(
            mgmt_iface=settings.mgmt_iface,
            data_iface=settings.data_iface,
        )
        logger.info(
            "Network planes: mgmt=%s data=%s mgmt_ip=%s data_ip=%s",
            planes.mgmt,
            planes.data,
            planes.mgmt_ip,
            planes.data_ip,
        )

        telemetry = TelemetryEngine(settings, planes)
        self._telemetry = telemetry

        registry = ModuleRegistry(settings, gui_mode=gui_mode)
        registry.register_factory("command_runner", CommandRunnerModule)
        registry.register_factory("prefect_worker", PrefectWorkerModule)
        registry.register_factory(
            "redis_queue_worker",
            lambda: RedisQueueWorkerModule(settings.redis_url),
        )
        registry.register_factory(
            "redis_queue_burst_worker",
            lambda: RedisQueueBurstWorkerModule(settings.redis_url, registry),
        )
        registry.register_factory("lsm_process_server", LSMProcessServerModule)
        registry.register_factory("oct_process_server", OCTProcessServerModule)
        registry.register_factory(
            "lsm_watcher", lambda: LSMWatcherModule(settings.redis_url)
        )
        registry.register_factory(
            "oct_watcher", lambda: OCTWatcherModule(settings.redis_url)
        )
        registry.register_factory(
            "primocache_monitor",
            lambda: PrimoCacheMonitorModule(
                settings.redis_url, settings.node_id, settings.primocache_exe
            ),
        )
        registry.restore_from_redis()
        self._registry = registry

        if gui_mode:
            self._core_queue = queue.Queue(maxsize=500)
            logging.getLogger().addHandler(
                logging.handlers.QueueHandler(self._core_queue)
            )

        hb = HeartbeatLoop(
            settings,
            self._stop,
            telemetry,
            planes,
            module_registry=registry,
        )
        threading.Thread(target=hb.run, name="heartbeat", daemon=True).start()

        if settings.auto_update and settings.github_repo.strip():
            from opticnode.updater import UpdateChecker
            uc = UpdateChecker(settings, self._stop)
            threading.Thread(target=uc.run, name="updater", daemon=True).start()

        servicer = OpticNodeServicer(settings, telemetry, registry)
        self._server = create_server(
            settings,
            servicer,
            extra_services=[
                (add_CommandRunnerServicer_to_server, CommandRunnerServicer(registry)),
                (add_WatcherServicer_to_server, WatcherServicer(registry)),
                (add_PrefectWorkerServicer_to_server, PrefectWorkerServicer(registry)),
            ],
        )

    def stop(self, grace_s: float = 5.0) -> None:
        """Signal all services to stop and begin graceful shutdown."""
        self._stop.set()
        if self._server is not None:
            try:
                self._server.stop(grace_s)
            except Exception:
                logger.exception("gRPC server stop failed")
        if self._registry is not None:
            self._registry.shutdown_all()

    def wait(self) -> None:
        """Start the gRPC server and block until stop() is called.

        For headless (non-GUI) mode: start the server on this thread, install
        signal handlers, and block.
        """
        if self._server is None:
            raise RuntimeError("NodeRuntime.start() must be called before wait()")

        def _handle_sig(_signum: int, _frame: object | None) -> None:
            self.stop()

        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)

        try:
            self._server.start()
            # Use gRPC's blocker, not threading.Event.wait(): SIGINT runs self.stop(),
            # which sets _stop; doing that from a handler while the main thread sits in
            # Event.wait() can deadlock. Native wait releases the GIL.
            self._server.wait_for_termination()
        finally:
            self.stop()

    def start_grpc_background(self) -> None:
        """Start the gRPC server in a background thread (for GUI mode).

        In GUI mode the main thread is owned by Tk, so the gRPC server must
        run on a daemon thread.
        """
        if self._server is None:
            raise RuntimeError("NodeRuntime.start() must be called first")

        def _serve() -> None:
            self._server.start()
            self._server.wait_for_termination()

        threading.Thread(target=_serve, name="grpc-server", daemon=True).start()

    def get_log_queues(self) -> dict[str, queue.Queue[logging.LogRecord]]:
        """Return per-module + core log queues for the GUI log viewer."""
        queues: dict[str, queue.Queue[logging.LogRecord]] = {}
        if self._registry is not None:
            queues.update(self._registry.gui_queues)
        if self._core_queue is not None:
            queues["core"] = self._core_queue
        return queues

    def get_registry(self) -> ModuleRegistry:
        if self._registry is None:
            raise RuntimeError("NodeRuntime.start() must be called before get_registry()")
        return self._registry

    def get_telemetry(self) -> TelemetryEngine:
        if self._telemetry is None:
            raise RuntimeError("NodeRuntime.start() must be called before get_telemetry()")
        return self._telemetry

    def snapshot_telemetry(self) -> TelemetrySnapshot:
        return self.get_telemetry().collect()

    def get_settings(self) -> Settings:
        return self._settings

    def replace_settings(self, settings: Settings) -> None:
        """Point the runtime at a new Settings instance (after GUI save)."""
        self._settings = settings

    def reload_settings_from_disk(self, path: Path | None = None) -> None:
        """Replace in-memory settings from disk (used by GUI Revert)."""
        self._settings = Settings.load(path or default_settings_path())


__all__ = ["NodeRuntime"]
