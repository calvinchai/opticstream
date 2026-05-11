"""Entry point: load configuration, start background threads, and run services."""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import queue
import signal
import threading

from .config import Settings
from .heartbeat import HeartbeatLoop
from .modules import ModuleRegistry
from .modules.command_runner import CommandRunnerModule
from .modules.prefect_worker import PrefectWorkerModule
from .modules.primocache_monitor import PrimoCacheMonitorModule
from .modules.redis_queue_worker import RedisQueueBurstWorkerModule, RedisQueueWorkerModule
from .modules.watcher import WatcherModule
from .generated.command_runner_pb2_grpc import add_CommandRunnerServicer_to_server
from .generated.prefect_worker_pb2_grpc import add_PrefectWorkerServicer_to_server
from .generated.watcher_pb2_grpc import add_WatcherServicer_to_server
from .server import create_server, serve_blocking
from .servicer import OpticNodeServicer
from .servicer.command_runner_rpc import CommandRunnerServicer
from .servicer.prefect_worker_rpc import PrefectWorkerServicer
from .servicer.watcher_rpc import WatcherServicer
from .telemetry import TelemetryEngine
from .utils.network import classify_interfaces

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="OpticNode local agent")
    parser.add_argument("--gui", action="store_true", help="Enable Tkinter log viewer + system tray")
    parser.add_argument(
        "--check-update",
        action="store_true",
        help="Check GitHub Releases for a newer build and exit (no apply)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()

    if args.check_update:
        from .updater import check_update_once
        print(check_update_once(settings))
        return

    gui_mode = bool(settings.gui_mode or args.gui)

    stop = threading.Event()

    def _handle_sig(_signum: int, _frame: object | None) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

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

    # Each module gets its own named logger routed to a rotating file + in-memory
    # deque (for RPC) + Redis tail (for post-mortem).  The registry creates a
    # ModuleLog per registered module; gui_mode also creates a per-module queue.
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
    registry.register_factory("watcher", lambda: WatcherModule(settings.redis_url))
    registry.register_factory(
        "primocache_monitor",
        lambda: PrimoCacheMonitorModule(settings.redis_url, settings.node_id, settings.primocache_exe),
    )
    registry.restore_from_redis()

    # Core (non-module) logs go to the root logger.  In GUI mode we capture them
    # on a separate queue so the viewer can show them alongside module logs.
    core_queue: queue.Queue[logging.LogRecord] | None = None
    if gui_mode:
        core_queue = queue.Queue(maxsize=500)
        logging.getLogger().addHandler(logging.handlers.QueueHandler(core_queue))

    hb = HeartbeatLoop(
        settings,
        stop,
        telemetry,
        planes,
        module_registry=registry,
    )
    hb_thread = threading.Thread(target=hb.run, name="heartbeat", daemon=True)
    hb_thread.start()

    if settings.auto_update and settings.github_repo.strip():
        from .updater import UpdateChecker
        uc = UpdateChecker(settings, stop)
        threading.Thread(target=uc.run, name="updater", daemon=True).start()

    servicer = OpticNodeServicer(settings, telemetry, registry)
    server = create_server(
        settings,
        servicer,
        extra_services=[
            (add_CommandRunnerServicer_to_server, CommandRunnerServicer(registry)),
            (add_WatcherServicer_to_server, WatcherServicer(registry)),
            (add_PrefectWorkerServicer_to_server, PrefectWorkerServicer(registry)),
        ],
    )

    try:
        if gui_mode and core_queue is not None:
            from .gui import launch_gui

            all_queues = {**registry.gui_queues, "core": core_queue}

            def _grpc_serve() -> None:
                server.start()
                server.wait_for_termination()

            threading.Thread(target=_grpc_serve, name="grpc-server", daemon=True).start()
            launch_gui(all_queues, stop, server)
        else:
            serve_blocking(server, stop)
    finally:
        stop.set()
        registry.shutdown_all()


if __name__ == "__main__":
    main()
