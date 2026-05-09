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
from .logging_buffer import NodeLogBuffer, NodeLogHandler
from .modules import ModuleRegistry
from .modules.command_runner import CommandRunnerModule
from .modules.copy_queue import CopyQueueModule
from .modules.prefect_worker import PrefectWorkerModule
from .modules.watcher import WatcherModule
from .generated.command_runner_pb2_grpc import add_CommandRunnerServicer_to_server
from .generated.copy_queue_pb2_grpc import add_CopyQueueServicer_to_server
from .generated.prefect_worker_pb2_grpc import add_PrefectWorkerServicer_to_server
from .generated.watcher_pb2_grpc import add_WatcherServicer_to_server
from .server import create_server, serve_blocking
from .servicer import OpticNodeServicer
from .servicer.command_runner_rpc import CommandRunnerServicer
from .servicer.copy_queue_rpc import CopyQueueServicer
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

    log_queue: queue.Queue[logging.LogRecord] | None = None
    if gui_mode:
        log_queue = queue.Queue(maxsize=500)
        logging.getLogger().addHandler(logging.handlers.QueueHandler(log_queue))

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

    log_buffer = NodeLogBuffer(settings)
    log_fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    node_log_handler = NodeLogHandler(log_buffer)
    node_log_handler.setFormatter(log_fmt)
    logging.getLogger().addHandler(node_log_handler)

    registry = ModuleRegistry(settings, log_buffer=log_buffer)
    registry.register_factory("command_runner", CommandRunnerModule)
    registry.register_factory("prefect_worker", lambda: PrefectWorkerModule(log_buffer))
    registry.register_factory("copy_queue", lambda: CopyQueueModule(settings))
    registry.register_factory("watcher", WatcherModule)
    registry.restore_from_redis()

    hb = HeartbeatLoop(
        settings,
        stop,
        telemetry,
        planes,
        log_buffer=log_buffer,
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
            (add_CopyQueueServicer_to_server, CopyQueueServicer(registry)),
            (add_WatcherServicer_to_server, WatcherServicer(registry)),
            (add_PrefectWorkerServicer_to_server, PrefectWorkerServicer(registry)),
        ],
    )

    try:
        if gui_mode and log_queue is not None:
            from .gui import launch_gui

            def _grpc_serve() -> None:
                server.start()
                server.wait_for_termination()

            threading.Thread(target=_grpc_serve, name="grpc-server", daemon=True).start()
            launch_gui(log_queue, stop, server)
        else:
            serve_blocking(server)
    finally:
        stop.set()
        logging.getLogger().removeHandler(node_log_handler)
        registry.shutdown_all()


if __name__ == "__main__":
    main()
