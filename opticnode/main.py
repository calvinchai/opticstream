"""Entry point: load configuration, start background threads, and run services."""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import queue
import signal
import threading

from .config import Settings
from .heartbeat import HeartbeatLoop, ResilientRedisLogHandler
from .server import create_server, serve_blocking
from .servicer import OpticNodeServicer
from .telemetry import TelemetryEngine
from .utils.network import classify_interfaces
from .work_queue import WorkQueue

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
    log_handler = ResilientRedisLogHandler(list_max=settings.log_buffer_size)
    hb = HeartbeatLoop(settings, stop, telemetry, planes, log_handler=log_handler)
    hb_thread = threading.Thread(target=hb.run, name="heartbeat", daemon=True)
    hb_thread.start()

    if settings.auto_update and settings.github_repo.strip():
        from .updater import UpdateChecker

        uc = UpdateChecker(settings, stop)
        threading.Thread(target=uc.run, name="updater", daemon=True).start()

    work_queue = WorkQueue(settings)
    servicer = OpticNodeServicer(settings, telemetry, work_queue)
    server = create_server(settings, servicer)

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
        work_queue.stop()


if __name__ == "__main__":
    main()
