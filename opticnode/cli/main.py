"""CLI entrypoint: parse arguments, build NodeRuntime, and launch the agent."""

from __future__ import annotations

import argparse
import logging


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

    from opticnode.app.config import Settings
    settings = Settings.load()

    if args.check_update:
        from opticnode.updater import check_update_once
        print(check_update_once(settings))
        return

    from opticnode.app.runtime import NodeRuntime

    gui_mode = bool(settings.gui_mode or args.gui)
    runtime = NodeRuntime(settings, gui_mode=gui_mode)
    runtime.start()

    if gui_mode:
        from opticnode.gui import launch_gui
        runtime.start_grpc_background()
        launch_gui(runtime)
    else:
        runtime.wait()
