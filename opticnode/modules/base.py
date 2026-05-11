"""Base types and registry for opticnode modules."""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel

from opticnode.app.module_log import ModuleLog
from opticnode.app.redis_utils import make_redis_client

logger = logging.getLogger(__name__)

_HEALTH_INTERVAL: float = 5.0
_MAX_RESTART_DELAY: float = 60.0


class ModuleConfig(BaseModel):
    """Base class for all module configuration models."""


class ModuleState(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"
    RESTARTING = "restarting"


@dataclass
class ModuleStatus:
    name: str
    state: ModuleState
    config: dict
    started_at: float | None
    error: str

    def to_json(self) -> str:
        d = asdict(self)
        d["state"] = self.state.value
        return json.dumps(d)


class NodeModule:
    """Base class for self-supervising modules.

    Once started, a module runs indefinitely. If the underlying work stops
    unexpectedly (_is_alive() returns False), the base supervisor restarts it
    automatically with exponential backoff. Subclasses implement three methods:

        _launch(config)   – start background work (non-blocking)
        _teardown()       – stop background work cleanly (blocking)
        _is_alive()       – return False to trigger a restart (default: True)

    The public interface (start/stop/reconfigure/status) is provided entirely
    by this base class and must not be overridden.
    """

    name: str = ""
    Config: type[ModuleConfig] = ModuleConfig

    def __init__(self) -> None:
        self._config: ModuleConfig = type(self).Config()
        self._state = ModuleState.STOPPED
        self._started_at: float | None = None
        self._error: str = ""
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._supervisor: threading.Thread | None = None

    # ---------- subclass interface ----------

    def _launch(self, config: ModuleConfig) -> None:
        """Start background work. Called on initial start and each restart."""
        raise NotImplementedError

    def _teardown(self) -> None:
        """Stop background work cleanly. Called before restart and during stop()."""
        raise NotImplementedError

    def _is_alive(self) -> bool:
        """Return False to trigger an automatic restart. Default: always healthy."""
        return True

    # ---------- public interface ----------

    def start(self, config: ModuleConfig) -> None:
        with self._lock:
            if self._state in (ModuleState.RUNNING, ModuleState.STARTING):
                raise RuntimeError(f"Module '{self.name}' is already running.")
            self._state = ModuleState.STARTING
            self._config = config
            self._stop_event.clear()

        try:
            self._launch(config)
        except Exception as exc:
            with self._lock:
                self._state = ModuleState.ERROR
                self._error = str(exc)
            raise

        with self._lock:
            self._state = ModuleState.RUNNING
            self._started_at = time.time()
            self._error = ""

        t = threading.Thread(
            target=self._supervise_loop,
            daemon=True,
            name=f"supervisor-{self.name}",
        )
        with self._lock:
            self._supervisor = t
        t.start()
        logger.info("Module '%s' started.", self.name)

    def stop(self) -> None:
        with self._lock:
            if self._state == ModuleState.STOPPED:
                return
            prev_state = self._state
            self._state = ModuleState.STOPPING
            self._stop_event.set()
            t = self._supervisor

        if prev_state != ModuleState.ERROR:
            self._teardown()

        if t is not None and t.is_alive():
            t.join(timeout=5.0)

        with self._lock:
            self._state = ModuleState.STOPPED
            self._started_at = None
            self._supervisor = None
        logger.info("Module '%s' stopped.", self.name)

    def reconfigure(self, patch: dict) -> None:
        with self._lock:
            new_config = type(self._config).model_validate(
                {**self._config.model_dump(), **patch}
            )
        self.stop()
        self.start(new_config)

    def status(self) -> ModuleStatus:
        with self._lock:
            return ModuleStatus(
                name=self.name,
                state=self._state,
                config=self._config.model_dump(),
                started_at=self._started_at,
                error=self._error,
            )

    def submit_job(self, payload: dict) -> str:
        raise NotImplementedError(f"Module '{self.name}' does not support job submission")


    def _supervise_loop(self) -> None:
        """Polls health every _HEALTH_INTERVAL seconds and restarts on failure."""
        restart_count = 0
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=_HEALTH_INTERVAL)
            if self._stop_event.is_set():
                break

            with self._lock:
                state = self._state

            if state != ModuleState.RUNNING:
                continue

            if self._is_alive():
                restart_count = 0
                continue

            # Module died unexpectedly.
            with self._lock:
                self._state = ModuleState.ERROR
                self._error = "stopped unexpectedly"
            logger.warning("Module '%s': stopped unexpectedly.", self.name)

            delay = min(2 ** restart_count, _MAX_RESTART_DELAY)
            restart_count += 1
            logger.info(
                "Module '%s': restarting in %.0fs (attempt %d).",
                self.name,
                delay,
                restart_count,
            )

            if self._stop_event.wait(timeout=delay):
                break

            with self._lock:
                if self._stop_event.is_set():
                    break
                config = self._config
                self._state = ModuleState.RESTARTING

            try:
                self._teardown()
                self._launch(config)
                with self._lock:
                    self._state = ModuleState.RUNNING
                    self._started_at = time.time()
                    self._error = ""
                logger.info("Module '%s': restarted successfully.", self.name)
            except Exception as exc:
                with self._lock:
                    self._state = ModuleState.ERROR
                    self._error = str(exc)
                logger.exception("Module '%s': restart failed.", self.name)
                # Loop continues; will retry after next health interval + backoff.


class LoopModule(NodeModule):
    """NodeModule for modules that run a single background-thread loop.

    Subclasses implement ``_run_loop()``, which should gate its while-condition
    on ``self._stop_event`` and use ``self._stop_event.wait(timeout=...)`` for
    sleeping so that teardown is prompt:

        def _run_loop(self) -> None:
            while not self._stop_event.is_set():
                self._do_work()
                self._stop_event.wait(timeout=self._interval_s)

    ``_launch``, ``_teardown``, and ``_is_alive`` are provided by this class and
    must not be overridden. Subclasses that need per-launch setup should call
    ``super()._launch(config)`` after their own setup (thread is started there).
    """

    _thread_join_timeout: float = 10.0

    def __init__(self) -> None:
        super().__init__()
        self._thread: threading.Thread | None = None

    def _run_loop(self) -> None:
        raise NotImplementedError

    def _launch(self, config: ModuleConfig) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name=self.name,
        )
        self._thread.start()

    def _teardown(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._thread_join_timeout)
        self._thread = None

    def _is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()


class ModuleRegistry:
    """Manages lifecycle of all registered node modules."""

    def __init__(self, settings: Any, *, gui_mode: bool = False) -> None:
        self._settings = settings
        self._log_dir = Path(getattr(settings, "log_dir", "logs"))
        self._redis_tail = getattr(settings, "redis_log_tail", 100)
        self._gui_mode = gui_mode
        self._factories: dict[str, Callable[[], NodeModule]] = {}
        self._modules: dict[str, NodeModule] = {}
        self._module_logs: dict[str, ModuleLog] = {}
        self._gui_queues: dict[str, queue.Queue[logging.LogRecord]] = {}
        self._lock = threading.Lock()
        self._redis: Any = None
        self._connect_redis()

    def _connect_redis(self) -> None:
        try:
            self._redis = make_redis_client(self._settings.redis_url)
        except Exception:
            logger.warning("ModuleRegistry: Redis unavailable; config persistence disabled.")
            self._redis = None

    def _config_key(self) -> str:
        return f"opticnode:{self._settings.node_id}:module_config"

    def _persist_config(self, name: str, config: dict, enabled: bool) -> None:
        if self._redis is None:
            return
        try:
            self._redis.hset(self._config_key(), name, json.dumps({"enabled": enabled, **config}))
        except Exception:
            logger.warning("ModuleRegistry: failed to persist config for %s", name)

    def _delete_config(self, name: str) -> None:
        if self._redis is None:
            return
        try:
            self._redis.hdel(self._config_key(), name)
        except Exception:
            logger.warning("ModuleRegistry: failed to delete config for %s", name)

    def register_factory(self, name: str, factory: Callable[[], NodeModule]) -> None:
        gui_q: queue.Queue[logging.LogRecord] | None = None
        if self._gui_mode:
            gui_q = queue.Queue(maxsize=500)
            self._gui_queues[name] = gui_q
        with self._lock:
            self._factories[name] = factory
            self._module_logs[name] = ModuleLog(
                name,
                self._log_dir,
                redis_tail=self._redis_tail,
                gui_queue=gui_q,
            )

    def set_redis_all(self, client: Any) -> None:
        """Propagate a Redis client (or None) to all module log handlers."""
        with self._lock:
            logs = dict(self._module_logs)
        for mod_log in logs.values():
            mod_log.set_redis(client)

    @property
    def gui_queues(self) -> dict[str, queue.Queue[logging.LogRecord]]:
        return dict(self._gui_queues)

    def _get_or_create(self, name: str) -> NodeModule:
        with self._lock:
            if name not in self._modules:
                if name not in self._factories:
                    raise KeyError(f"Unknown module: '{name}'. Registered: {list(self._factories)}")
                self._modules[name] = self._factories[name]()
        return self._modules[name]

    def start(self, name: str, config: dict) -> None:
        with self._lock:
            module = self._get_or_create(name)
        parsed = type(module).Config.model_validate(config)
        module.start(parsed)
        self._persist_config(name, parsed.model_dump(), enabled=True)
        logger.info("Module '%s' started.", name)

    def stop(self, name: str) -> None:
        with self._lock:
            module = self._modules.get(name)
        if module is None:
            raise KeyError(f"Module '{name}' is not instantiated.")
        module.stop()
        status = module.status()
        self._persist_config(name, status.config, enabled=False)
        logger.info("Module '%s' stopped.", name)

    def reconfigure(self, name: str, patch: dict) -> None:
        with self._lock:
            module = self._modules.get(name)
        if module is None:
            raise KeyError(f"Module '{name}' is not instantiated.")
        module.reconfigure(patch)
        status = module.status()
        self._persist_config(name, status.config, enabled=True)
        logger.info("Module '%s' reconfigured with %s.", name, patch)

    def submit_job(self, name: str, payload: dict) -> str:
        with self._lock:
            module = self._modules.get(name)
        if module is None:
            raise KeyError(f"Module '{name}' is not instantiated.")
        return module.submit_job(payload)

    def get_job_result(self, name: str, job_id: str) -> dict | None:
        with self._lock:
            module = self._modules.get(name)
        if module is None:
            raise KeyError(f"Module '{name}' is not instantiated.")
        return module.get_job_result(job_id)

    def list_job_results(self, name: str, limit: int | None = None) -> list[dict]:
        with self._lock:
            module = self._modules.get(name)
        if module is None:
            raise KeyError(f"Module '{name}' is not instantiated.")
        return module.list_job_results(limit)

    def list_all(self) -> list[ModuleStatus]:
        with self._lock:
            modules = dict(self._modules)
        return [m.status() for m in modules.values()]

    def get_logs(self, name: str, tail: int = 100) -> list[str]:
        """Return the last ``tail`` log lines for a module (0 = all available)."""
        with self._lock:
            mod_log = self._module_logs.get(name)
        if mod_log is None:
            raise KeyError(f"Unknown module '{name}'. Registered: {list(self._module_logs)}")
        return mod_log.get_tail(tail)

    def restore_from_redis(self) -> None:
        """Re-start modules that were enabled before the last shutdown."""
        if self._redis is None:
            return
        try:
            entries = self._redis.hgetall(self._config_key())
        except Exception:
            logger.warning("ModuleRegistry: could not read saved configs from Redis.")
            return

        for name, raw in entries.items():
            try:
                cfg = json.loads(raw)
            except Exception:
                continue
            enabled = cfg.pop("enabled", False)
            if not enabled:
                continue
            if name not in self._factories:
                logger.warning("ModuleRegistry: saved module '%s' has no factory; skipping.", name)
                continue
            try:
                self.start(name, cfg)
                logger.info("ModuleRegistry: restored module '%s' from Redis config.", name)
            except Exception:
                logger.exception("ModuleRegistry: failed to restore module '%s'.", name)

    def shutdown_all(self, timeout: float = 15.0) -> None:
        with self._lock:
            modules = dict(self._modules)
            logs = dict(self._module_logs)
        for name, module in modules.items():
            try:
                module.stop()
            except Exception:
                logger.exception("ModuleRegistry: error stopping module '%s'.", name)
        for mod_log in logs.values():
            mod_log.close()
