"""Base types and registry for opticnode modules."""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel

from ..module_log import ModuleLog

logger = logging.getLogger(__name__)


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
    """Base class for all opticnode modules. Subclass and override as needed."""

    name: str = ""
    Config: type[ModuleConfig] = ModuleConfig

    def start(self, config: ModuleConfig) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def reconfigure(self, patch: dict) -> None:
        raise NotImplementedError

    def status(self) -> ModuleStatus:
        raise NotImplementedError

    def submit_job(self, payload: dict) -> str:
        raise NotImplementedError(f"Module '{self.name}' does not support job submission")

    def get_job_result(self, job_id: str) -> dict | None:
        raise NotImplementedError(f"Module '{self.name}' does not support job result lookup")

    def list_job_results(self, limit: int | None = None) -> list[dict]:
        raise NotImplementedError(f"Module '{self.name}' does not support job result listing")


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
            from redis import Redis
            self._redis = Redis.from_url(self._settings.redis_url, decode_responses=True)
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
