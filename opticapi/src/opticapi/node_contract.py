"""Redis keys and log channel IDs shared between opticnode and optichub.

Every Redis key written or read by the hub–node protocol is defined here so
that opticnode and optichub never diverge on key names.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Static keys
# ---------------------------------------------------------------------------

NODES_SET_KEY = "opticnode:nodes"
"""Registry set written by node heartbeats; read by hub."""

# ---------------------------------------------------------------------------
# Per-node key builders  (all prefixed ``opticnode:{node_id}:``)
# ---------------------------------------------------------------------------

def node_key_prefix(node_id: str) -> str:
    return f"opticnode:{node_id}"

def node_meta_key(node_id: str) -> str:
    return f"opticnode:{node_id}:meta"

def node_last_seen_key(node_id: str) -> str:
    return f"opticnode:{node_id}:last_seen"

def node_stats_key(node_id: str) -> str:
    return f"opticnode:{node_id}:stats"

def node_logs_key(node_id: str, module_name: str) -> str:
    return f"opticnode:{node_id}:logs:{module_name}"

def node_module_config_key(node_id: str) -> str:
    return f"opticnode:{node_id}:module_config"

def node_primocache_stats_key(node_id: str) -> str:
    return f"opticnode:{node_id}:primocache_stats"

def node_update_key(node_id: str) -> str:
    return f"opticnode:{node_id}:update"

# ---------------------------------------------------------------------------
# Module IDs
# ---------------------------------------------------------------------------

# Module names that publish per-module logs to Redis via ``node_logs_key``.
# Keep aligned with ModuleLog Redis keys and factories in opticnode.app.runtime.
LOG_MODULE_IDS: frozenset[str] = frozenset(
    {
        "command_runner",
        "prefect_worker",
        "redis_queue_worker",
        "redis_queue_burst_worker",
        "lsm_process_server",
        "oct_process_server",
        "lsm_watcher",
        "oct_watcher",
        "primocache_monitor",
    }
)
