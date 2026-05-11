"""Per-node detail: modules (overview + per-module tabs), logs, execute (gRPC)."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from typing import Any

import streamlit as st

from optichub.config import HubSettings
from optichub.dashboard.hub_ui import fmt_age, hub_settings
from optichub.dashboard.views.node_modules_ui import (
    KNOWN_NODE_MODULES,
    module_overview_rows,
    render_module_tab,
    render_overview_stop_buttons,
)
from optichub.grpc_client import (
    ExecResult,
    execute_command,
    get_module_logs,
    list_modules,
    ping_node,
)
from optichub.redis_client import (
    NodeRecord,
    get_node,
    get_node_module_logs_redis,
    list_manage_node_ids,
)
from opticnode.logging_buffer import LOG_MODULE_IDS

_HISTORY_CAP = 10


def _wk(node_id: str, suffix: str) -> str:
    h = hashlib.sha256(node_id.encode("utf-8")).hexdigest()[:12]
    return f"{suffix}_{h}"


def _history_key(node_id: str) -> str:
    h = hashlib.sha256(node_id.encode("utf-8")).hexdigest()[:32]
    return f"node_cmd_history_{h}"


def _parse_args(text: str) -> list[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def _result_to_dict(res: ExecResult) -> dict[str, Any]:
    return {
        "ok": res.ok,
        "exit_code": res.exit_code,
        "stdout": res.stdout,
        "stderr": res.stderr,
        "timed_out": res.timed_out,
        "error": res.error,
        "rpc_error": res.rpc_error,
    }


def _render_result(res: ExecResult) -> None:
    if res.rpc_error:
        st.error(f"gRPC error: {res.rpc_error}")
        return

    if res.timed_out:
        st.warning("Command timed out on the node.")

    if res.ok and res.exit_code == 0 and not res.timed_out:
        st.success("Finished (exit code 0).")
    elif res.ok:
        st.warning(f"Finished with exit code {res.exit_code}.")
    else:
        st.error(res.error or "Command reported ok=False.")

    st.metric("Exit code", res.exit_code)
    if res.stdout:
        st.markdown("**stdout**")
        st.code(res.stdout, language="text")
    if res.stderr:
        st.markdown("**stderr**")
        st.code(res.stderr, language="text")


def _render_module_logs_section(
    settings: HubSettings,
    node_id: str,
    rec: NodeRecord,
    *,
    key_fn: Callable[[str], str],
) -> None:
    st.subheader("Module logs")
    st.caption(
        "Live or full history via **gRPC** when the node is reachable. **Redis** stores only a "
        "recent tail per module for when the node is down."
    )
    log_module = st.selectbox(
        "Log module",
        options=sorted(LOG_MODULE_IDS),
        key=key_fn("log_module_select"),
    )
    tail_n = st.number_input(
        "Tail lines",
        min_value=1,
        max_value=10_000,
        value=100,
        step=1,
        key=key_fn("log_tail_n"),
    )
    entire_buf = st.checkbox(
        "Full in-memory buffer (gRPC only)",
        value=False,
        key=key_fn("log_entire_buf"),
    )

    if st.button("Fetch logs", key=key_fn("fetch_module_logs")):
        lines_out: list[str] = []
        source = ""
        if rec.ipv4:
            timeout = 120.0 if entire_buf else 30.0
            lines_grpc, rpc_err = get_module_logs(
                rec.ipv4,
                rec.grpc_port,
                module=log_module,
                tail_lines=int(tail_n),
                entire_buffer=entire_buf,
                timeout_s=timeout,
            )
            if rpc_err is None:
                lines_out = lines_grpc
                source = "gRPC"
            else:
                st.warning(f"gRPC failed ({rpc_err}); showing Redis tail instead.")
                lines_out = get_node_module_logs_redis(
                    settings.redis_url, node_id, log_module, int(tail_n)
                )
                source = "Redis (fallback)"
        else:
            lines_out = get_node_module_logs_redis(
                settings.redis_url, node_id, log_module, int(tail_n)
            )
            source = "Redis (no node IPv4 in meta)"
        if entire_buf and source.startswith("Redis"):
            st.info("Full buffer is only available over gRPC; Redis holds a short tail per module.")
        st.caption(f"**Source:** {source}")
        st.code("\n".join(lines_out) if lines_out else "(empty)", language="text")


def _render_execute_section(
    node_id: str,
    rec: NodeRecord,
    *,
    key_fn: Callable[[str], str],
) -> None:
    hist_key = _history_key(node_id)
    if hist_key not in st.session_state:
        st.session_state[hist_key] = []

    st.subheader("Run command")
    st.caption(
        "Non-shell mode: `command` is argv0, each line in **Args** is an additional argv element. "
        "Shell mode: only **Command** is passed to the shell; **Args** are ignored (opticnode behavior)."
    )

    command = st.text_input(
        "Command",
        value="",
        placeholder="e.g. echo or /usr/bin/uptime",
        key=key_fn("exec_command"),
    )
    args_text = st.text_area(
        "Args (one per line)",
        value="",
        height=100,
        placeholder="hello\nworld",
        key=key_fn("exec_args"),
    )
    shell = st.checkbox("Shell mode", value=False, key=key_fn("exec_shell"))
    timeout_s = st.number_input(
        "Timeout (seconds)",
        min_value=0.1,
        max_value=600.0,
        value=30.0,
        step=1.0,
        key=key_fn("exec_timeout_s"),
    )

    if st.button("Run", type="primary", key=key_fn("exec_run")):
        cmd = command.strip()
        if not cmd:
            st.error("Command must be non-empty.")
        else:
            args_list = _parse_args(args_text)
            with st.spinner("Executing on node…"):
                res = execute_command(
                    rec.ipv4,
                    rec.grpc_port,
                    command=cmd,
                    args=args_list,
                    shell=shell,
                    timeout_s=float(timeout_s),
                )
            _render_result(res)

            entry = {
                "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "command": cmd,
                "args": args_list,
                "shell": shell,
                "timeout_s": float(timeout_s),
                "result": _result_to_dict(res),
            }
            hist: list[dict[str, Any]] = list(st.session_state.get(hist_key, []))
            hist.insert(0, entry)
            st.session_state[hist_key] = hist[:_HISTORY_CAP]

    hist = list(st.session_state.get(hist_key, []))
    if hist:
        st.subheader("Recent runs (this session)")
        for i, entry in enumerate(hist):
            r = entry["result"]
            label = f"{entry['ts']} — `{entry['command'][:40]}{'…' if len(entry['command']) > 40 else ''}`"
            if r.get("rpc_error"):
                label += " [RPC error]"
            elif r.get("timed_out"):
                label += " [timed out]"
            elif r.get("ok") and r.get("exit_code") == 0:
                label += " [ok]"
            else:
                label += f" [exit {r.get('exit_code')}]"
            with st.expander(label, expanded=False, key=key_fn(f"hist_exp_{i}")):
                st.json(entry)


def main() -> None:
    settings: HubSettings = hub_settings()

    node_id = st.session_state.get("_hub_open_node")
    if not node_id:
        st.info("No node selected. Pick a node from the **Dashboard** or **Nodes** page.")
        return

    st.warning(
        "This page runs **arbitrary commands** on the selected opticnode over gRPC. "
        "The hub has **no authentication** — use only on trusted networks."
    )

    known = set(list_manage_node_ids(settings.redis_url))
    if node_id not in known:
        st.error(
            f"Node `{node_id}` is no longer in the registry."
        )
        st.session_state.pop("_hub_open_node", None)
        return

    if st.button(":material/arrow_back: Back to nodes", key="node_detail_back"):
        st.session_state.pop("_hub_open_node", None)
        st.rerun()

    st.header(f"Node `{node_id}`")
    st.caption("Inspect modules, logs, and remote execution for this node.")

    def key_fn(suffix: str) -> str:
        return _wk(node_id, suffix)

    now = time.time()
    rec = get_node(settings.redis_url, node_id, online_grace_s=settings.online_grace_s, now=now)
    grpc_display = f"{rec.ipv4}:{rec.grpc_port}" if rec.ipv4 else f":{rec.grpc_port}"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Status", "online" if rec.online else "offline")
    c2.write(f"**gRPC target:** `{grpc_display}`")
    c3.write(f"**Hostname:** {rec.hostname or '—'}")
    c4.write(f"**Version:** {rec.version or '—'}")

    if rec.last_seen_ts is not None:
        st.caption(f"Last seen: {fmt_age(now - rec.last_seen_ts)}")
    else:
        st.caption("Last seen: never")

    if rec.online and rec.ipv4:
        rtt = ping_node(rec.ipv4, rec.grpc_port, timeout_ms=settings.grpc_ping_timeout_ms)
        if rtt is not None:
            st.caption(f"Current ping latency: **{rtt:.1f} ms**")
        else:
            st.caption("Current ping latency: **unreachable** (gRPC ping failed)")
    else:
        st.caption("Current ping latency: **n/a** (offline or no address)")

    if not rec.ipv4:
        st.error(
            "No advertised IPv4 in Redis for this node — module control and **Run command** need gRPC. "
            "You can still fetch log tails from Redis below."
        )
        _render_module_logs_section(settings, node_id, rec, key_fn=key_fn)
        return

    modules, modules_err = list_modules(rec.ipv4, rec.grpc_port, timeout_s=15.0)
    by_name = {m.name: m for m in modules}

    tab_labels = ["Overview", *[m.replace("_", " ") for m in KNOWN_NODE_MODULES]]
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        st.subheader("Modules (summary)")
        rows, row_err = module_overview_rows(KNOWN_NODE_MODULES, modules, modules_err)
        if row_err:
            st.warning(f"Could not list modules: {row_err}")
        elif not rows:
            st.info("No module rows.")
        else:
            st.dataframe(rows, use_container_width=True, hide_index=True)
        if modules_err is None:
            render_overview_stop_buttons(
                rec.ipv4,
                rec.grpc_port,
                modules,
                KNOWN_NODE_MODULES,
                key_fn,
            )
        if modules_err is None and modules:
            with st.expander("Current config JSON", expanded=False):
                for m in sorted(modules, key=lambda x: x.name):
                    st.markdown(f"**{m.name}** (`{m.state}`)")
                    st.json(m.config)

        st.divider()
        _render_module_logs_section(settings, node_id, rec, key_fn=key_fn)
        st.divider()
        _render_execute_section(node_id, rec, key_fn=key_fn)

    for i, name in enumerate(KNOWN_NODE_MODULES):
        with tabs[i + 1]:
            render_module_tab(
                name,
                host=rec.ipv4,
                port=rec.grpc_port,
                info=by_name.get(name),
                key=key_fn,
            )
