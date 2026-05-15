"""OpticNode desktop UI: dashboard, settings, and per-module tabs."""

from __future__ import annotations

import json
import threading
import time
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
from typing import TYPE_CHECKING, Any

from opticnode.app.config import Settings, default_settings_path
from opticnode.app.redis_utils import make_redis_client
from opticnode.gui.log_viewer import LogPanel
from opticnode.utils.network import classify_interfaces

if TYPE_CHECKING:
    from opticnode.app.runtime import NodeRuntime


class ModuleTab(ttk.Frame):
    """One module: status, JSON config, lifecycle actions, log stream."""

    def __init__(
        self,
        master: tk.Misc,
        runtime: NodeRuntime,
        module_name: str,
        log_queue: Any,
    ) -> None:
        super().__init__(master)
        self._runtime = runtime
        self._name = module_name
        self._root = master.winfo_toplevel()
        self._form_fields: list[tuple[str, str, str]] = _MODULE_FORM_FIELDS.get(module_name, [])
        self._form_vars: dict[str, tk.Variable] = {}

        top = ttk.Frame(self)
        top.pack(fill="x", padx=4, pady=4)
        self._state_var = tk.StringVar(value="")
        self._err_var = tk.StringVar(value="")
        ttk.Label(top, textvariable=self._state_var, font=("TkDefaultFont", 10, "bold")).pack(anchor="w")
        ttk.Label(top, textvariable=self._err_var, foreground="red").pack(anchor="w")

        if self._form_fields:
            form = ttk.LabelFrame(self, text="Configuration")
            form.pack(fill="x", padx=4, pady=4)
            for row, (label, key, kind) in enumerate(self._form_fields):
                ttk.Label(form, text=label).grid(row=row, column=0, sticky="w", padx=4, pady=2)
                if kind == "bool":
                    v = tk.BooleanVar(value=False)
                    w = ttk.Checkbutton(form, variable=v)
                else:
                    v = tk.StringVar(value="")
                    w = ttk.Entry(form, textvariable=v, width=52)
                w.grid(row=row, column=1, sticky="ew", padx=4, pady=2)
                self._form_vars[key] = v
            form.columnconfigure(1, weight=1)

        ttk.Label(self, text="Config (JSON):").pack(anchor="w", padx=4)
        json_height = 4 if self._form_fields else 10
        self._json = scrolledtext.ScrolledText(self, height=json_height, wrap="word", font=("TkFixedFont", 10))
        self._json.pack(fill="both", expand=False, padx=4, pady=2)

        btn = ttk.Frame(self)
        btn.pack(fill="x", padx=4, pady=4)
        ttk.Button(btn, text="Start", command=self._on_start).pack(side="left", padx=2)
        ttk.Button(btn, text="Stop", command=self._on_stop).pack(side="left", padx=2)
        ttk.Button(btn, text="Apply", command=self._on_apply).pack(side="left", padx=2)
        ttk.Button(btn, text="Reload config", command=self._reload_json).pack(side="left", padx=2)

        ttk.Label(self, text="Logs:").pack(anchor="w", padx=4)
        self._log = LogPanel(self, log_queue, height=14)
        self._log.pack(fill="both", expand=True, padx=4, pady=2)

        self._reload_json()
        self._refresh_status()

    def poll_logs(self) -> None:
        self._log.poll()

    def _refresh_status(self) -> None:
        st = self._runtime.get_registry().status_for(self._name)
        self._state_var.set(f"State: {st.state.value}")
        self._err_var.set(f"Error: {st.error}" if st.error else "")

    def _reload_json(self) -> None:
        st = self._runtime.get_registry().status_for(self._name)
        cfg = st.config
        body = json.dumps(cfg, indent=2)
        self._json.delete("1.0", "end")
        self._json.insert("1.0", body)
        self._load_form(cfg)
        self._refresh_status()

    def _load_form(self, cfg: dict[str, Any]) -> None:
        for _label, key, kind in self._form_fields:
            var = self._form_vars[key]
            val = cfg.get(key)
            if kind == "bool":
                var.set(bool(val))
            elif val is None:
                var.set("")
            else:
                var.set(str(val))

    def _parse_json(self) -> dict[str, Any]:
        if self._form_fields:
            return self._gather_form()
        raw = self._json.get("1.0", "end").strip()
        if not raw:
            return {}
        return json.loads(raw)

    def _gather_form(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for _label, key, kind in self._form_fields:
            var = self._form_vars[key]
            if kind == "bool":
                out[key] = bool(var.get())
            elif kind == "int":
                raw = str(var.get()).strip()
                out[key] = int(raw) if raw else 0
            elif kind == "float":
                raw = str(var.get()).strip()
                out[key] = float(raw) if raw else 0.0
            else:
                out[key] = str(var.get()).strip()
        return out

    def _bg(self, fn: Any) -> None:
        def work() -> None:
            try:
                fn()

                def done() -> None:
                    self._refresh_status()
                    self._reload_json()

                self._root.after(0, done)
            except Exception as exc:  # noqa: BLE001 — surface to user
                self._root.after(
                    0,
                    lambda: messagebox.showerror("OpticNode", str(exc), parent=self._root),
                )

        threading.Thread(target=work, daemon=True, name=f"gui-{self._name}").start()

    def _on_start(self) -> None:
        try:
            cfg = self._parse_json()
        except json.JSONDecodeError as exc:
            messagebox.showerror("OpticNode", f"Invalid JSON: {exc}", parent=self._root)
            return

        def go() -> None:
            self._runtime.get_registry().start(self._name, cfg)

        self._bg(go)

    def _on_stop(self) -> None:
        def go() -> None:
            self._runtime.get_registry().stop(self._name)

        self._bg(go)

    def _on_apply(self) -> None:
        try:
            patch = self._parse_json()
        except json.JSONDecodeError as exc:
            messagebox.showerror("OpticNode", f"Invalid JSON: {exc}", parent=self._root)
            return

        def go() -> None:
            self._runtime.get_registry().reconfigure(self._name, patch)

        self._bg(go)


class MainWindow:
    """Primary Toplevel: Dashboard, Settings, Modules (with core log tab)."""

    def __init__(self, master: tk.Tk, runtime: NodeRuntime) -> None:
        self._master = master
        self._runtime = runtime
        self._win = tk.Toplevel(master)
        self._win.title("OpticNode")
        self._win.geometry("1024x720")
        self._log_panels: list[LogPanel] = []
        self._module_tabs: list[ModuleTab] = []

        self._win.protocol("WM_DELETE_WINDOW", self.hide)

        nb = ttk.Notebook(self._win)
        nb.pack(fill="both", expand=True, padx=4, pady=4)

        dash = ttk.Frame(nb)
        self._build_dashboard(dash)
        nb.add(dash, text="Dashboard")

        sett = ttk.Frame(nb)
        self._build_settings(sett)
        nb.add(sett, text="Settings")

        mod_nb = ttk.Notebook(nb)
        nb.add(mod_nb, text="Modules")

        queues = runtime.get_log_queues()
        core_f = ttk.Frame(mod_nb)
        ttk.Label(core_f, text="Core (root logger)").pack(anchor="w", padx=4, pady=2)
        core_log = LogPanel(core_f, queues.get("core"), height=28)
        core_log.pack(fill="both", expand=True, padx=4, pady=4)
        self._log_panels.append(core_log)
        mod_nb.add(core_f, text="core")

        for name in runtime.get_registry().registered_module_names():
            tab = ttk.Frame(mod_nb)
            mt = ModuleTab(tab, runtime, name, queues.get(name))
            mt.pack(fill="both", expand=True)
            self._module_tabs.append(mt)
            self._log_panels.append(mt._log)
            mod_nb.add(tab, text=name)

        self._poll_logs()
        self._schedule_dashboard_refresh()

    def _poll_logs(self) -> None:
        if not self._win.winfo_exists():
            return
        for p in self._log_panels:
            p.poll()
        for t in self._module_tabs:
            t.poll_logs()
        self._win.after(150, self._poll_logs)

    def _schedule_dashboard_refresh(self) -> None:
        if not self._win.winfo_exists():
            return
        self._refresh_dashboard()
        self._win.after(1800, self._schedule_dashboard_refresh)

    def _build_dashboard(self, parent: ttk.Frame) -> None:
        self._redis_var = tk.StringVar(value="Redis: …")
        ttk.Label(parent, textvariable=self._redis_var).pack(anchor="w", padx=4, pady=(4, 0))

        self._dash_info = scrolledtext.ScrolledText(parent, height=8, wrap="word", state="disabled")
        self._dash_info.pack(fill="x", padx=4, pady=4)

        cols = ("module", "state", "error", "uptime_s")
        self._mod_tree = ttk.Treeview(parent, columns=cols, show="headings", height=12)
        for c, w in zip(cols, (180, 100, 320, 80), strict=True):
            self._mod_tree.heading(c, text=c.replace("_", " ").title())
            self._mod_tree.column(c, width=w)
        self._mod_tree.pack(fill="both", expand=True, padx=4, pady=4)

    def _refresh_dashboard(self) -> None:
        s = self._runtime.get_settings()
        planes = classify_interfaces(mgmt_iface=s.mgmt_iface, data_iface=s.data_iface)
        snap = self._runtime.snapshot_telemetry()
        net_lines = ", ".join(f"{n.name} tx={n.bytes_sent} rx={n.bytes_recv}" for n in snap.net[:8])
        if len(snap.net) > 8:
            net_lines += " …"

        lines = [
            f"Node ID: {s.node_id}",
            f"gRPC: {s.grpc_host}:{s.grpc_port}",
            f"Redis URL: {s.redis_url}",
            f"Mgmt ifaces: {', '.join(planes.mgmt)}  IP: {planes.mgmt_ip}",
            f"Data ifaces: {', '.join(planes.data)}  IP: {planes.data_ip}",
            f"Telemetry: CPU {snap.cpu_pct:.1f}%  RAM {snap.ram_used_pct:.1f}%",
            f"Net (B/s): {net_lines or 'n/a'}",
            f"Settings file: {default_settings_path()}",
        ]
        self._dash_info.configure(state="normal")
        self._dash_info.delete("1.0", "end")
        self._dash_info.insert("1.0", "\n".join(lines) + "\n")
        self._dash_info.configure(state="disabled")

        for row in self._mod_tree.get_children():
            self._mod_tree.delete(row)
        now = time.time()
        reg = self._runtime.get_registry()
        for name in reg.registered_module_names():
            st = reg.status_for(name)
            up = ""
            if st.started_at is not None:
                up = f"{now - st.started_at:.0f}"
            err = (st.error or "")[:200]
            self._mod_tree.insert("", "end", values=(st.name, st.state.value, err, up))

        self._redis_ping_async()

    def _redis_ping_async(self) -> None:
        url = self._runtime.get_settings().redis_url

        def work() -> None:
            try:
                r = make_redis_client(url)
                r.ping()
                msg = "Redis ping: OK"
            except Exception as exc:  # noqa: BLE001
                msg = f"Redis ping: FAILED ({exc})"

            def done() -> None:
                if self._win.winfo_exists():
                    self._redis_var.set(msg)

            self._win.after(0, done)

        threading.Thread(target=work, daemon=True, name="redis-ping").start()

    def _build_settings(self, parent: ttk.Frame) -> None:
        self._sett_vars: dict[str, tk.Variable] = {}
        grid = ttk.Frame(parent)
        grid.pack(fill="both", expand=True, padx=4, pady=4)

        row = 0
        for label, key, kind in _SETTINGS_FIELDS:
            ttk.Label(grid, text=label).grid(row=row, column=0, sticky="nw", pady=2)
            if kind == "bool":
                v = tk.BooleanVar(value=False)
                w = ttk.Checkbutton(grid, variable=v)
            else:
                v = tk.StringVar(value="")
                w = ttk.Entry(grid, textvariable=v, width=64)
            w.grid(row=row, column=1, sticky="ew", pady=2)
            self._sett_vars[key] = v
            row += 1

        grid.columnconfigure(1, weight=1)

        hint = (
            "Saving writes opticnode.json. Restart OpticNode for gRPC listen address, "
            "Redis URL, node_id, log paths, NIC overrides, and similar fields to apply everywhere."
        )
        ttk.Label(parent, text=hint, wraplength=900, justify="left").pack(anchor="w", padx=4, pady=8)

        bf = ttk.Frame(parent)
        bf.pack(fill="x", padx=4, pady=4)
        ttk.Button(bf, text="Save", command=self._save_settings).pack(side="left", padx=2)
        ttk.Button(bf, text="Revert from disk", command=self._revert_settings).pack(side="left", padx=2)

        self._load_settings_into_ui()

    def _load_settings_into_ui(self) -> None:
        s = self._runtime.get_settings()
        d = s.model_dump(mode="json")
        for label, key, kind in _SETTINGS_FIELDS:
            var = self._sett_vars[key]
            val = d.get(key)
            if kind == "bool":
                var.set(bool(val))
            else:
                if val is None:
                    var.set("")
                else:
                    var.set(str(val))

    def _gather_settings_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for label, key, kind in _SETTINGS_FIELDS:
            var = self._sett_vars[key]
            if kind == "bool":
                out[key] = bool(var.get())
            elif kind == "int":
                raw = str(var.get()).strip()
                out[key] = int(raw)
            elif kind == "float":
                raw = str(var.get()).strip()
                out[key] = float(raw)
            else:
                raw = str(var.get()).strip()
                if key in ("mgmt_iface", "data_iface", "advertised_host") and raw == "":
                    out[key] = None
                elif key == "log_dir":
                    out[key] = raw or "logs"
                else:
                    out[key] = raw
        return out

    def _save_settings(self) -> None:
        try:
            data = self._gather_settings_dict()
            new_s = Settings.model_validate(data)
            new_s.save()
            self._runtime.replace_settings(new_s)
            messagebox.showinfo("OpticNode", "Settings saved.", parent=self._win)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("OpticNode", str(exc), parent=self._win)

    def _revert_settings(self) -> None:
        self._runtime.reload_settings_from_disk()
        self._load_settings_into_ui()

    def show(self) -> None:
        self._win.deiconify()
        self._win.lift()

    def hide(self) -> None:
        self._win.withdraw()


_MODULE_FORM_FIELDS: dict[str, list[tuple[str, str, str]]] = {
    "lsm_watcher": [
        ("Watch path", "watch_path", "str"),
        ("Project name", "project_name", "str"),
        ("Poll interval (s)", "poll_interval", "int"),
        ("Stability seconds", "stability_seconds", "int"),
        ("Force resend", "force_resend", "bool"),
        ("Slice offset", "slice_offset", "int"),
        ("Prefect deployment", "prefect_deployment", "str"),
        ("Allowed window (min)", "allowed_window_minutes", "float"),
        ("Process cache dir", "process_cache_dir", "str"),
    ],
    "oct_watcher": [
        ("Watch path", "watch_path", "str"),
        ("Project name", "project_name", "str"),
        ("Poll interval (s)", "poll_interval", "int"),
        ("Stability seconds", "stability_seconds", "int"),
        ("Force resend", "force_resend", "bool"),
        ("Slice offset", "slice_offset", "int"),
        ("Mosaic ranges", "mosaic_ranges", "str"),
        ("Project base path", "project_base_path", "str"),
        ("Min complex file size (bytes)", "min_complex_file_size_bytes", "int"),
        ("Prefer spectral for complex+spectral", "prefer_spectral_for_complex_with_spectral", "bool"),
        ("Prefect deployment", "prefect_deployment", "str"),
        ("Allowed window (min)", "allowed_window_minutes", "float"),
    ],
}

_SETTINGS_FIELDS: list[tuple[str, str, str]] = [
    ("Node ID", "node_id", "str"),
    ("Redis URL", "redis_url", "str"),
    ("gRPC host", "grpc_host", "str"),
    ("gRPC port", "grpc_port", "int"),
    ("Heartbeat interval (s)", "heartbeat_interval_s", "float"),
    ("Heartbeat TTL (s)", "heartbeat_ttl_s", "int"),
    ("Redis log tail lines", "redis_log_tail", "int"),
    ("Log directory", "log_dir", "str"),
    ("PrimoCache exe", "primocache_exe", "str"),
    ("GUI mode", "gui_mode", "bool"),
    ("Management NIC override", "mgmt_iface", "str"),
    ("Data NIC override", "data_iface", "str"),
    ("Advertised host (hub)", "advertised_host", "str"),
    ("GitHub repo (owner/repo)", "github_repo", "str"),
    ("Auto update", "auto_update", "bool"),
    ("Update check interval (s)", "update_check_interval_s", "int"),
    ("Updater asset pattern", "updater_asset_pattern", "str"),
]
