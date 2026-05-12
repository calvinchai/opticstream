"""Hub settings: edit optichub.json fields; each change saves immediately."""

from __future__ import annotations

import os

import streamlit as st

from optichub.config import HubSettings, default_settings_path
from optichub.dashboard.hub_ui import hub_settings

_PREFIX = "hub_cfg_"


def _init_widget_state() -> None:
    disk = HubSettings.load_disk()
    for field, value in disk.model_dump().items():
        key = _PREFIX + field
        if key not in st.session_state:
            st.session_state[key] = value


def _persist_hub_settings() -> None:
    st.session_state.pop("_hub_cfg_err", None)
    try:
        s = HubSettings(
            redis_url=str(st.session_state[_PREFIX + "redis_url"]),
            prefect_server_url=str(st.session_state[_PREFIX + "prefect_server_url"]),
            grpc_ping_timeout_ms=int(st.session_state[_PREFIX + "grpc_ping_timeout_ms"]),
            online_grace_s=float(st.session_state[_PREFIX + "online_grace_s"]),
            dashboard_refresh_s=int(st.session_state[_PREFIX + "dashboard_refresh_s"]),
        )
        s.save()
        hub_settings.clear()
        st.toast("Settings saved", icon=":material/check_circle:")
    except Exception as e:
        st.session_state["_hub_cfg_err"] = str(e)


def main() -> None:
    st.subheader("Settings")
    path = default_settings_path()
    st.caption(f"Configuration file: `{path}`")

    if os.environ.get("REDIS_URL", "").strip():
        st.info(
            "**REDIS_URL** is set in the environment. The running hub uses that value for Redis. "
            "The **Redis URL** field below updates what is stored in the JSON file and is used "
            "when **REDIS_URL** is not set."
        )

    _init_widget_state()

    err = st.session_state.get("_hub_cfg_err")
    if err:
        st.error(f"Could not save settings: {err}")

    st.text_input(
        "Redis URL",
        key=_PREFIX + "redis_url",
        on_change=_persist_hub_settings,
    )
    st.text_input(
        "Prefect server URL",
        help="Base URL for the Prefect server (not used by the hub yet).",
        key=_PREFIX + "prefect_server_url",
        on_change=_persist_hub_settings,
    )
    st.number_input(
        "gRPC ping timeout (ms)",
        min_value=50,
        max_value=30_000,
        step=1,
        key=_PREFIX + "grpc_ping_timeout_ms",
        on_change=_persist_hub_settings,
    )
    st.number_input(
        "Online grace period (s)",
        min_value=0.1,
        max_value=3600.0,
        step=0.1,
        format="%.1f",
        key=_PREFIX + "online_grace_s",
        on_change=_persist_hub_settings,
    )
    st.number_input(
        "Hub auto-refresh (s)",
        min_value=1,
        max_value=60,
        step=1,
        key=_PREFIX + "dashboard_refresh_s",
        on_change=_persist_hub_settings,
    )


main()
