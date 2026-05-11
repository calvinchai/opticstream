"""Shared Streamlit helpers for the hub dashboard."""

from __future__ import annotations

import streamlit as st

from optichub.config import HubSettings


def fmt_age(seconds: float) -> str:
    if seconds < 1:
        return "< 1s ago"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    m = int(seconds // 60)
    if m < 60:
        return f"{m}m ago"
    h = int(seconds // 3600)
    return f"{h}h ago"


@st.cache_resource
def hub_settings() -> HubSettings:
    return HubSettings.load()
