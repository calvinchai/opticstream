"""OpticStream Hub: multipage app with page scripts under pages/."""

from __future__ import annotations

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from optichub.dashboard.hub_ui import hub_settings
from optichub.redis_client import list_manage_node_ids


def main() -> None:
    st.set_page_config(page_title="OpticStream Hub", layout="wide")
    st.title("OpticStream Hub")

    settings = hub_settings()
    st_autorefresh(interval=int(settings.dashboard_refresh_s * 1000), key="hub_app_autorefresh")
    node_ids = list_manage_node_ids(settings.redis_url)
    open_node = st.session_state.get("_hub_open_node")
    if open_node is not None and open_node not in node_ids:
        st.session_state.pop("_hub_open_node", None)
        open_node = None

    open_project = st.session_state.get("_hub_open_project")

    dashboard_page = st.Page(
        "pages/Overview.py",
        title="Dashboard",
        icon=":material/dashboard:",
        default=open_node is None and open_project is None,
    )
    nodes_page = st.Page(
        "pages/Nodes.py",
        title="Nodes",
        icon=":material/dns:",
    )
    node_detail_page = st.Page(
        "pages/Node_detail.py",
        title="Node",
        icon=":material/memory:",
        default=open_node is not None,
        visibility="hidden",
    )
    projects_page = st.Page(
        "pages/Projects.py",
        title="Projects",
        icon=":material/folder:",
    )
    settings_page = st.Page(
        "pages/Settings.py",
        title="Settings",
        icon=":material/settings:",
    )
    project_detail_page = st.Page(
        "pages/Project_detail.py",
        title="Project",
        icon=":material/science:",
        default=open_project is not None and open_node is None,
        visibility="hidden",
    )

    pg = st.navigation(
        {
            "Hub": [dashboard_page, nodes_page, projects_page, settings_page],
            "": [node_detail_page, project_detail_page],
        }
    )
    pg.run()


if __name__ == "__main__":
    main()
