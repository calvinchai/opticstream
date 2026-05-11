"""OpticStream Hub: multipage app with static pages."""

from __future__ import annotations

import streamlit as st

from optichub.dashboard.hub_ui import hub_settings
from optichub.dashboard.views import node_detail, nodes, overview, project_detail, projects
from optichub.redis_client import list_manage_node_ids


def main() -> None:
    st.set_page_config(page_title="OpticStream Hub", layout="wide")
    st.title("OpticStream Hub")

    settings = hub_settings()
    node_ids = list_manage_node_ids(settings.redis_url)

    open_node = st.session_state.get("_hub_open_node")
    if open_node is not None and open_node not in node_ids:
        st.session_state.pop("_hub_open_node", None)
        open_node = None

    open_project = st.session_state.get("_hub_open_project")

    dashboard_page = st.Page(
        overview.main,
        title="Dashboard",
        icon=":material/dashboard:",
        default=open_node is None and open_project is None,
    )
    nodes_page = st.Page(
        nodes.main,
        title="Nodes",
        icon=":material/dns:",
    )
    node_detail_page = st.Page(
        node_detail.main,
        title="Node",
        icon=":material/memory:",
        default=open_node is not None,
    )
    projects_page = st.Page(
        projects.main,
        title="Projects",
        icon=":material/folder:",
    )
    project_detail_page = st.Page(
        project_detail.main,
        title="Project",
        icon=":material/science:",
        default=open_project is not None and open_node is None,
    )

    pg = st.navigation(
        {
            "Hub": [dashboard_page, nodes_page, projects_page],
            "": [node_detail_page, project_detail_page],
        }
    )
    pg.run()


if __name__ == "__main__":
    main()
