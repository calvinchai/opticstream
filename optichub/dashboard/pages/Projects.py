"""Projects list page: all LSM and OCT projects from state services."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from opticapi.project_state.lsm_state_service import LSMProjectStateService
from opticapi.project_state.oct_state_service import OCTProjectStateService

from optichub.dashboard.hub_ui import hub_settings
from optichub.dashboard.project_hub import make_state_backend, project_status_icon
from optichub.dashboard.project_setup import ProjectSetupResult
from optichub.dashboard.project_setup import create_lsm_project, create_oct_project


def _optional_path(raw: str) -> Path | None:
    value = raw.strip()
    if not value:
        return None
    return Path(value).expanduser()


def _render_setup_result(result: ProjectSetupResult) -> None:
    st.success(f"Saved Prefect block `{result.block_name}`.")
    if result.redis_project_initialized:
        st.caption("Initialized Redis project state so the project appears in the list below.")
    if result.created:
        st.write("Created directories:")
        for path in result.created:
            st.code(str(path))
    if result.verified:
        st.write("Verified existing directories:")
        for path in result.verified:
            st.code(str(path))
    if not result.created and not result.verified:
        st.caption("No directories were created or verified.")


def _render_add_project_form(backend) -> None:
    with st.expander("Add project", expanded=False):
        st.caption("Create or update an LSM/OCT Prefect config block and ensure project directories exist.")
        project_type = st.selectbox(
            "Project type",
            ["LSM", "OCT"],
            index=1,
            key="add_project_type",
        )

        with st.form("add_project_form"):
            project_name = st.text_input("Project name")
            project_base_path = st.text_input("Project base path", help="Empty uses the CLI default: `.`")

            if project_type == "LSM":
                info_file = st.text_input("Info file", help="Empty uses the CLI default: `./info.mat`")
                output_path = st.text_input("Output path", help="Empty uses the CLI default: `.`")
            else:
                grid_size_x_normal = st.number_input("Grid size X normal", min_value=1, value=1, step=1)
                grid_size_x_tilted = st.number_input("Grid size X tilted", min_value=1, value=1, step=1)
                grid_size_y = st.number_input("Grid size Y", min_value=1, value=1, step=1)

            submitted = st.form_submit_button("Create project", type="primary")

        if not submitted:
            return

        try:
            if project_type == "LSM":
                result = create_lsm_project(
                    project_name,
                    state_backend=backend,
                    project_base_path=_optional_path(project_base_path),
                    info_file=_optional_path(info_file),
                    output_path=_optional_path(output_path),
                )
            else:
                result = create_oct_project(
                    project_name,
                    state_backend=backend,
                    project_base_path=_optional_path(project_base_path),
                    grid_size_x_normal=int(grid_size_x_normal),
                    grid_size_x_tilted=int(grid_size_x_tilted),
                    grid_size_y=int(grid_size_y),
                )
        except Exception as e:
            st.error(f"Could not create project: {e}")
            return

        _render_setup_result(result)


def _render_project_table(
    project_type: str,
    names: list[str],
    peek_fn,
) -> None:
    if not names:
        st.info(f"No {project_type} projects found.")
        return

    rows: list[dict[str, str]] = []
    for name in names:
        try:
            view = peek_fn(name)
            status = view.processing_state.value
            created = view.created_at.strftime("%Y-%m-%d %H:%M")
            updated = view.updated_at.strftime("%Y-%m-%d %H:%M")
        except Exception:
            status = "error"
            created = "—"
            updated = "—"

        rows.append(
            {
                "Project": name,
                "Type": project_type.upper(),
                "Status": status,
                "Created": created,
                "Updated": updated,
            }
        )

    st.dataframe(rows, use_container_width=True, hide_index=True)

    for name in names:
        col_name, col_open = st.columns([5, 1])
        try:
            view = peek_fn(name)
            status = view.processing_state.value
        except Exception:
            status = "error"
        col_name.write(f"{project_status_icon(status)} `{name}`")
        if col_open.button("Open", key=f"proj_open_{project_type}_{name}", use_container_width=True):
            st.session_state["_hub_open_project"] = name
            st.session_state["_hub_open_project_type"] = project_type
            st.switch_page("pages/Project_detail.py")


def main() -> None:
    st.subheader("Projects")

    settings = hub_settings()
    backend = make_state_backend(settings.redis_url)

    

    lsm_svc = LSMProjectStateService(backend)
    oct_svc = OCTProjectStateService(backend)

    st.subheader("LSM")
    with st.spinner("Loading LSM projects…"):
        try:
            lsm_names = lsm_svc.list_project_names()
        except Exception as e:
            st.error(f"Failed to connect to state service: {e}")
            lsm_names = []
    _render_project_table("lsm", lsm_names, lsm_svc.peek_project_by_parts)

    st.divider()

    st.subheader("OCT")
    with st.spinner("Loading OCT projects…"):
        try:
            oct_names = oct_svc.list_project_names()
        except Exception as e:
            st.error(f"Failed to connect to state service: {e}")
            oct_names = []
    _render_project_table("oct", oct_names, oct_svc.peek_project_by_parts)
    st.divider()
    _render_add_project_form(backend)

main()
