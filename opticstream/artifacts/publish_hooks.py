"""Thin publish hooks for Prefect flow completion callbacks."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from prefect.logging.loggers import flow_run_logger

from opticstream.artifacts.lsm import publish_lsm_project_artifact, publish_lsm_slice_artifact
from opticstream.artifacts.oct import publish_oct_mosaic_artifact, publish_oct_project_artifact
from opticstream.state.lsm_project_state import LSMProjectId, LSMSliceId
from opticstream.state.oct_project_state import OCTMosaicId, OCTProjectId
from opticstream.utils.flow_run_name_parse import (
    missing_required_fields,
    parse_flow_run_name_fields,
)


def _run_artifact_publish_hook(
    *,
    flow: Any,
    flow_run: Any,
    required_fields: tuple[str, ...],
    ident_factory: Callable[[dict[str, str | int]], Any],
    publisher: Callable[..., str],
    hook_name: str,
) -> None:
    logger = flow_run_logger(flow_run, flow)
    parsed = parse_flow_run_name_fields(flow_run.name or "")
    missing = missing_required_fields(parsed, required_fields)
    if missing:
        logger.info(
            "%s skipped for run %s; missing fields: %s",
            hook_name,
            flow_run.name,
            ", ".join(missing),
        )
        return

    ident = ident_factory(parsed)
    artifact_key = publisher(ident, logger=logger)
    if artifact_key:
        logger.info("%s published artifact %s", hook_name, artifact_key)
    else:
        logger.info("%s skipped publish for %s", hook_name, flow_run.name)


def publish_oct_mosaic_hook(flow: Any, flow_run: Any, state: Any) -> None:
    _run_artifact_publish_hook(
        flow=flow,
        flow_run=flow_run,
        required_fields=("project_name", "slice_id", "mosaic_id"),
        ident_factory=lambda p: OCTMosaicId(
            project_name=str(p["project_name"]),
            slice_id=int(p["slice_id"]),
            mosaic_id=int(p["mosaic_id"]),
        ),
        publisher=publish_oct_mosaic_artifact,
        hook_name="publish_oct_mosaic_hook",
    )


def publish_oct_project_hook(flow: Any, flow_run: Any, state: Any) -> None:
    _run_artifact_publish_hook(
        flow=flow,
        flow_run=flow_run,
        required_fields=("project_name",),
        ident_factory=lambda p: OCTProjectId(project_name=str(p["project_name"])),
        publisher=publish_oct_project_artifact,
        hook_name="publish_oct_project_hook",
    )


def publish_lsm_slice_hook(flow: Any, flow_run: Any, state: Any) -> None:
    _run_artifact_publish_hook(
        flow=flow,
        flow_run=flow_run,
        required_fields=("project_name", "slice_id"),
        ident_factory=lambda p: LSMSliceId(
            project_name=str(p["project_name"]),
            slice_id=int(p["slice_id"]),
        ),
        publisher=publish_lsm_slice_artifact,
        hook_name="publish_lsm_slice_hook",
    )


def publish_lsm_project_hook(flow: Any, flow_run: Any, state: Any) -> None:
    _run_artifact_publish_hook(
        flow=flow,
        flow_run=flow_run,
        required_fields=("project_name",),
        ident_factory=lambda p: LSMProjectId(project_name=str(p["project_name"])),
        publisher=publish_lsm_project_artifact,
        hook_name="publish_lsm_project_hook",
    )
