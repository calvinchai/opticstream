"""RQ job entrypoints: run Prefect deployments for LSM strips and OCT batches."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from prefect.deployments import run_deployment
from pydantic import BaseModel, Field
from redis import Redis
from rq import Queue

from opticstream.state.lsm_project_state import LSMStripId
from opticstream.state.oct_project_state import OCTBatchId

logger = logging.getLogger(__name__)

JOB_KIND_LSM_STRIP = "lsm_strip"
JOB_KIND_OCT_BATCH = "oct_batch"

ENV_REDIS_URL = "OPTICNODE_RQ_REDIS_URL"
ENV_BACKLOG_QUEUE = "OPTICNODE_RQ_BACKLOG_QUEUE"
ENV_ALLOWED_WINDOW_MINUTES = "OPTICNODE_RQ_ALLOWED_WINDOW_MINUTES"
ENV_PREFECT_DEPLOYMENT = "OPTICNODE_RQ_PREFECT_DEPLOYMENT"
ENV_PREFECT_DEPLOYMENT_LSM = "OPTICNODE_RQ_PREFECT_DEPLOYMENT_LSM"
ENV_PREFECT_DEPLOYMENT_OCT = "OPTICNODE_RQ_PREFECT_DEPLOYMENT_OCT"

_PROCESS_BACKLOG = "opticnode.modules.worker.process_backlog"


class StripTask(BaseModel):
    job_kind: Literal["lsm_strip"] = JOB_KIND_LSM_STRIP
    lsm_strip_id: LSMStripId
    timestamp: datetime = Field(default_factory=datetime.now)
    strip_path: str
    force_rerun: bool = False


class OctBatchTask(BaseModel):
    job_kind: Literal["oct_batch"] = JOB_KIND_OCT_BATCH
    batch_id: OCTBatchId
    file_list: list[str]
    timestamp: datetime = Field(default_factory=datetime.now)
    force_rerun: bool = False


def _redis_url() -> str:
    return (
        os.environ.get(ENV_REDIS_URL)
        or os.environ.get("REDIS_URL")
        or "redis://127.0.0.1:6379/0"
    )


def _redis_conn() -> Redis:
    return Redis.from_url(_redis_url(), decode_responses=False)


def _backlog_queue_name() -> str:
    return os.environ.get(ENV_BACKLOG_QUEUE, "").strip()


def _allowed_window() -> timedelta:
    raw = os.environ.get(ENV_ALLOWED_WINDOW_MINUTES, "10").strip()
    if raw == "":
        return timedelta(minutes=10.0)
    mins = float(raw)
    if mins <= 0:
        return timedelta(0)
    return timedelta(minutes=mins)


def _deployment_name_for(kind: str) -> str:
    if kind == JOB_KIND_LSM_STRIP:
        specific = os.environ.get(ENV_PREFECT_DEPLOYMENT_LSM, "").strip()
    else:
        specific = os.environ.get(ENV_PREFECT_DEPLOYMENT_OCT, "").strip()
    if specific:
        return specific
    return os.environ.get(ENV_PREFECT_DEPLOYMENT, "").strip()


def _send_to_backlog(payload: dict[str, Any]) -> None:
    bq = _backlog_queue_name()
    if not bq:
        logger.warning(
            "Job is outside allowed time window but %s is unset; skipping",
            ENV_BACKLOG_QUEUE,
        )
        return
    conn = _redis_conn()
    Queue(bq, connection=conn).enqueue(_PROCESS_BACKLOG, payload)
    logger.info("Re-queued job to backlog queue %r", bq)


def _run_lsm_deployment(task: StripTask) -> None:
    from opticstream.config.lsm_scan_config import LSMScanConfigModel, get_lsm_scan_config

    name = _deployment_name_for(JOB_KIND_LSM_STRIP)
    if not name:
        raise RuntimeError(
            f"Set {ENV_PREFECT_DEPLOYMENT} or {ENV_PREFECT_DEPLOYMENT_LSM} for LSM jobs"
        )
    project_name = task.lsm_strip_id.project_name
    block = get_lsm_scan_config(project_name)
    scan_config = LSMScanConfigModel.model_validate(block.model_dump())
    run_deployment(
        name=name,
        parameters={
            "strip_ident": task.lsm_strip_id.model_dump(),
            "strip_path": task.strip_path,
            "scan_config": scan_config.model_dump(mode="json"),
            "force_rerun": task.force_rerun,
        },
    )


def _run_oct_deployment(task: OctBatchTask) -> None:
    from opticstream.config.psoct_scan_config import PSOCTScanConfigModel, get_psoct_scan_config

    name = _deployment_name_for(JOB_KIND_OCT_BATCH)
    if not name:
        raise RuntimeError(
            f"Set {ENV_PREFECT_DEPLOYMENT} or {ENV_PREFECT_DEPLOYMENT_OCT} for OCT jobs"
        )
    project_name = task.batch_id.project_name
    block = get_psoct_scan_config(project_name)
    config = PSOCTScanConfigModel.model_validate(block.model_dump())
    run_deployment(
        name=name,
        parameters={
            "batch_id": task.batch_id.model_dump(),
            "config": config.model_dump(mode="json"),
            "file_list": [str(Path(p)) for p in task.file_list],
            "force_rerun": task.force_rerun,
        },
    )


def _coerce_timestamp(val: object) -> datetime:
    if isinstance(val, datetime):
        return val
    s = str(val).replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def process(payload: dict[str, Any]) -> None:
    """Main queue handler: defer stale jobs to backlog when configured."""
    kind = payload.get("job_kind", JOB_KIND_LSM_STRIP)
    window = _allowed_window()
    if window > timedelta(0):
        ts_raw = payload.get("timestamp")
        if ts_raw is not None:
            ts = _coerce_timestamp(ts_raw)
            if ts.tzinfo is not None:
                ts = ts.astimezone().replace(tzinfo=None)
            if ts < datetime.now() - window:
                logger.info("Job timestamp outside allowed window; sending to backlog")
                _send_to_backlog(payload)
                return

    if kind == JOB_KIND_LSM_STRIP:
        task = StripTask.model_validate(payload)
        _run_lsm_deployment(task)
    elif kind == JOB_KIND_OCT_BATCH:
        task = OctBatchTask.model_validate(payload)
        _run_oct_deployment(task)
    else:
        raise ValueError(f"Unknown job_kind: {kind!r}")


def process_backlog(payload: dict[str, Any]) -> None:
    """Backlog queue handler: always run deployment (no time-window check)."""
    kind = payload.get("job_kind", JOB_KIND_LSM_STRIP)
    if kind == JOB_KIND_LSM_STRIP:
        task = StripTask.model_validate(payload)
        _run_lsm_deployment(task)
    elif kind == JOB_KIND_OCT_BATCH:
        task = OctBatchTask.model_validate(payload)
        _run_oct_deployment(task)
    else:
        raise ValueError(f"Unknown job_kind: {kind!r}")
