"""Local copy/archive work queue with Redis-backed job status."""

from __future__ import annotations

import logging
import platform
import queue
import re
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings

logger = logging.getLogger(__name__)

_PROGRESS_RE = re.compile(r"(\d{1,3})\s*%")

_JOB_PENDING = "PENDING"
_JOB_ACTIVE = "ACTIVE"
_JOB_COMPLETED = "COMPLETED"
_JOB_FAILED = "FAILED"


@dataclass
class CopyJob:
    job_id: str
    src_path: str
    dst_path: str
    move_mode: bool


class WorkQueue:
    """Single-worker queue; Robocopy on Windows, shutil on other platforms."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._q: queue.Queue[CopyJob] = queue.Queue()
        self._paused = threading.Event()
        self._paused.set()
        self._stop = threading.Event()
        self._redis: Any = None
        self._thread = threading.Thread(target=self._worker, name="work-queue", daemon=True)
        self._thread.start()
        self._connect_redis()

    def _connect_redis(self) -> None:
        try:
            from redis import Redis

            self._redis = Redis.from_url(self._settings.redis_url, decode_responses=True)
        except Exception:
            logger.exception("WorkQueue Redis init failed")
            self._redis = None

    def _job_key(self, job_id: str) -> str:
        return f"opticnode:{self._settings.node_id}:jobs:{job_id}"

    def _jobs_index_key(self) -> str:
        return f"opticnode:{self._settings.node_id}:job_ids"

    def _persist(self, job_id: str, mapping: dict[str, str]) -> None:
        if self._redis is None:
            try:
                self._connect_redis()
            except Exception:
                return
        try:
            self._redis.hset(self._job_key(job_id), mapping=mapping)
        except Exception:
            logger.exception("WorkQueue Redis persist failed")
            self._redis = None

    def enqueue(self, src_path: str, dst_path: str, *, move_mode: bool) -> str:
        job_id = str(uuid.uuid4())
        job = CopyJob(job_id=job_id, src_path=src_path, dst_path=dst_path, move_mode=move_mode)
        now = str(time.time())
        self._persist(
            job_id,
            {
                "status": _JOB_PENDING,
                "progress": "0",
                "src_path": src_path,
                "dst_path": dst_path,
                "move_mode": str(move_mode).lower(),
                "message": "queued",
                "exit_code": "",
                "updated_at": now,
            },
        )
        try:
            if self._redis is not None:
                self._redis.sadd(self._jobs_index_key(), job_id)
        except Exception:
            logger.exception("WorkQueue job index update failed")
        self._q.put(job)
        return job_id

    def pause(self) -> None:
        self._paused.clear()

    def resume(self) -> None:
        self._paused.set()

    def stop(self) -> None:
        self._stop.set()
        self._paused.set()

    def _worker(self) -> None:
        while not self._stop.is_set():
            while not self._paused.is_set() and not self._stop.is_set():
                time.sleep(0.2)
            if self._stop.is_set():
                break
            try:
                job = self._q.get(timeout=0.5)
            except queue.Empty:
                continue
            self._run_job(job)

    def _run_job(self, job: CopyJob) -> None:
        jid = job.job_id
        src = Path(job.src_path).expanduser()
        dst = Path(job.dst_path).expanduser()
        if not src.exists():
            self._persist(
                jid,
                {
                    "status": _JOB_FAILED,
                    "progress": "0",
                    "src_path": str(src),
                    "dst_path": str(dst),
                    "move_mode": str(job.move_mode).lower(),
                    "message": "source does not exist",
                    "exit_code": "-1",
                    "updated_at": str(time.time()),
                },
            )
            return

        self._persist(
            jid,
            {
                "status": _JOB_ACTIVE,
                "progress": "0",
                "src_path": str(src),
                "dst_path": str(dst),
                "move_mode": str(job.move_mode).lower(),
                "message": "running",
                "exit_code": "",
                "updated_at": str(time.time()),
            },
        )

        try:
            if platform.system() == "Windows":
                code, msg = self._robocopy_run(src, dst, jid)
            else:
                code, msg = self._shutil_run(src, dst, jid)
        except Exception as exc:
            code = -1
            msg = str(exc)
            logger.exception("Copy job failed")

        status = _JOB_COMPLETED if code == 0 else _JOB_FAILED
        self._persist(
            jid,
            {
                "status": status,
                "progress": "100" if status == _JOB_COMPLETED else "0",
                "src_path": str(src),
                "dst_path": str(dst),
                "move_mode": str(job.move_mode).lower(),
                "message": msg[:4000],
                "exit_code": str(code),
                "updated_at": str(time.time()),
            },
        )

        if job.move_mode and status == _JOB_COMPLETED and src.exists():
            try:
                if src.is_dir():
                    shutil.rmtree(src, ignore_errors=True)
                else:
                    src.unlink(missing_ok=True)
            except Exception:
                logger.exception("Move cleanup failed for %s", src)

    def _update_progress(self, job_id: str, pct: int, message: str) -> None:
        self._persist(
            job_id,
            {
                "status": _JOB_ACTIVE,
                "progress": str(max(0, min(100, pct))),
                "message": message[:2000],
                "updated_at": str(time.time()),
            },
        )

    def _robocopy_run(self, src: Path, dst: Path, job_id: str) -> tuple[int, str]:
        cmd = [
            "robocopy",
            str(src),
            str(dst),
            "/E",
            "/MT:8",
            "/R:2",
            "/W:2",
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        lines: list[str] = []
        last_pct = 0
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)
            for m in _PROGRESS_RE.finditer(line):
                last_pct = max(last_pct, int(m.group(1)))
                self._update_progress(job_id, last_pct, line.strip()[:500])
        proc.wait()
        rc = int(proc.returncode or 0)
        if rc < 8:
            return 0, "".join(lines[-40:])
        return rc, "".join(lines[-80:])

    def _shutil_run(self, src: Path, dst: Path, job_id: str) -> tuple[int, str]:
        try:
            if src.is_dir():
                dst.mkdir(parents=True, exist_ok=True)
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
            self._update_progress(job_id, 100, "copy finished")
            return 0, "ok"
        except Exception as exc:
            return -1, str(exc)
