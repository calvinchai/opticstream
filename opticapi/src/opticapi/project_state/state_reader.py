"""Read-only Redis access to sharded LSM/OCT project state (no Prefect)."""

from __future__ import annotations

import json
from typing import Any

from redis import Redis

from opticapi.project_state.lsm_models import (
    LSM_PROJECT_TYPE,
    LSMProjectStateView,
)
from opticapi.project_state.oct_models import (
    OCT_PROJECT_TYPE,
    OCTProjectStateView,
)

_LSM_PREFIX = f"{LSM_PROJECT_TYPE}:project"
_OCT_PREFIX = f"{OCT_PROJECT_TYPE}:project"


def _client(redis_url: str) -> Redis:
    return Redis.from_url(redis_url, decode_responses=True)


class LSMStateReader:
    """List projects and peek full LSM project trees from Redis."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self._client is None:
            self._client = _client(self._redis_url)
        return self._client

    def list_project_names(self) -> list[str]:
        """Sorted names of all LSM projects with a meta key in Redis."""
        suffix = ":meta"
        prefix = f"{_LSM_PREFIX}:"
        names: set[str] = set()
        for key in self.client.scan_iter(f"{prefix}*{suffix}"):
            rest = key.removeprefix(prefix)
            if rest.count(":") == 1 and rest.endswith(suffix):
                names.add(rest.removesuffix(suffix))
        return sorted(names)

    def peek_project_by_parts(self, project_name: str) -> LSMProjectStateView:
        pfx = f"{_LSM_PREFIX}:{project_name}"
        raw_meta = self.client.get(f"{pfx}:meta")
        project_data: dict[str, Any] = json.loads(raw_meta) if raw_meta else {}
        project_data.setdefault("slices", {})

        slice_states: dict[int, dict[str, Any]] = {}

        for key in self.client.scan_iter(f"{pfx}:slice:*:meta"):
            sid = int(key.removeprefix(f"{pfx}:slice:").removesuffix(":meta"))
            row = self.client.get(key)
            if row:
                slice_states[sid] = json.loads(row)
                slice_states[sid].setdefault("channels", {})

        for key in self.client.scan_iter(f"{pfx}:channel:*:meta"):
            rest = key.removeprefix(f"{pfx}:channel:").removesuffix(":meta")
            sid_s, cid_s = rest.split(":")
            sid, cid = int(sid_s), int(cid_s)
            row = self.client.get(key)
            if not row:
                continue
            ch = json.loads(row)
            ch.setdefault("strips", {})
            slice_states.setdefault(sid, {"slice_id": sid, "channels": {}})
            slice_states[sid].setdefault("channels", {})
            slice_states[sid]["channels"][cid] = ch

        for field, raw in (self.client.hgetall(f"{pfx}:strips") or {}).items():
            sid_s, cid_s, stid_s = field.split(":")
            sid, cid, stid = int(sid_s), int(cid_s), int(stid_s)
            strip = json.loads(raw)
            slice_states.setdefault(sid, {"slice_id": sid, "channels": {}})
            slice_states[sid].setdefault("channels", {})
            slice_states[sid]["channels"].setdefault(
                cid,
                {"slice_id": sid, "channel_id": cid, "strips": {}},
            )
            slice_states[sid]["channels"][cid].setdefault("strips", {})
            slice_states[sid]["channels"][cid]["strips"][stid] = strip

        project_data["slices"] = slice_states
        return LSMProjectStateView.model_validate(project_data)


class OCTStateReader:
    """List projects and peek full OCT project trees from Redis."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self._client is None:
            self._client = _client(self._redis_url)
        return self._client

    def list_project_names(self) -> list[str]:
        suffix = ":meta"
        prefix = f"{_OCT_PREFIX}:"
        names: set[str] = set()
        for key in self.client.scan_iter(f"{prefix}*{suffix}"):
            rest = key.removeprefix(prefix)
            if rest.count(":") == 1 and rest.endswith(suffix):
                names.add(rest.removesuffix(suffix))
        return sorted(names)

    def peek_project_by_parts(self, project_name: str) -> OCTProjectStateView:
        pfx = f"{_OCT_PREFIX}:{project_name}"
        raw_meta = self.client.get(f"{pfx}:meta")
        project_data: dict[str, Any] = json.loads(raw_meta) if raw_meta else {}
        project_data.setdefault("slices", {})

        slice_states: dict[int, dict[str, Any]] = {}

        for key in self.client.scan_iter(f"{pfx}:slice:*:meta"):
            sid = int(key.removeprefix(f"{pfx}:slice:").removesuffix(":meta"))
            row = self.client.get(key)
            if row:
                slice_states[sid] = json.loads(row)
                slice_states[sid].setdefault("mosaics", {})

        for key in self.client.scan_iter(f"{pfx}:mosaic:*:meta"):
            rest = key.removeprefix(f"{pfx}:mosaic:").removesuffix(":meta")
            sid_s, mid_s = rest.split(":")
            sid, mid = int(sid_s), int(mid_s)
            row = self.client.get(key)
            if not row:
                continue
            mo = json.loads(row)
            mo.setdefault("batches", {})
            slice_states.setdefault(sid, {"slice_id": sid, "mosaics": {}})
            slice_states[sid].setdefault("mosaics", {})
            slice_states[sid]["mosaics"][mid] = mo

        for field, raw in (self.client.hgetall(f"{pfx}:batches") or {}).items():
            sid_s, mid_s, bid_s = field.split(":")
            sid, mid, bid = int(sid_s), int(mid_s), int(bid_s)
            batch = json.loads(raw)
            slice_states.setdefault(sid, {"slice_id": sid, "mosaics": {}})
            slice_states[sid].setdefault("mosaics", {})
            slice_states[sid]["mosaics"].setdefault(
                mid,
                {"slice_id": sid, "mosaic_id": mid, "batches": {}},
            )
            slice_states[sid]["mosaics"][mid].setdefault("batches", {})
            slice_states[sid]["mosaics"][mid]["batches"][bid] = batch

        project_data["slices"] = slice_states
        return OCTProjectStateView.model_validate(project_data)
