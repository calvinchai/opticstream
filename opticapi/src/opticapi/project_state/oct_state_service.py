"""OCT-specific sharded state service."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager, contextmanager
from typing import Iterator

from opticapi.project_state.redis_backend import RedisStateBackend
from opticapi.project_state.oct_models import (
    OCT_PROJECT_TYPE,
    OCTProjectId,
    OCTSliceId,
    OCTMosaicId,
    OCTBatchId,
    OCTBatchState,
    OCTBatchStateView,
    OCTMosaicState,
    OCTMosaicStateView,
    OCTSliceState,
    OCTSliceStateView,
    OCTProjectState,
    OCTProjectStateView,
)


_PREFIX = f"{OCT_PROJECT_TYPE}:project"


class OCTProjectStateService:
    """OCT-specific state service with granular per-resource locking."""

    def __init__(self, backend: RedisStateBackend) -> None:
        self._backend = backend

    # -- discovery ----------------------------------------------------------

    def list_project_names(self) -> list[str]:
        """Return sorted names of all OCT projects with a meta key in Redis."""
        client = self._backend.client
        suffix = ":meta"
        prefix = f"{_PREFIX}:"
        names: set[str] = set()
        for key in client.scan_iter(f"{prefix}*{suffix}"):
            rest = key.removeprefix(prefix)
            if rest.count(":") == 1 and rest.endswith(suffix):
                names.add(rest.removesuffix(suffix))
        return sorted(names)

    # -- key helpers --------------------------------------------------------

    def _project_key(self, name: str) -> str:
        return f"{_PREFIX}:{name}:meta"

    def _slice_key(self, name: str, sid: int) -> str:
        return f"{_PREFIX}:{name}:slice:{sid}:meta"

    def _mosaic_key(self, name: str, sid: int, mid: int) -> str:
        return f"{_PREFIX}:{name}:mosaic:{sid}:{mid}:meta"

    def _batches_hash(self, name: str, sid: int, mid: int) -> str:
        return f"{_PREFIX}:{name}:batches:{sid}:{mid}"

    @staticmethod
    def _batch_field(bid: int) -> str:
        return str(bid)

    def _lock(self, name: str, *parts: str | int) -> str:
        suffix = ":".join(str(p) for p in parts) if parts else ""
        base = f"lock:{_PREFIX}:{name}"
        return f"{base}:{suffix}" if suffix else base

    # -- reconstruction helpers ---------------------------------------------

    def _reconstruct_project(self, project_name: str) -> OCTProjectState:
        b = self._backend
        client = b.client
        prefix = f"{_PREFIX}:{project_name}"

        project = b.load_key(f"{prefix}:meta", OCTProjectState) or OCTProjectState()

        for key in client.scan_iter(f"{prefix}:slice:*:meta"):
            sid = int(key.removeprefix(f"{prefix}:slice:").removesuffix(":meta"))
            sl = b.load_key(key, OCTSliceState)
            if sl:
                project.slices[sid] = sl

        for key in client.scan_iter(f"{prefix}:mosaic:*:meta"):
            rest = key.removeprefix(f"{prefix}:mosaic:").removesuffix(":meta")
            sid_s, mid_s = rest.split(":")
            mo = b.load_key(key, OCTMosaicState)
            if mo:
                sl = project.get_or_create_slice(int(sid_s))
                sl.mosaics[int(mid_s)] = mo

        for key in client.scan_iter(f"{prefix}:batches:*"):
            rest = key.removeprefix(f"{prefix}:batches:")
            sid_s, mid_s = rest.split(":")
            sid, mid = int(sid_s), int(mid_s)
            mo = project.get_or_create_mosaic(sid, mid)
            for field, batch in b.load_all_hash_fields(key, OCTBatchState).items():
                mo.batches[int(field)] = batch

        return project

    def _save_full_project(self, project_name: str, state: OCTProjectState) -> None:
        client = self._backend.client
        prefix = f"{_PREFIX}:{project_name}"

        old_keys = list(client.scan_iter(f"{prefix}:*"))
        if old_keys:
            client.delete(*old_keys)

        pipe = client.pipeline()
        pipe.set(f"{prefix}:meta", json.dumps(state.model_dump(mode="json", exclude={"slices"})))

        for sid, sl in state.slices.items():
            pipe.set(
                f"{prefix}:slice:{sid}:meta",
                json.dumps(sl.model_dump(mode="json", exclude={"mosaics"})),
            )
            for mid, mo in sl.mosaics.items():
                pipe.set(
                    f"{prefix}:mosaic:{sid}:{mid}:meta",
                    json.dumps(mo.model_dump(mode="json", exclude={"batches"})),
                )
                for bid, batch in mo.batches.items():
                    pipe.hset(
                        f"{prefix}:batches:{sid}:{mid}",
                        str(bid),
                        json.dumps(batch.model_dump(mode="json")),
                    )

        pipe.execute()

    def _load_mosaic_view(
        self, project_name: str, sid: int, mid: int,
    ) -> OCTMosaicStateView | None:
        mo = self._backend.load_key(self._mosaic_key(project_name, sid, mid), OCTMosaicState)
        if mo is None:
            return None
        for field, batch in self._backend.load_all_hash_fields(
            self._batches_hash(project_name, sid, mid), OCTBatchState,
        ).items():
            mo.batches[int(field)] = batch
        return mo.to_view()

    def _load_slice_view(
        self, project_name: str, sid: int,
    ) -> OCTSliceStateView | None:
        sl = self._backend.load_key(self._slice_key(project_name, sid), OCTSliceState)
        if sl is None:
            return None
        b = self._backend
        client = b.client
        prefix = f"{_PREFIX}:{project_name}"
        for key in client.scan_iter(f"{prefix}:mosaic:{sid}:*:meta"):
            mid = int(key.removeprefix(f"{prefix}:mosaic:{sid}:").removesuffix(":meta"))
            mo = b.load_key(key, OCTMosaicState)
            if mo:
                sl.mosaics[mid] = mo
        for key in client.scan_iter(f"{prefix}:batches:{sid}:*"):
            mid = int(key.removeprefix(f"{prefix}:batches:{sid}:"))
            mo = sl.get_or_create_mosaic(mid)
            for field, batch in b.load_all_hash_fields(key, OCTBatchState).items():
                mo.batches[int(field)] = batch
        return sl.to_view()

    # ------------------------------------------------------------------
    # Mutable scoped access (open_*)
    # ------------------------------------------------------------------

    def open_project(
        self,
        project_ident: OCTProjectId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTProjectState]:
        return self.open_project_by_parts(
            project_name=project_ident.project_name,
            timeout_seconds=timeout_seconds,
        )

    @contextmanager
    def open_project_by_parts(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> Iterator[OCTProjectState]:
        lock = self._backend.client.lock(
            self._lock(project_name),
            timeout=timeout_seconds,
            blocking_timeout=timeout_seconds,
        )
        if not lock.acquire():
            raise TimeoutError(f"Could not acquire project lock for {project_name!r}")
        try:
            state = self._reconstruct_project(project_name)
            yield state
            self._save_full_project(project_name, state)
        finally:
            lock.release()

    def open_slice(
        self,
        slice_ident: OCTSliceId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTSliceState]:
        return self.open_slice_by_parts(
            project_name=slice_ident.project_name,
            slice_id=slice_ident.slice_id,
            timeout_seconds=timeout_seconds,
        )

    def open_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTSliceState]:
        return self._backend.open_key(
            key=self._slice_key(project_name, slice_id),
            model_cls=OCTSliceState,
            lock_key=self._lock(project_name, "slice", slice_id),
            default_factory=lambda: OCTSliceState(slice_id=slice_id),
            exclude_on_save={"mosaics"},
            timeout_seconds=timeout_seconds,
        )

    def open_mosaic(
        self,
        mosaic_ident: OCTMosaicId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTMosaicState]:
        return self.open_mosaic_by_parts(
            project_name=mosaic_ident.project_name,
            slice_id=mosaic_ident.slice_id,
            mosaic_id=mosaic_ident.mosaic_id,
            timeout_seconds=timeout_seconds,
        )

    def open_mosaic_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        mosaic_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTMosaicState]:
        return self._backend.open_key(
            key=self._mosaic_key(project_name, slice_id, mosaic_id),
            model_cls=OCTMosaicState,
            lock_key=self._lock(project_name, "mosaic", slice_id, mosaic_id),
            default_factory=lambda: OCTMosaicState(
                slice_id=slice_id, mosaic_id=mosaic_id,
            ),
            exclude_on_save={"batches"},
            timeout_seconds=timeout_seconds,
        )

    def open_batch(
        self,
        batch_ident: OCTBatchId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTBatchState]:
        return self.open_batch_by_parts(
            project_name=batch_ident.project_name,
            slice_id=batch_ident.slice_id,
            mosaic_id=batch_ident.mosaic_id,
            batch_id=batch_ident.batch_id,
            timeout_seconds=timeout_seconds,
        )

    def open_batch_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[OCTBatchState]:
        field = self._batch_field(batch_id)
        return self._backend.open_hash_field(
            hash_key=self._batches_hash(project_name, slice_id, mosaic_id),
            field=field,
            model_cls=OCTBatchState,
            lock_key=self._lock(project_name, "batch", slice_id, mosaic_id, batch_id),
            default_factory=lambda: OCTBatchState(
                slice_id=slice_id, mosaic_id=mosaic_id, batch_id=batch_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Readonly access (read_* = peek_* since each Redis op is atomic)
    # ------------------------------------------------------------------

    def read_project(
        self,
        project_ident: OCTProjectId,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTProjectStateView:
        return self.peek_project_by_parts(project_name=project_ident.project_name)

    def read_project_by_parts(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTProjectStateView:
        return self.peek_project_by_parts(project_name)

    def read_slice(
        self,
        slice_ident: OCTSliceId,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTSliceStateView | None:
        return self.peek_slice_by_parts(
            project_name=slice_ident.project_name, slice_id=slice_ident.slice_id,
        )

    def read_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTSliceStateView | None:
        return self.peek_slice_by_parts(project_name, slice_id=slice_id)

    def read_mosaic(
        self,
        mosaic_ident: OCTMosaicId,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTMosaicStateView | None:
        return self.peek_mosaic_by_parts(
            project_name=mosaic_ident.project_name,
            slice_id=mosaic_ident.slice_id,
            mosaic_id=mosaic_ident.mosaic_id,
        )

    def read_mosaic_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        mosaic_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTMosaicStateView | None:
        return self.peek_mosaic_by_parts(
            project_name, slice_id=slice_id, mosaic_id=mosaic_id,
        )

    def read_batch(
        self,
        batch_ident: OCTBatchId,
        *,
        timeout_seconds: float | None = None,
    ) -> OCTBatchStateView | None:
        return self.peek_batch_by_parts(
            project_name=batch_ident.project_name,
            slice_id=batch_ident.slice_id,
            mosaic_id=batch_ident.mosaic_id,
            batch_id=batch_ident.batch_id,
        )

    def read_batch_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        mosaic_id: int,
        batch_id: int,
        timeout_seconds: float | None = None,
    ) -> OCTBatchStateView | None:
        return self.peek_batch_by_parts(
            project_name, slice_id=slice_id, mosaic_id=mosaic_id, batch_id=batch_id,
        )

    # ------------------------------------------------------------------
    # Unlocked readonly access (peek_*)
    # ------------------------------------------------------------------

    def peek_project(self, project_ident: OCTProjectId) -> OCTProjectStateView:
        return self.peek_project_by_parts(project_name=project_ident.project_name)

    def peek_project_by_parts(self, project_name: str) -> OCTProjectStateView:
        return self._reconstruct_project(project_name).to_view()

    def peek_slice(
        self,
        slice_ident: OCTSliceId,
    ) -> OCTSliceStateView | None:
        return self.peek_slice_by_parts(
            project_name=slice_ident.project_name,
            slice_id=slice_ident.slice_id,
        )

    def peek_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
    ) -> OCTSliceStateView | None:
        return self._load_slice_view(project_name, slice_id)

    def peek_mosaic(
        self,
        mosaic_ident: OCTMosaicId,
    ) -> OCTMosaicStateView | None:
        return self.peek_mosaic_by_parts(
            project_name=mosaic_ident.project_name,
            slice_id=mosaic_ident.slice_id,
            mosaic_id=mosaic_ident.mosaic_id,
        )

    def peek_mosaic_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        mosaic_id: int,
    ) -> OCTMosaicStateView | None:
        return self._load_mosaic_view(project_name, slice_id, mosaic_id)

    def peek_batch(
        self,
        batch_ident: OCTBatchId,
    ) -> OCTBatchStateView | None:
        return self.peek_batch_by_parts(
            project_name=batch_ident.project_name,
            slice_id=batch_ident.slice_id,
            mosaic_id=batch_ident.mosaic_id,
            batch_id=batch_ident.batch_id,
        )

    def peek_batch_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        mosaic_id: int,
        batch_id: int,
    ) -> OCTBatchStateView | None:
        return self._backend.load_hash_field(
            self._batches_hash(project_name, slice_id, mosaic_id),
            self._batch_field(batch_id),
            OCTBatchStateView,
        )
