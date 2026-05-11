"""LSM-specific sharded state service."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager, contextmanager
from typing import Iterator

from opticstream.state.project_state_redis import RedisStateBackend
from opticstream.state.lsm_models import (
    LSM_PROJECT_TYPE,
    STATE_REDIS_BLOCK_NAME,
    LSMProjectId,
    LSMSliceId,
    LSMChannelId,
    LSMStripId,
    LSMStripState,
    LSMStripStateView,
    LSMChannelState,
    LSMChannelStateView,
    LSMSliceState,
    LSMSliceStateView,
    LSMProjectState,
    LSMProjectStateView,
)


_PREFIX = f"{LSM_PROJECT_TYPE}:project"


class LSMProjectStateService:
    """LSM-specific state service with granular per-resource locking."""

    def __init__(self, backend: RedisStateBackend | None = None) -> None:
        self._backend = backend or RedisStateBackend(STATE_REDIS_BLOCK_NAME)

    # -- discovery ----------------------------------------------------------

    def list_project_names(self) -> list[str]:
        """Return sorted names of all LSM projects with a meta key in Redis."""
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

    def _channel_key(self, name: str, sid: int, cid: int) -> str:
        return f"{_PREFIX}:{name}:channel:{sid}:{cid}:meta"

    def _strips_hash(self, name: str) -> str:
        return f"{_PREFIX}:{name}:strips"

    @staticmethod
    def _strip_field(sid: int, cid: int, stid: int) -> str:
        return f"{sid}:{cid}:{stid}"

    def _lock(self, name: str, *parts: str | int) -> str:
        suffix = ":".join(str(p) for p in parts) if parts else ""
        base = f"lock:{_PREFIX}:{name}"
        return f"{base}:{suffix}" if suffix else base

    # -- reconstruction helpers ---------------------------------------------

    def _reconstruct_project(self, project_name: str) -> LSMProjectState:
        b = self._backend
        client = b.client
        prefix = f"{_PREFIX}:{project_name}"

        project = b.load_key(f"{prefix}:meta", LSMProjectState) or LSMProjectState()

        for key in client.scan_iter(f"{prefix}:slice:*:meta"):
            sid = int(key.removeprefix(f"{prefix}:slice:").removesuffix(":meta"))
            sl = b.load_key(key, LSMSliceState)
            if sl:
                project.slices[sid] = sl

        for key in client.scan_iter(f"{prefix}:channel:*:meta"):
            rest = key.removeprefix(f"{prefix}:channel:").removesuffix(":meta")
            sid_s, cid_s = rest.split(":")
            ch = b.load_key(key, LSMChannelState)
            if ch:
                sl = project.get_or_create_slice(int(sid_s))
                sl.channels[int(cid_s)] = ch

        for field, strip in b.scan_hash_fields(f"{prefix}:strips", LSMStripState).items():
            sid_s, cid_s, stid_s = field.split(":")
            ch = project.get_or_create_channel(int(sid_s), int(cid_s))
            ch.strips[int(stid_s)] = strip

        return project

    def _save_full_project(self, project_name: str, state: LSMProjectState) -> None:
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
                json.dumps(sl.model_dump(mode="json", exclude={"channels"})),
            )
            for cid, ch in sl.channels.items():
                pipe.set(
                    f"{prefix}:channel:{sid}:{cid}:meta",
                    json.dumps(ch.model_dump(mode="json", exclude={"strips"})),
                )
                for stid, strip in ch.strips.items():
                    pipe.hset(
                        f"{prefix}:strips",
                        f"{sid}:{cid}:{stid}",
                        json.dumps(strip.model_dump(mode="json")),
                    )

        pipe.execute()

    def _load_channel_view(
        self, project_name: str, sid: int, cid: int,
    ) -> LSMChannelStateView | None:
        ch = self._backend.load_key(self._channel_key(project_name, sid, cid), LSMChannelState)
        if ch is None:
            return None
        for field, strip in self._backend.scan_hash_fields(
            self._strips_hash(project_name), LSMStripState, match=f"{sid}:{cid}:*",
        ).items():
            ch.strips[int(field.split(":")[2])] = strip
        return ch.to_view()

    def _load_slice_view(
        self, project_name: str, sid: int,
    ) -> LSMSliceStateView | None:
        sl = self._backend.load_key(self._slice_key(project_name, sid), LSMSliceState)
        if sl is None:
            return None
        client = self._backend.client
        prefix = f"{_PREFIX}:{project_name}"
        for key in client.scan_iter(f"{prefix}:channel:{sid}:*:meta"):
            cid = int(key.removeprefix(f"{prefix}:channel:{sid}:").removesuffix(":meta"))
            ch = self._backend.load_key(key, LSMChannelState)
            if ch:
                sl.channels[cid] = ch
        for field, strip in self._backend.scan_hash_fields(
            self._strips_hash(project_name), LSMStripState, match=f"{sid}:*",
        ).items():
            parts = field.split(":")
            ch = sl.get_or_create_channel(int(parts[1]))
            ch.strips[int(parts[2])] = strip
        return sl.to_view()

    # ------------------------------------------------------------------
    # Mutable scoped access (open_*)
    # ------------------------------------------------------------------

    def open_project(
        self,
        project_ident: LSMProjectId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMProjectState]:
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
    ) -> Iterator[LSMProjectState]:
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
        slice_ident: LSMSliceId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMSliceState]:
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
    ) -> AbstractContextManager[LSMSliceState]:
        return self._backend.open_key(
            key=self._slice_key(project_name, slice_id),
            model_cls=LSMSliceState,
            lock_key=self._lock(project_name, "slice", slice_id),
            default_factory=lambda: LSMSliceState(slice_id=slice_id),
            exclude_on_save={"channels"},
            timeout_seconds=timeout_seconds,
        )

    def open_channel(
        self,
        channel_ident: LSMChannelId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMChannelState]:
        return self.open_channel_by_parts(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
            timeout_seconds=timeout_seconds,
        )

    def open_channel_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMChannelState]:
        return self._backend.open_key(
            key=self._channel_key(project_name, slice_id, channel_id),
            model_cls=LSMChannelState,
            lock_key=self._lock(project_name, "channel", slice_id, channel_id),
            default_factory=lambda: LSMChannelState(slice_id=slice_id, channel_id=channel_id),
            exclude_on_save={"strips"},
            timeout_seconds=timeout_seconds,
        )

    def open_strip(
        self,
        strip_ident: LSMStripId,
        *,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMStripState]:
        return self.open_strip_by_parts(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
            timeout_seconds=timeout_seconds,
        )

    def open_strip_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
        timeout_seconds: float | None = None,
    ) -> AbstractContextManager[LSMStripState]:
        field = self._strip_field(slice_id, channel_id, strip_id)
        return self._backend.open_hash_field(
            hash_key=self._strips_hash(project_name),
            field=field,
            model_cls=LSMStripState,
            lock_key=self._lock(project_name, "strip", field),
            default_factory=lambda: LSMStripState(
                slice_id=slice_id, channel_id=channel_id, strip_id=strip_id,
            ),
            timeout_seconds=timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Readonly access (read_* = peek_* since each Redis op is atomic)
    # ------------------------------------------------------------------

    def read_project(
        self,
        project_ident: LSMProjectId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMProjectStateView:
        return self.peek_project_by_parts(project_name=project_ident.project_name)

    def read_project_by_parts(
        self,
        project_name: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMProjectStateView:
        return self.peek_project_by_parts(project_name)

    def read_slice(
        self,
        slice_ident: LSMSliceId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMSliceStateView | None:
        return self.peek_slice_by_parts(
            project_name=slice_ident.project_name, slice_id=slice_ident.slice_id,
        )

    def read_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        timeout_seconds: float | None = None,
    ) -> LSMSliceStateView | None:
        return self.peek_slice_by_parts(project_name, slice_id=slice_id)

    def read_channel(
        self,
        channel_ident: LSMChannelId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMChannelStateView | None:
        return self.peek_channel_by_parts(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
        )

    def read_channel_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
        timeout_seconds: float | None = None,
    ) -> LSMChannelStateView | None:
        return self.peek_channel_by_parts(
            project_name, slice_id=slice_id, channel_id=channel_id,
        )

    def read_strip(
        self,
        strip_ident: LSMStripId,
        *,
        timeout_seconds: float | None = None,
    ) -> LSMStripStateView | None:
        return self.peek_strip_by_parts(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
        )

    def read_strip_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
        timeout_seconds: float | None = None,
    ) -> LSMStripStateView | None:
        return self.peek_strip_by_parts(
            project_name, slice_id=slice_id, strip_id=strip_id, channel_id=channel_id,
        )

    # ------------------------------------------------------------------
    # Unlocked readonly access (peek_*)
    # ------------------------------------------------------------------

    def peek_project(self, project_ident: LSMProjectId) -> LSMProjectStateView:
        return self.peek_project_by_parts(project_name=project_ident.project_name)

    def peek_project_by_parts(self, project_name: str) -> LSMProjectStateView:
        return self._reconstruct_project(project_name).to_view()

    def peek_slice(
        self,
        slice_ident: LSMSliceId,
    ) -> LSMSliceStateView | None:
        return self.peek_slice_by_parts(
            project_name=slice_ident.project_name,
            slice_id=slice_ident.slice_id,
        )

    def peek_slice_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
    ) -> LSMSliceStateView | None:
        return self._load_slice_view(project_name, slice_id)

    def peek_channel(
        self,
        channel_ident: LSMChannelId,
    ) -> LSMChannelStateView | None:
        return self.peek_channel_by_parts(
            project_name=channel_ident.project_name,
            slice_id=channel_ident.slice_id,
            channel_id=channel_ident.channel_id,
        )

    def peek_channel_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        channel_id: int,
    ) -> LSMChannelStateView | None:
        return self._load_channel_view(project_name, slice_id, channel_id)

    def peek_strip(
        self,
        strip_ident: LSMStripId,
    ) -> LSMStripStateView | None:
        return self.peek_strip_by_parts(
            project_name=strip_ident.project_name,
            slice_id=strip_ident.slice_id,
            strip_id=strip_ident.strip_id,
            channel_id=strip_ident.channel_id,
        )

    def peek_strip_by_parts(
        self,
        project_name: str,
        *,
        slice_id: int,
        strip_id: int,
        channel_id: int = 1,
    ) -> LSMStripStateView | None:
        return self._backend.load_hash_field(
            self._strips_hash(project_name),
            self._strip_field(slice_id, channel_id, strip_id),
            LSMStripStateView,
        )


LSM_STATE_SERVICE = LSMProjectStateService()
