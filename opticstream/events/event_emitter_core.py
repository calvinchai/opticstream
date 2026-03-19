from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, Mapping, Protocol, TypeVar

from prefect.events import emit_event


class ProjectScopedIdent(Protocol):
    project_name: str

    def __str__(self) -> str: ...


TIdent = TypeVar("TIdent", bound=ProjectScopedIdent)


@dataclass(frozen=True)
class ScopedEventAdapter(Generic[TIdent]):
    """
    Adapter for emitting system/level-specific events from a shared ident contract.
    """

    build_ident_payload: Callable[[TIdent], dict[str, Any]]
    build_default_resource: Callable[[TIdent], dict[str, str]] | None = None
    build_default_payload: Callable[[TIdent], dict[str, Any]] | None = None


_SHARED_RESOURCE_KEYS = {
    "prefect.resource.id",
    "linc.opticstream.project",
}


def _validate_no_resource_key_conflicts(extra_resource: Mapping[str, str] | None) -> None:
    if not extra_resource:
        return
    conflicts = _SHARED_RESOURCE_KEYS.intersection(extra_resource.keys())
    if conflicts:
        raise ValueError(
            "extra_resource cannot override shared resource keys: "
            f"{sorted(conflicts)}"
        )


def _validate_no_payload_key_conflicts(
    base_payload: Mapping[str, Any],
    extra_payload: Mapping[str, Any] | None,
) -> None:
    if not extra_payload:
        return
    conflicts = set(base_payload.keys()).intersection(extra_payload.keys())
    if conflicts:
        raise ValueError(
            "extra_payload cannot override base payload keys: "
            f"{sorted(conflicts)}"
        )


def default_project_scoped_resource(ident: ProjectScopedIdent) -> dict[str, str]:
    return {
        "prefect.resource.id": str(ident),
        "linc.opticstream.project": ident.project_name,
    }


def emit_scoped_ident_event(
    event: str,
    ident: TIdent,
    *,
    adapter: ScopedEventAdapter[TIdent],
    extra_resource: Mapping[str, str] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> None:
    """
    Emit a Prefect event using a shared project-scoped ident contract.
    """
    _validate_no_resource_key_conflicts(extra_resource)

    resource = default_project_scoped_resource(ident)
    if adapter.build_default_resource is not None:
        resource.update(adapter.build_default_resource(ident))
    if extra_resource:
        resource.update(extra_resource)

    payload = adapter.build_ident_payload(ident)
    if adapter.build_default_payload is not None:
        payload.update(adapter.build_default_payload(ident))

    _validate_no_payload_key_conflicts(payload, extra_payload)
    if extra_payload:
        payload.update(extra_payload)

    emit_event(
        event,
        resource=resource,
        payload=payload,
    )
