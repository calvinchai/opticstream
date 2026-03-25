from __future__ import annotations

import asyncio
from typing import Annotated, Optional, Protocol, runtime_checkable

from cyclopts import Parameter
from prefect import get_client

from .cli import utils_cli


@runtime_checkable
class _HasNameAndId(Protocol):
    id: object
    name: str


async def _clear_resource(
    label: str,
    items: list[_HasNameAndId],
    delete_fn,
    dry_run: bool,
) -> tuple[int, int]:
    matched = len(items)
    deleted = 0
    for item in items:
        extra = f" flow_name={item.flow_name!r}" if hasattr(item, "flow_name") else ""
        desc = f"id={item.id} name={item.name!r}{extra}"
        if dry_run:
            print(f"[DRY] [{label}] {desc}")
        else:
            try:
                await delete_fn(item.id)
                deleted += 1
                print(f"[DELETED] [{label}] {desc}")
            except Exception as exc:
                print(f"[ERROR] [{label}] Failed to delete {item.id} ({item.name!r}): {exc}")
    return matched, deleted


async def _clear_impl(
    *,
    flows: bool,
    deployments: bool,
    automations: bool,
    name: Optional[str],
    dry_run: bool,
) -> None:
    clear_all = not flows and not deployments and not automations
    do_flows = clear_all or flows
    do_deployments = clear_all or deployments
    do_automations = clear_all or automations

    total_matched = 0
    total_deleted = 0

    async with get_client() as client:
        name_clause = repr(name) if name else "ANY"

        if do_flows:
            all_flows = await client.read_flows()
            selected = [f for f in all_flows if name is None or name in f.name]
            print(f"[SELECT] [flows] name={name_clause} matched={len(selected)}")
            m, d = await _clear_resource("flows", selected, client.delete_flow, dry_run)
            total_matched += m
            total_deleted += d

        if do_deployments:
            all_deps = await client.read_deployments()
            selected = [dep for dep in all_deps if name is None or name in dep.name]
            print(f"[SELECT] [deployments] name={name_clause} matched={len(selected)}")
            m, d = await _clear_resource("deployments", selected, client.delete_deployment, dry_run)
            total_matched += m
            total_deleted += d

        if do_automations:
            all_autos = await client.read_automations()
            selected = [a for a in all_autos if name is None or name in a.name]
            print(f"[SELECT] [automations] name={name_clause} matched={len(selected)}")
            m, d = await _clear_resource("automations", selected, client.delete_automation, dry_run)
            total_matched += m
            total_deleted += d

    if not dry_run:
        print(f"[SUMMARY] total_matched={total_matched} total_deleted={total_deleted}")


@utils_cli.command
def clear(
    *,
    flows: bool = False,
    deployments: bool = False,
    automations: bool = False,
    name: Annotated[
        Optional[str],
        Parameter(name=["--name", "-f"]),
    ] = None,
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry-run", "-n"]),
    ] = True,
) -> None:
    """
    Delete Prefect flows, deployments, and/or automations.

    When none of --flows, --deployments, or --automations are specified, all
    three resource types are cleared.

    Parameters
    ----------
    flows:
        Clear registered flows.
    deployments:
        Clear deployments.
    automations:
        Clear automations.
    name:
        Optional substring filter applied to resource names. When omitted,
        all resources of each selected type are targeted.
    dry_run:
        When True (or when using the -n / --dry-run flag), only list matching
        resources without deleting them.
    """
    asyncio.run(
        _clear_impl(
            flows=flows,
            deployments=deployments,
            automations=automations,
            name=name,
            dry_run=dry_run,
        )
    )
