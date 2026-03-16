from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Annotated, List, Optional, Sequence

from cyclopts import Parameter
from prefect import get_client
from prefect.client.schemas import StateType
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterExpectedStartTime,
    FlowRunFilterName,
    FlowRunFilterState,
    FlowRunFilterStateType
)
from prefect.states import Cancelled

from .cli import utils_cli


DEFAULT_STATES: Sequence[str] = ("CANCELLING", "SCHEDULED", "RUNNING")


@dataclass
class _SelectionSummary:
    cutoff: datetime
    states: Sequence[str]
    name_filter: Optional[str]
    limit: int
    matched: int


async def _cancel_flow_runs_impl(
    *,
    states: Optional[Sequence[str]],
    name: Optional[str],
    older_than_days: float,
    limit: int,
    delete: bool,
    dry_run: bool,
) -> None:
    effective_states: Sequence[str] = tuple(states) if states else DEFAULT_STATES

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=older_than_days)
    state_filter = FlowRunFilterStateType(not_any_=["COMPLETED", "FAILED", "CANCELLED", "CRASHED"])
    state_filter = FlowRunFilterState(type=state_filter)
    created_filter = FlowRunFilterExpectedStartTime(before_=cutoff)
    name_filter = FlowRunFilterName(like_=name) if name else None

    flow_run_filter = FlowRunFilter(
        state=state_filter,
        created=created_filter,
        name=name_filter,
    )
    print(flow_run_filter)

    async with get_client() as client:
        runs = await client.read_flow_runs(flow_run_filter=flow_run_filter, limit=limit)

        summary = _SelectionSummary(
            cutoff=cutoff,
            states=effective_states,
            name_filter=name,
            limit=limit,
            matched=len(runs),
        )

        _print_selection_summary(summary)

        if not runs:
            return

        if dry_run:
            for run in runs:
                print(
                    f"[DRY] id={run.id} name={getattr(run, 'name', None)!r} "
                    f"state={getattr(run.state, 'type', None)!r} "
                    f"created={getattr(run, 'created', None)}"
                )
            return

        cancelled_count = 0
        deleted_count = 0

        for run in runs:
            try:
                await client.set_flow_run_state(
                    flow_run_id=run.id,
                    state=Cancelled(),
                    force=True,
                )
                cancelled_count += 1
                print(f"[CANCELLED] {run.id}")
            except Exception as exc:
                print(f"[ERROR] Failed to cancel {run.id}: {exc}")
                continue

            if delete:
                try:
                    await client.delete_flow_run(flow_run_id=run.id)
                    deleted_count += 1
                    print(f"[DELETED] {run.id}")
                except Exception as exc:
                    print(f"[ERROR] Failed to delete {run.id}: {exc}")

        print(
            f"[SUMMARY] matched={summary.matched} "
            f"cancelled={cancelled_count} "
            f"deleted={deleted_count}"
        )


def _print_selection_summary(summary: _SelectionSummary) -> None:
    name_clause = summary.name_filter if summary.name_filter is not None else "ANY"
    print(
        "[SELECT] "
        f"states={list(summary.states)} "
        f"name={name_clause} "
        f"created_before={summary.cutoff.isoformat()} "
        f"limit={summary.limit} "
        f"matched={summary.matched}"
    )


@utils_cli.command
def cancel_flow_runs(
    *,
    states: Optional[List[str]] = None,
    name: Optional[str] = None,
    older_than_days: float = 1.0,
    limit: int = 10,
    delete: bool = False,
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry-run", "-n"]),
    ] = False,
) -> None:
    """
    Batch-cancel Prefect flow runs and optionally delete them.

    By default, this command targets flow runs in states Running, Cancelling, or
    Waiting, regardless of name, that were created more than 1 day ago.

    Parameters
    ----------
    states:
        Optional list of flow run states to target. When omitted, defaults to
        Running, Cancelling, and Waiting.
    name:
        Optional substring filter on the flow run name. When omitted, all flow
        run names are included.
    older_than_days:
        Only include flow runs created more than this many days ago. Defaults
        to 1.0.
    limit:
        Maximum number of flow runs to process in a single invocation.
    delete:
        When True, delete each matching flow run after requesting cancellation.
    dry_run:
        When True (or when using the -n / --dry-run flag), only list matching
        flow runs without cancelling or deleting them.
    """
    asyncio.run(
        _cancel_flow_runs_impl(
            states=states,
            name=name,
            older_than_days=older_than_days,
            limit=limit,
            delete=delete,
            dry_run=dry_run,
        )
    )

