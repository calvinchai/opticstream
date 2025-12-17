#!/usr/bin/env python3
"""
Script to retry all failed Prefect flow runs.

Usage:
    python retry_failed_runs.py [--flow-name FLOW_NAME] [--limit LIMIT] [--dry-run]
"""

import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional

from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterState
from prefect.client.schemas.sorting import FlowRunSort
from prefect.states import StateType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_failed_runs(
    flow_name: Optional[str] = None,
    limit: Optional[int] = None,
    since: Optional[datetime] = None
):
    """
    Query for failed flow runs.
    
    Parameters
    ----------
    flow_name : str, optional
        Filter by specific flow name
    limit : int, optional
        Maximum number of runs to return
    since : datetime, optional
        Only get runs since this datetime
    
    Returns
    -------
    list
        List of failed flow runs
    """
    async with get_client() as client:
        filters = FlowRunFilter(
            state=FlowRunFilterState(
                type={"any_": [StateType.FAILED, StateType.CRASHED]}
            )
        )

        if flow_name:
            filters.flow = {"name": {"any_": [flow_name]}}

        if since:
            filters.start_time = {"after_": since}

        runs = await client.read_flow_runs(
            flow_run_filter=filters,
            sort=FlowRunSort.START_TIME_DESC,
            limit=limit
        )
        return runs


async def retry_flow_run(
    run_id: str,
    dry_run: bool = False,
    requeue_same_run: bool = False
):
    """
    Retry a specific flow run.
    
    Parameters
    ----------
    run_id : str
        The flow run ID to retry
    dry_run : bool
        If True, only log what would be done without actually retrying
    requeue_same_run : bool
        If True, requeue the same run. If False, create a new run from deployment.
    """
    async with get_client() as client:
        try:
            # Get the flow run to check its state
            run = await client.read_flow_run(run_id)

            if run.state_type not in [StateType.FAILED, StateType.CRASHED]:
                logger.warning(
                    f"Flow run {run_id} is not in a failed state "
                    f"(current state: {run.state_type}). Skipping."
                )
                return None

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would retry flow run: {run_id} "
                    f"(flow: {run.name}, method: "
                    f"{'requeue' if requeue_same_run else 'new run'})"
                )
                return None

            if requeue_same_run:
                # Option 1: Requeue the same flow run
                from datetime import datetime, timezone
                from prefect.states import Scheduled

                state = Scheduled(scheduled_time=datetime.now(timezone.utc))
                result = await client.set_flow_run_state(
                    flow_run_id=run_id,
                    state=state,
                    force=True  # Required to move from terminal state to SCHEDULED
                )

                if result.status == "ACCEPT":
                    logger.info(
                        f"Successfully requeued flow run: {run_id} (flow: {run.name})")
                    return run_id
                else:
                    logger.warning(
                        f"State change not accepted for run {run_id}: {result.status}")
                    return None
            else:
                # Option 2: Create a new flow run from the same deployment (recommended)
                if not run.deployment_id:
                    logger.warning(
                        f"Flow run {run_id} has no deployment_id. "
                        f"Cannot create new run. Try using --requeue-same-run option."
                    )
                    return None

                from datetime import datetime, timezone
                from prefect.states import Scheduled

                params = getattr(run, "parameters", {}) or {}

                new_run = await client.create_flow_run_from_deployment(
                    run.deployment_id,
                    parameters=params,
                    state=Scheduled(scheduled_time=datetime.now(timezone.utc)),
                )

                logger.info(
                    f"Created new flow run: {new_run.id} from deployment "
                    f"(original run: {run_id}, flow: {run.name})"
                )
                return new_run.id

        except Exception as e:
            logger.error(f"Error retrying flow run {run_id}: {e}")
            return None


async def retry_all_failed_runs(
    flow_name: Optional[str] = None,
    limit: Optional[int] = None,
    since_days: Optional[int] = None,
    dry_run: bool = False,
    requeue_same_run: bool = False
):
    """
    Retry all failed flow runs.
    
    Parameters
    ----------
    flow_name : str, optional
        Filter by specific flow name (e.g., "process_tile_flow", "upload_flow")
    limit : int, optional
        Maximum number of runs to retry
    since_days : int, optional
        Only retry runs from the last N days
    dry_run : bool
        If True, only show what would be retried without actually retrying
    requeue_same_run : bool
        If True, requeue the same run. If False, create a new run from deployment.
    """
    since = None
    if since_days:
        since = datetime.now() - timedelta(days=since_days)

    logger.info("Querying for failed flow runs...")
    if flow_name:
        logger.info(f"Filtering by flow name: {flow_name}")
    if since:
        logger.info(f"Only considering runs since: {since}")
    if limit:
        logger.info(f"Limit: {limit} runs")

    failed_runs = await get_failed_runs(
        flow_name=flow_name,
        limit=limit,
        since=since
    )

    if not failed_runs:
        logger.info("No failed flow runs found.")
        return

    logger.info(f"Found {len(failed_runs)} failed flow run(s)")

    if dry_run:
        logger.info("\n=== DRY RUN MODE - No runs will be retried ===\n")

    # Group by flow name for summary
    by_flow = {}
    for run in failed_runs:
        flow_name_key = run.name or "Unknown"
        if flow_name_key not in by_flow:
            by_flow[flow_name_key] = []
        by_flow[flow_name_key].append(run)

    # Print summary
    logger.info("\nFailed runs summary:")
    for flow_name_key, runs in by_flow.items():
        logger.info(f"  {flow_name_key}: {len(runs)} run(s)")

    logger.info("\nRetrying failed runs...\n")

    # Retry each run
    success_count = 0
    error_count = 0

    for i, run in enumerate(failed_runs, 1):
        logger.info(
            f"[{i}/{len(failed_runs)}] Processing run {run.id} "
            f"(flow: {run.name}, state: {run.state_type})"
        )

        try:
            result = await retry_flow_run(run.id, dry_run=dry_run,
                                          requeue_same_run=requeue_same_run)
            if not dry_run and result:
                success_count += 1
            elif not dry_run:
                error_count += 1
        except Exception as e:
            logger.error(f"Failed to retry run {run.id}: {e}")
            error_count += 1

    logger.info("\n" + "=" * 50)
    if dry_run:
        logger.info(f"DRY RUN complete: {len(failed_runs)} run(s) would be retried")
    else:
        logger.info(f"Retry complete: {success_count} successful, {error_count} errors")
    logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(
        description="Retry all failed Prefect flow runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Retry all failed runs
  python retry_failed_runs.py
  
  # Retry only runs from the last 7 days
  python retry_failed_runs.py --since-days 7
  
  # Retry only specific flow
  python retry_failed_runs.py --flow-name process_tile_flow
  
  # Dry run to see what would be retried
  python retry_failed_runs.py --dry-run
  
  # Limit to 10 runs
  python retry_failed_runs.py --limit 10
  
  # Requeue the same runs instead of creating new ones
  python retry_failed_runs.py --requeue-same-run
        """
    )

    parser.add_argument(
        "--flow-name",
        type=str,
        help="Filter by specific flow name (e.g., 'process_tile_flow', 'upload_flow')"
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of runs to retry"
    )

    parser.add_argument(
        "--since-days",
        type=int,
        help="Only retry runs from the last N days"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be retried without actually retrying"
    )

    parser.add_argument(
        "--requeue-same-run",
        action="store_true",
        help="Requeue the same flow run instead of creating a new one. "
             "By default, creates a new run from the deployment (recommended)."
    )

    args = parser.parse_args()

    import asyncio

    asyncio.run(retry_all_failed_runs(
        flow_name=args.flow_name,
        limit=args.limit,
        since_days=args.since_days,
        dry_run=args.dry_run,
        requeue_same_run=args.requeue_same_run
    ))


if __name__ == "__main__":
    main()
