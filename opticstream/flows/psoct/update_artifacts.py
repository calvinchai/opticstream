from __future__ import annotations

from datetime import datetime

from prefect.artifacts import create_table_artifact
from prefect.logging import get_run_logger
from prefect import task

from opticstream.state.oct_project_state import OCT_STATE_SERVICE, OCTMosaicId


@task(task_run_name="publish-mosaic-progress-{mosaic_ident}")
def publish_mosaic_progress_artifact(mosaic_ident: OCTMosaicId) -> str:
    mosaic_view = OCT_STATE_SERVICE.peek_mosaic(mosaic_ident=mosaic_ident)
    if mosaic_view is None:
        return ""
    batches = list(mosaic_view.batches.values())
    total_batches = len(batches)
    table = [
        {
            "state": "processed",
            "count": sum(1 for b in batches if b.enface_processed),
            "total": total_batches,
        },
        {
            "state": "archived",
            "count": sum(1 for b in batches if b.archived),
            "total": total_batches,
        },
        {
            "state": "uploaded",
            "count": sum(1 for b in batches if b.uploaded),
            "total": total_batches,
        },
    ]
    artifact_key = (
        f"{mosaic_ident.project_name.lower().replace('_', '-')}-"
        f"mosaic-{mosaic_ident.mosaic_id}-progress"
    )
    create_table_artifact(
        key=artifact_key,
        table=table,
        description=f"Last updated: {datetime.now().isoformat()}",
    )
    get_run_logger().info("Updated artifact %s", artifact_key)
    return artifact_key
