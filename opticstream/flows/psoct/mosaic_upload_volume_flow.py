from __future__ import annotations

from typing import Any, Dict

from prefect import flow, get_run_logger

from opticstream.events import MOSAIC_VOLUME_UPLOADED
from opticstream.events.psoct_event_emitters import emit_mosaic_psoct_event
from opticstream.flows.psoct.utils import (
    mosaic_ident_from_payload,
    non_empty_paths_from_mapping,
)
from opticstream.state.milestone_wrappers_psoct import oct_mosaic_processing_milestone
from opticstream.state.oct_project_state import OCTMosaicId
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.tasks.dandi_upload import upload_to_dandi_batch


@flow(flow_run_name="upload-mosaic-volume-{mosaic_ident}")
@oct_mosaic_processing_milestone(field_name="volume_uploaded")
def upload_mosaic_volume_to_dandi_flow(
    mosaic_ident: OCTMosaicId,
    volume_outputs: Dict[str, str],
    *,
    dandi_instance: str = "linc",
    force_rerun: bool = False,
) -> Dict[str, Any]:
    logger = get_run_logger()
    file_list = non_empty_paths_from_mapping(volume_outputs)
    if not file_list:
        logger.warning("No volume files found for %s", mosaic_ident)
        return {"uploaded": 0}
    upload_to_dandi_batch(file_list=file_list, dandi_instance=dandi_instance, realpath=False)
    emit_mosaic_psoct_event(MOSAIC_VOLUME_UPLOADED, mosaic_ident)
    return {"uploaded": len(file_list)}


@flow
def upload_mosaic_volume_to_dandi_event_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    mosaic_ident = mosaic_ident_from_payload(payload)
    return upload_mosaic_volume_to_dandi_flow(
        mosaic_ident=mosaic_ident,
        volume_outputs=payload.get("volume_outputs", {}),
        force_rerun=force_rerun_from_payload(payload),
    )
