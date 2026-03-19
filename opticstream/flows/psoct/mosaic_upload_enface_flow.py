from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from prefect import flow, get_run_logger

from opticstream.flows.psoct.utils import mosaic_ident_from_payload, nifti_paths_from_enface_outputs
from opticstream.state.milestone_wrappers_psoct import oct_mosaic_processing_milestone
from opticstream.state.oct_project_state import OCTMosaicId
from opticstream.state.state_guards import force_rerun_from_payload
from opticstream.tasks.dandi_upload import upload_to_dandi_batch


@flow(flow_run_name="upload-mosaic-enface-{mosaic_ident}")
@oct_mosaic_processing_milestone(field_name="enface_uploaded")
def upload_mosaic_enface_to_dandi_flow(
    mosaic_ident: OCTMosaicId,
    enface_outputs: Dict[str, Dict[str, str]],
    *,
    dandi_instance: str = "linc",
    force_rerun: bool = False,
) -> Dict[str, Any]:
    logger = get_run_logger()
    file_list = nifti_paths_from_enface_outputs(enface_outputs)
    if not file_list:
        logger.warning("No enface NIfTI files found for %s", mosaic_ident)
        return {"uploaded": 0}
    upload_to_dandi_batch(file_list=file_list, dandi_instance=dandi_instance, realpath=False)
    return {"uploaded": len(file_list)}


@flow
def upload_mosaic_enface_to_dandi_event_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    mosaic_ident = mosaic_ident_from_payload(payload)
    return upload_mosaic_enface_to_dandi_flow(
        mosaic_ident=mosaic_ident,
        enface_outputs=payload.get("enface_outputs", {}),
        force_rerun=force_rerun_from_payload(payload),
    )
