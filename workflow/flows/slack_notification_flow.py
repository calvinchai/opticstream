"""
Slack notification flow triggered by mosaic.enface_stitched event.

This flow sends notifications to Slack when 2D enface images are stitched,
including uploading JPEG preview images.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Optional

from prefect import flow, get_run_logger
from prefect.events import DeploymentEventTrigger

from workflow.tasks.slack_notifications import (
    upload_image_to_slack_task,
    send_slack_message_task,
)

logger = logging.getLogger(__name__)


# Default Slack configuration (can be overridden via environment variables)
DEFAULT_SLACK_BOT_TOKEN = os.getenv(
    "SLACK_BOT_TOKEN",
    "xoxb-5890633520343-9932310181297-biQaPdHnAP3jlE0uxhkB2RSF"
)
DEFAULT_SLACK_CHANNEL_ID = os.getenv(
    "SLACK_CHANNEL_ID",
    "C09SA78A8QJ"
)


@flow(name="slack_enface_notification_flow")
def slack_enface_notification_flow(
    payload: dict,
) -> Dict[str, bool]:
    """
    Flow triggered by mosaic.enface_stitched event to send Slack notifications.
    
    This flow:
    1. Extracts enface output paths from the event payload
    2. Sends a text message to Slack about the completion
    3. Uploads JPEG preview images for each stitched enface modality
    
    Parameters
    ----------
    payload : dict
        Event payload containing:
        - project_name: str
        - project_base_path: str
        - mosaic_id: int
        - enface_outputs: Dict[str, Dict[str, str]] - mapping modality to output paths
        
    Returns
    -------
    Dict[str, bool]
        Dictionary mapping modality to upload success status
    """
    logger_instance = get_run_logger()
    
    # Extract payload data
    project_name = payload.get("project_name", "unknown")
    project_base_path = payload.get("project_base_path", "")
    mosaic_id = payload.get("mosaic_id", 0)
    enface_outputs = payload.get("enface_outputs", {})
    
    # Get Slack configuration from environment or use defaults
    slack_bot_token = os.getenv("SLACK_BOT_TOKEN", DEFAULT_SLACK_BOT_TOKEN)
    slack_channel_id = os.getenv("SLACK_CHANNEL_ID", DEFAULT_SLACK_CHANNEL_ID)
    
    logger_instance.info(
        f"Processing Slack notification for mosaic {mosaic_id} "
        f"in project {project_name}"
    )
    
    # Send initial notification message
    message = (
        f"✅ 2D Enface images stitched for mosaic {mosaic_id} "
        f"(project: {project_name})\n"
        f"Modalities: {', '.join(enface_outputs.keys())}"
    )
    
    send_slack_message_task(
        message=message,
        slack_bot_token=slack_bot_token,
        slack_channel_id=slack_channel_id,
    )
    
    # Upload JPEG images for each modality
    upload_results = {}
    for modality, outputs in enface_outputs.items():
        jpeg_path = outputs.get("jpeg")
        
        if jpeg_path and os.path.exists(jpeg_path):
            title = f"Mosaic {mosaic_id} - {modality.upper()}"
            initial_comment = (
                f"Stitched {modality.upper()} enface image for "
                f"mosaic {mosaic_id} (project: {project_name})"
            )
            
            try:
                success = upload_image_to_slack_task(
                    filepath=jpeg_path,
                    slack_bot_token=slack_bot_token,
                    slack_channel_id=slack_channel_id,
                    title=title,
                    initial_comment=initial_comment,
                )
                upload_results[modality] = success
                logger_instance.info(
                    f"Uploaded {modality} JPEG to Slack: {success}"
                )
            except Exception as e:
                logger_instance.error(
                    f"Failed to upload {modality} JPEG to Slack: {e}"
                )
                upload_results[modality] = False
        else:
            logger_instance.warning(
                f"JPEG path not found or doesn't exist for {modality}: {jpeg_path}"
            )
            upload_results[modality] = False
    
    logger_instance.info(
        f"Slack notification complete for mosaic {mosaic_id}. "
        f"Upload results: {upload_results}"
    )
    
    return upload_results


# Deployment configuration for event-driven triggering
if __name__ == "__main__":
    slack_enface_notification_deployment = slack_enface_notification_flow.to_deployment(
        name="slack_enface_notification_flow",
        tags=["event-driven", "slack-notifications", "enface-stitched"],
        triggers=[
            DeploymentEventTrigger(
                expect={"mosaic.enface_stitched"},
                parameters={
                    "payload": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload | tojson }}",
                        }
                    }
                },
            )
        ],
    )



