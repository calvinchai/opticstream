"""
Slack notification flow triggered by linc.oct.mosaic.stitched event.

This flow sends notifications to Slack when mosaics are stitched,
including uploading JPEG preview images for enface modalities.
Per Section 7.3 of design document.
"""

import os
from typing import Dict

from prefect import flow
from prefect.logging import get_run_logger

from workflow.events import MOSAIC_STITCHED, get_event_trigger
from workflow.tasks.slack_notifications import (
    send_slack_message_task,
    upload_multiple_files_to_slack_task,
)

# Default Slack configuration (can be overridden via environment variables)
DEFAULT_SLACK_BOT_TOKEN = os.getenv(
    "SLACK_BOT_TOKEN", "xoxb-5890633520343-9932310181297-biQaPdHnAP3jlE0uxhkB2RSF"
)
DEFAULT_SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "C09SA78A8QJ")


@flow
def slack_enface_notification_flow(
    payload: dict,
) -> Dict[str, bool]:
    """
        Flow triggered by linc.oct.mosaic.stitched event to send Slack notifications.

        This flow:
        1. Extracts enface output paths from the event payload
        2. Sends a text message to Slack about the completion
        3. Uploads JPEG preview images for each stitched enface modality
    """
    logger_instance = get_run_logger()

    # Extract payload data
    project_name = payload.get("project_name", "unknown")
    payload.get("project_base_path", "")
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
        f"(Project: {project_name})\n"
        f"✅ 2D Enface images stitched for mosaic {mosaic_id} "
        f"Modalities: {', '.join(enface_outputs.keys())}"
    )

    send_slack_message_task(
        message=message,
        slack_bot_token=slack_bot_token,
        slack_channel_id=slack_channel_id,
    )

    # Collect all JPEG images for batch upload
    filepaths = []
    titles = []
    modality_to_filepath = {}  # Map modality to filepath for result conversion

    for modality, outputs in enface_outputs.items():
        jpeg_path = outputs.get("jpeg")

        if jpeg_path and os.path.exists(jpeg_path):
            filepaths.append(jpeg_path)
            titles.append(f"Mosaic {mosaic_id} - {modality.upper()}")
            modality_to_filepath[modality] = jpeg_path
        else:
            logger_instance.warning(
                f"JPEG path not found or doesn't exist for {modality}: {jpeg_path}"
            )

    # Upload all JPEG images in a single message
    upload_results = {}
    if filepaths:
        initial_comment = (
            f"Stitched enface images for mosaic {mosaic_id} "
            f"(project: {project_name})"
        )

        try:
            file_results = upload_multiple_files_to_slack_task(
                filepaths=filepaths,
                slack_bot_token=slack_bot_token,
                slack_channel_id=slack_channel_id,
                titles=titles,
                initial_comment=initial_comment,
            )

            # Convert filepath-based results to modality-based results
            for modality, filepath in modality_to_filepath.items():
                upload_results[modality] = file_results.get(filepath, False)
                logger_instance.info(
                    f"Upload result for {modality} JPEG: {upload_results[modality]}"
                )
        except Exception as e:
            logger_instance.error(f"Failed to upload JPEGs to Slack: {e}")
            # Mark all modalities as failed
            for modality in modality_to_filepath.keys():
                upload_results[modality] = False
    else:
        logger_instance.warning("No valid JPEG files to upload")
        # Mark all modalities as failed
        for modality in enface_outputs.keys():
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
            get_event_trigger(MOSAIC_STITCHED),
        ],
    )
