"""
Slack notification flow triggered by linc.oct.mosaic.stitched event.

This flow sends notifications to Slack when mosaics are stitched,
including uploading JPEG preview images for enface modalities.
Per Section 7.3 of design document.
"""

import os
from typing import Dict, List, Optional

from prefect import flow, task
from prefect.logging import get_run_logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from opticstream.events import MOSAIC_STITCHED, get_event_trigger


@task(retries=2, retry_delay_seconds=5)
def send_slack_message_task(
    message: str,
    slack_bot_token: str,
    slack_channel_id: str,
) -> bool:
    """
    Send a text message to Slack channel.

    Parameters
    ----------
    message : str
        Message text to send
    slack_bot_token : str
        Slack bot token (xoxb-...)
    slack_channel_id : str
        Slack channel ID (C...)

    Returns
    -------
    bool
        True if message sent successfully, False otherwise
    """
    logger_instance = get_run_logger()
    client = WebClient(token=slack_bot_token)

    try:
        logger_instance.info(f"Sending Slack message: {message[:100]}...")

        response = client.chat_postMessage(
            channel=slack_channel_id,
            text=message,
        )

        if response["ok"]:
            logger_instance.info("Successfully sent message to Slack")
            return True
        else:
            logger_instance.error(
                f"Slack API returned error: {response.get('error', 'Unknown error')}"
            )
            return False

    except SlackApiError as e:
        logger_instance.error(f"Slack API error: {e.response['error']}")
        raise


@task(retries=2, retry_delay_seconds=5)
def upload_multiple_files_to_slack_task(
    filepaths: List[str],
    slack_bot_token: str,
    slack_channel_id: str,
    titles: Optional[List[str]] = None,
    initial_comment: Optional[str] = None,
    thread_ts: Optional[str] = None,
) -> Dict[str, bool]:
    """
    Upload multiple files to Slack channel in a single API call.

    Parameters
    ----------
    filepaths : List[str]
        List of paths to files to upload
    slack_bot_token : str
        Slack bot token (xoxb-...)
    slack_channel_id : str
        Slack channel ID (C...)
    titles : List[str], optional
        List of titles for each uploaded file. If None, uses filenames.
        Must match length of filepaths if provided.
    initial_comment : str, optional
        Initial comment to post with the files
    thread_ts : str, optional
        Thread timestamp to reply in a thread

    Returns
    -------
    Dict[str, bool]
        Dictionary mapping filepath to upload success status
    """
    logger_instance = get_run_logger()
    client = WebClient(token=slack_bot_token)

    if not filepaths:
        logger_instance.warning("No files provided for upload")
        return {}

    # Validate all files exist
    valid_files = []
    invalid_files = []
    for filepath in filepaths:
        if os.path.exists(filepath):
            valid_files.append(filepath)
        else:
            logger_instance.error(f"File not found: {filepath}")
            invalid_files.append(filepath)

    if not valid_files:
        logger_instance.error("No valid files to upload")
        return {fp: False for fp in filepaths}

    # Build file_uploads list
    file_uploads = []
    for idx, filepath in enumerate(valid_files):
        filename = os.path.basename(filepath)
        title = titles[idx] if titles and idx < len(titles) else filename

        file_upload_dict = {
            "file": filepath,
            "filename": filename,
            "title": title,
        }
        file_uploads.append(file_upload_dict)

    # Build result dictionary with invalid files marked as False
    results = {fp: False for fp in invalid_files}

    try:
        logger_instance.info(
            f"Uploading {len(valid_files)} file(s) to Slack: "
            f"{', '.join([os.path.basename(fp) for fp in valid_files])}"
        )

        # Prepare API call parameters
        api_params = {
            "channel": slack_channel_id,
            "file_uploads": file_uploads,
        }

        if initial_comment:
            api_params["initial_comment"] = initial_comment

        if thread_ts:
            api_params["thread_ts"] = thread_ts

        response = client.files_upload_v2(**api_params)

        # Mark all valid files as successful if API call succeeded
        if response.get("ok", False):
            logger_instance.info(
                f"Successfully uploaded {len(valid_files)} file(s) to Slack"
            )
            for filepath in valid_files:
                results[filepath] = True
        else:
            error_msg = response.get("error", "Unknown error")
            logger_instance.error(f"Slack API returned error: {error_msg}")
            for filepath in valid_files:
                results[filepath] = False

        return results

    except SlackApiError as e:
        logger_instance.error(f"Slack upload error: {e.response['error']}")
        # Mark all valid files as failed
        for filepath in valid_files:
            results[filepath] = False
        raise


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
            f"Stitched enface images for mosaic {mosaic_id} (project: {project_name})"
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
