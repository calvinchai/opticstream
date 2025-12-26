"""
Slack notification tasks for workflow events.
"""

import os
from typing import Optional

from prefect import get_run_logger, task
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


@task(name="upload_image_to_slack", retries=2, retry_delay_seconds=5)
def upload_image_to_slack_task(
    filepath: str,
    slack_bot_token: str,
    slack_channel_id: str,
    title: Optional[str] = None,
    initial_comment: Optional[str] = None,
) -> bool:
    """
    Upload an image file to Slack channel.
    
    Parameters
    ----------
    filepath : str
        Path to the image file to upload
    slack_bot_token : str
        Slack bot token (xoxb-...)
    slack_channel_id : str
        Slack channel ID (C...)
    title : str, optional
        Title for the uploaded file
    initial_comment : str, optional
        Initial comment to post with the file
        
    Returns
    -------
    bool
        True if upload successful, False otherwise
    """
    logger_instance = get_run_logger()
    client = WebClient(token=slack_bot_token)
    filename = os.path.basename(filepath)

    if not os.path.exists(filepath):
        logger_instance.error(f"File not found: {filepath}")
        return False

    if title is None:
        title = f"New Image: {filename}"

    if initial_comment is None:
        initial_comment = f"Detected new file: {filename}"

    try:
        logger_instance.info(f"Uploading to Slack: {filename}")

        response = client.files_upload_v2(
            channel=slack_channel_id,
            file=filepath,
            title=title,
            initial_comment=initial_comment,
        )
        logger_instance.info(f"Successfully uploaded {filename} to Slack")
        return True

    except SlackApiError as e:
        logger_instance.error(f"Slack upload error: {e.response['error']}")
        raise


@task(name="send_slack_message", retries=2, retry_delay_seconds=5)
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
                f"Slack API returned error: {response.get('error', 'Unknown error')}")
            return False

    except SlackApiError as e:
        logger_instance.error(f"Slack API error: {e.response['error']}")
        raise
