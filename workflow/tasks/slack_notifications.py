"""
Slack notification tasks for workflow events.
"""

import os
from typing import Dict, List, Optional

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


@task(name="upload_multiple_files_to_slack", retries=2, retry_delay_seconds=5)
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
