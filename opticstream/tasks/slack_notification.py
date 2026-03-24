import os
from typing import Dict, List, Optional

from prefect import get_run_logger, task
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from prefect.blocks.system import Secret
from prefect.blocks.notifications import SlackWebhook
from prefect_slack import SlackCredentials

from opticstream.config.constants import SLACK_API_TOKEN_BLOCK_NAME, SLACK_CHANNEL_BLOCK_NAME, SLACK_WEBHOOK_BLOCK_NAME

@task(retries=2, retry_delay_seconds=5)
def send_slack_message(
    message: str,
    slack_bot_token: str = SlackCredentials.load(SLACK_API_TOKEN_BLOCK_NAME).token.get_secret_value(),
    slack_channel_id: str = Secret.load(SLACK_CHANNEL_BLOCK_NAME).get(),
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
def send_slack_message_webhook(
    body:str,
    subject: Optional[str] = None,
    webhook: SlackWebhook = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME),
) -> bool:
    """
    Send a text message to Slack channel using a webhook.
    """
    webhook.notify(body, subject=subject)

@task(retries=2, retry_delay_seconds=5)
def upload_multiple_files_to_slack(
    filepaths: List[str],
    slack_bot_token: str = SlackCredentials.load(SLACK_API_TOKEN_BLOCK_NAME).token.get_secret_value(),
    slack_channel_id: str = Secret.load(SLACK_CHANNEL_BLOCK_NAME).get(),
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
