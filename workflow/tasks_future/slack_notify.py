import os
import time
import glob
import shutil
from prefect import flow, task, get_run_logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from prefect.blocks.system import Secret

def get_slack_bot_token_from_block(block_name: str = "slack-bot-token") -> str:
    """
    Retrieve the Slack bot token from a Prefect Secret block.

    Args:
        block_name (str): The name of the block containing the Slack bot token.

    Returns:
        str: The Slack bot token.
    """
    secret_block = Secret.load(block_name)
    return secret_block.get()


@task(name="Upload to Slack", retries=2, retry_delay_seconds=5)
def upload_image_to_slack(filepath, slack_bot_token_block: str = "slack-bot-token", slack_channel_id: str = None):
    """
    Upload an image to Slack.
    
    Parameters
    ----------
    filepath : str
        Path to the image file to upload
    slack_bot_token_block : str
        Name of the Prefect Secret block containing the Slack bot token
    slack_channel_id : str, optional
        Slack channel ID to upload to. If not provided, must be set via environment variable or config.
    """
    logger = get_run_logger()
    slack_bot_token = get_slack_bot_token_from_block(slack_bot_token_block)
    client = WebClient(token=slack_bot_token)
    filename = os.path.basename(filepath)
    
    # Get channel ID from parameter or environment variable
    channel_id = slack_channel_id or os.environ.get("SLACK_CHANNEL_ID")
    if not channel_id:
        raise ValueError("slack_channel_id must be provided or set as SLACK_CHANNEL_ID environment variable")

    try:
        logger.info(f"Uploading: {filename}")

        response = client.files_upload_v2(
            channel=channel_id,
            file=filepath,
            title=f"New Image: {filename}",
            initial_comment=f"Detected new file: {filename}"
        )
        logger.info(f"Uploaded {filename}")
        return True
        
    except SlackApiError as e:
        logger.error(f"Slack upload error: {e.response['error']}")
        raise