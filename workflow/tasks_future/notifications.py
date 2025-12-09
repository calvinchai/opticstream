"""
Tasks for sending notifications (Slack).
"""

import logging
from typing import Dict, List, Optional
from prefect import task

logger = logging.getLogger(__name__)


@task(name="notify_slack", allow_failure=True)
def notify_slack_task(
    message: str,
    image_path: Optional[str] = None,
    slack_config: Optional[Dict] = None
) -> bool:
    """
    Send notification to Slack channel (async, non-blocking).
    
    Parameters
    ----------
    message : str
        Message to send
    image_path : str, optional
        Path to image file to upload
    slack_config : dict, optional
        Slack configuration (webhook_url, bot_token, channel)
    
    Returns
    -------
    bool
        True if notification sent successfully
    """
    if not slack_config or not slack_config.get("enabled", False):
        return False
    
    logger.info(f"Sending Slack notification: {message}")
    
    try:
        # TODO: Implement actual Slack integration
        # Option 1: Webhook
        # if slack_config.get("webhook_url"):
        #     from slack_sdk.webhook import WebhookClient
        #     webhook = WebhookClient(slack_config["webhook_url"])
        #     if image_path:
        #         with open(image_path, "rb") as f:
        #             response = webhook.send(text=message, files=[("image", f)])
        #     else:
        #         response = webhook.send(text=message)
        #     return response.status_code == 200
        
        # Option 2: Bot API
        # if slack_config.get("bot_token"):
        #     from slack_sdk import WebClient
        #     client = WebClient(token=slack_config["bot_token"])
        #     channel = slack_config.get("channel", "#oct-processing")
        #     if image_path:
        #         response = client.files_upload_v2(
        #             channel=channel,
        #             file=image_path,
        #             initial_comment=message
        #         )
        #     else:
        #         response = client.chat_postMessage(
        #             channel=channel,
        #             text=message
        #         )
        #     return response["ok"]
        
        logger.debug(f"Slack notification (placeholder): {message}")
        return True
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
        return False


@task(name="notify_tile_complete", allow_failure=True)
def notify_tile_complete_task(
    mosaic_id: str,
    tile_index: int,
    slack_config: Optional[Dict] = None
) -> bool:
    """
    Send tile completion notification (async, non-blocking).
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    tile_index : int
        Tile index
    slack_config : dict, optional
        Slack configuration
    
    Returns
    -------
    bool
        True if notification sent successfully
    """
    message = f"✅ Tile {tile_index} in {mosaic_id} completed"
    return notify_slack_task(message, slack_config=slack_config)


@task(name="notify_stitched_complete", allow_failure=True)
def notify_stitched_complete_task(
    mosaic_id: str,
    aip_path: str,
    slack_config: Optional[Dict] = None
) -> bool:
    """
    Send stitched image to Slack channel (async, non-blocking).
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    aip_path : str
        Path to stitched AIP image
    slack_config : dict, optional
        Slack configuration
    
    Returns
    -------
    bool
        True if notification sent successfully
    """
    message = f"🎨 Stitched mosaic {mosaic_id} completed"
    return notify_slack_task(message, image_path=aip_path, slack_config=slack_config)


@task(name="monitor_tile_progress")
def monitor_tile_progress_task(
    mosaic_id: str,
    total_tiles: int,
    completed_tiles: List[str],
    slack_config: Optional[Dict] = None
) -> None:
    """
    Monitor tile progress and send milestone notifications (async, background).
    
    Parameters
    ----------
    mosaic_id : str
        Mosaic identifier
    total_tiles : int
        Total number of tiles
    completed_tiles : List[str]
        List of completed tile indices
    slack_config : dict, optional
        Slack configuration
    """
    completed_count = len(completed_tiles)
    progress = completed_count / total_tiles if total_tiles > 0 else 0.0
    
    milestones = [0.25, 0.50, 0.75, 1.0]
    # TODO: Track milestones per mosaic (could use Redis for distributed)
    # For now, this is a simple implementation that sends all milestones
    # In production, you'd want to track which milestones have been sent
    
    for milestone in milestones:
        if progress >= milestone:
            message = (
                f"🎯 Mosaic {mosaic_id}: {milestone*100:.0f}% complete "
                f"({completed_count}/{total_tiles} tiles)"
            )
            notify_slack_task.submit(message, slack_config=slack_config)
            # Note: In production, you'd want to track sent milestones
            # to avoid sending duplicates

