from typing import Any, Dict, Optional

from prefect import get_run_logger
from prefect.blocks.notifications import SlackWebhook

from opticstream.config.constants import SLACK_WEBHOOK_BLOCK_NAME


def notify_slack(status: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
    """
    Send a compact Slack notification using the configured webhook block.

    If the block cannot be loaded, this function logs a warning and returns
    without raising an exception.
    """
    logger = get_run_logger()
    try:
        slack_webhook_block = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            f"Failed to load Slack webhook block '{SLACK_WEBHOOK_BLOCK_NAME}': {exc}"
        )
        return

    lines = [f"[{status.upper()}] {message}"]
    if details:
        for key, value in details.items():
            lines.append(f"{key}: {value}")

    slack_webhook_block.notify("\n".join(lines))

