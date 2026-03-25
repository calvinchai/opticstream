from typing import Any
from prefect.blocks.notifications import SlackWebhook
from prefect.settings import PREFECT_UI_URL

from opticstream.config.constants import SLACK_WEBHOOK_BLOCK_NAME


def slack_notification_hook(flow: Any, flow_run: Any, state: Any) -> None:
    """
    Send a Slack notification when a flow enters a state.
    """
    slack_webhook_block = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME)
    slack_webhook_block.notify(
        (
            f"Your job {flow_run.name} entered {state.name} "
            f"See <{PREFECT_UI_URL.value()}/runs/"
            f"flow-run/{flow_run.id}|the flow run in the UI>\n\n"
            f"Tags: {flow_run.tags}\n\n"
            f"Scheduled start: {flow_run.expected_start_time}"
        )
    )
