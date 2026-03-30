from typing import Any

from prefect.blocks.notifications import SlackWebhook
from prefect.settings import PREFECT_UI_URL
from prefect.variables import Variable

from opticstream.config.constants import SLACK_ONCALL_VARIABLE_NAME, SLACK_WEBHOOK_BLOCK_NAME


def slack_notification_hook(flow: Any, flow_run: Any, state: Any) -> None:
    """
    Send a Slack notification when a flow enters a state.
    """
    oncall_person = Variable.get(SLACK_ONCALL_VARIABLE_NAME, default={})
    oncall_person_mention = " ".join([f"<@{person}>" for person in oncall_person.values()])
    slack_webhook_block = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME)
    slack_webhook_block.notify(
        (
            f"Your job {flow_run.name} entered {state.name} "
            f"See <{PREFECT_UI_URL.value()}/runs/"
            f"flow-run/{flow_run.id}|the flow run in the UI>\n\n"
            f"Oncall: {oncall_person_mention}"
        )
    )
