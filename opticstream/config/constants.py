from typing import Literal

SLACK_WEBHOOK_BLOCK_NAME = "watchdog"
SLACK_API_TOKEN_BLOCK_NAME = "watchdog"
SLACK_CHANNEL_BLOCK_NAME = "watchdog-channel"

DANDI_API_TOKEN_BLOCK_NAME = "dandi-api-key"
LINC_API_TOKEN_BLOCK_NAME = "linc-api-key"

DANDI_INSTANCE = Literal["dandi", "linc"]