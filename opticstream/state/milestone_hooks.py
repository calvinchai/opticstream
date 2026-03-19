from __future__ import annotations

from typing import Any, Callable

from opticstream.state.lsm_project_state import LSMChannelId, LSMStripId


def send_ops_slack_message(message: str) -> None:
    # TODO: wire Slack client
    raise NotImplementedError


def channel_success_slack_message(
    template: str,
) -> Callable[[LSMChannelId, Any], None]:
    def _hook(channel_ident: LSMChannelId, result: Any) -> None:
        message = template.format(channel_ident=channel_ident, result=result)
        send_ops_slack_message(message)

    return _hook


def strip_success_slack_message(
    template: str,
) -> Callable[[LSMStripId, Any], None]:
    def _hook(strip_ident: LSMStripId, result: Any) -> None:
        message = template.format(strip_ident=strip_ident, result=result)
        send_ops_slack_message(message)

    return _hook


def channel_failure_slack_hook(
    field_name: str,
) -> Callable[[LSMChannelId, Exception], None]:
    def _hook(channel_ident: LSMChannelId, exc: Exception) -> None:
        send_ops_slack_message(
            f"Channel milestone failed: {field_name} for {channel_ident}. Error: {exc}"
        )

    return _hook


def strip_failure_slack_hook(
    field_name: str,
) -> Callable[[LSMStripId, Exception], None]:
    def _hook(strip_ident: LSMStripId, exc: Exception) -> None:
        send_ops_slack_message(
            f"Strip milestone failed: {field_name} for {strip_ident}. Error: {exc}"
        )

    return _hook