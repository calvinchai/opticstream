"""
LSM (Light Sheet Microscopy) flows.

Submodules:
- process_strip_flow: Strip compression and processing
- upload_strip_flow: Upload strips to DANDI
"""

from opticstream.flows.lsm.channel_process_flow import (
    process_channel,
    process_channel_event,
)
from opticstream.flows.lsm.channel_upload_flow import (
    upload_channel_volume,
    upload_channel_volume_event,
)
from opticstream.flows.lsm.channel_volume_flow import (
    process_channel_volume,
    process_channel_volume_event,
)
from opticstream.flows.lsm.strip_process_flow import (
    process_strip_event,
    process_strip,
)
from opticstream.flows.lsm.strip_update_flow import (
    lsm_strip_update_event_flow,
    on_strip_events,
    strip_update_to_deployment,
)
from opticstream.flows.lsm.strip_upload_flow import to_deployment as _strip_upload_to_deployment

(
    upload_strip_to_dandi_flow_deployment,
    upload_strip_to_dandi_event_flow_deployment,
) = _strip_upload_to_deployment()
lsm_strip_update_event_flow_deployment = strip_update_to_deployment()

__all__ = [
    "lsm_strip_update_event_flow",
    "lsm_strip_update_event_flow_deployment",
    "on_strip_events",
    "process_channel",
    "process_channel_event",
    "process_channel_volume",
    "process_channel_volume_event",
    "upload_channel_volume",
    "upload_channel_volume_event",
    "process_strip_event",
    "process_strip",
    "strip_update_to_deployment",
    "upload_strip_to_dandi_event_flow_deployment",
    "upload_strip_to_dandi_flow_deployment",
]
