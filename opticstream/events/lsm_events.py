"""Prefect event names for LSM flows."""

STRIP_READY = "linc.opticstream.lsm.strip.ready"
STRIP_PROCESSED = "linc.opticstream.lsm.strip.processed"
STRIP_COMPRESSED = "linc.opticstream.lsm.strip.compressed"
STRIP_UPLOADED = "linc.opticstream.lsm.strip.uploaded"
STRIP_ARCHIVED = "linc.opticstream.lsm.strip.archived"

CHANNEL_READY = "linc.opticstream.lsm.channel.ready"
CHANNEL_MIP_STITCHED = "linc.opticstream.lsm.channel.mip_stitched"
CHANNEL_VOLUME_STITCHED = "linc.opticstream.lsm.channel.volume_stitched"
CHANNEL_VOLUME_UPLOADED = "linc.opticstream.lsm.channel.volume_uploaded"
