"""
Configuration management for OCT pipeline workflow.

Loads configuration from YAML file and provides typed access to settings.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ProcessingConfig:
    """Processing configuration settings."""
    surface_method: str = "find"
    depth: int = 80
    overlap: int = 50
    mask_threshold: int = 55


@dataclass
class PathsConfig:
    """Path configuration settings."""
    data_root: str = ""
    output_base: str = ""
    compressed_base: str = ""
    cloud_upload_path: str = ""


@dataclass
class ResourcesConfig:
    """Resource configuration settings."""
    tile_processing_workers: int = 16
    stitching_workers: int = 4
    registration_workers: int = 2


@dataclass
class CloudConfig:
    """Cloud upload configuration settings."""
    provider: str = "s3"  # s3, gcs, azure
    bucket: str = ""
    region: str = ""
    upload: Dict[str, Any] = field(default_factory=lambda: {
        "max_concurrent": 5,
        "cli_tool": "aws",
        "cli_base_args": ["s3", "cp"]
    })


@dataclass
class SlackConfig:
    """Slack notification configuration settings."""
    enabled: bool = False
    webhook_url: Optional[str] = None
    bot_token: Optional[str] = None
    channel: str = "#oct-processing"
    milestones: List[float] = field(default_factory=lambda: [0.25, 0.50, 0.75, 1.0])
    send_stitched_images: bool = True


@dataclass
class WorkflowConfig:
    """Complete workflow configuration."""
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    resources: ResourcesConfig = field(default_factory=ResourcesConfig)
    cloud: CloudConfig = field(default_factory=CloudConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)


def load_config(config_path: str) -> WorkflowConfig:
    """
    Load configuration from YAML file.
    
    Parameters
    ----------
    config_path : str
        Path to configuration YAML file
    
    Returns
    -------
    WorkflowConfig
        Loaded configuration object
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return WorkflowConfig()
    
    with open(config_file, 'r') as f:
        config_dict = yaml.safe_load(f) or {}
    
    # Parse processing config
    processing_dict = config_dict.get("processing", {})
    processing = ProcessingConfig(
        surface_method=processing_dict.get("surface_method", "find"),
        depth=processing_dict.get("depth", 80),
        overlap=processing_dict.get("overlap", 50),
        mask_threshold=processing_dict.get("mask_threshold", 55)
    )
    
    # Parse paths config
    paths_dict = config_dict.get("paths", {})
    paths = PathsConfig(
        data_root=paths_dict.get("data_root", ""),
        output_base=paths_dict.get("output_base", ""),
        compressed_base=paths_dict.get("compressed_base", ""),
        cloud_upload_path=paths_dict.get("cloud_upload_path", "")
    )
    
    # Parse resources config
    resources_dict = config_dict.get("resources", {})
    resources = ResourcesConfig(
        tile_processing_workers=resources_dict.get("tile_processing_workers", 16),
        stitching_workers=resources_dict.get("stitching_workers", 4),
        registration_workers=resources_dict.get("registration_workers", 2)
    )
    
    # Parse cloud config
    cloud_dict = config_dict.get("cloud", {})
    upload_dict = cloud_dict.get("upload", {})
    cloud = CloudConfig(
        provider=cloud_dict.get("provider", "s3"),
        bucket=cloud_dict.get("bucket", ""),
        region=cloud_dict.get("region", ""),
        upload={
            "max_concurrent": upload_dict.get("max_concurrent", 5),
            "cli_tool": upload_dict.get("cli_tool", "aws"),
            "cli_base_args": upload_dict.get("cli_base_args", ["s3", "cp"])
        }
    )
    
    # Parse slack config
    slack_dict = config_dict.get("slack", {})
    slack = SlackConfig(
        enabled=slack_dict.get("enabled", False),
        webhook_url=slack_dict.get("webhook_url"),
        bot_token=slack_dict.get("bot_token"),
        channel=slack_dict.get("channel", "#oct-processing"),
        milestones=slack_dict.get("milestones", [0.25, 0.50, 0.75, 1.0]),
        send_stitched_images=slack_dict.get("send_stitched_images", True)
    )
    
    return WorkflowConfig(
        processing=processing,
        paths=paths,
        resources=resources,
        cloud=cloud,
        slack=slack
    )


def get_slack_config_dict(config: WorkflowConfig) -> Optional[Dict[str, Any]]:
    """
    Convert SlackConfig to dictionary for task usage.
    
    Parameters
    ----------
    config : WorkflowConfig
        Workflow configuration
    
    Returns
    -------
    Optional[Dict[str, Any]]
        Slack configuration dictionary, or None if disabled
    """
    if not config.slack.enabled:
        return None
    
    return {
        "enabled": config.slack.enabled,
        "webhook_url": config.slack.webhook_url,
        "bot_token": config.slack.bot_token,
        "channel": config.slack.channel,
        "milestones": config.slack.milestones,
        "send_stitched_images": config.slack.send_stitched_images
    }

