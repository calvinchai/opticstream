"""
Main entry point for OCT pipeline workflow.

This module provides command-line interface and programmatic entry point
for running the Prefect workflow.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, List

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from .flows import process_experiment_flow
from .config import load_config, get_slack_config_dict
from .upload_queue import get_upload_queue_manager

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def run_experiment(
    config_path: str,
    data_root_path: Optional[str] = None,
    output_base_path: Optional[str] = None,
    slice_numbers: Optional[List[int]] = None,
    dry_run: bool = False
):
    """
    Run experiment processing workflow.
    
    Parameters
    ----------
    config_path : str
        Path to configuration YAML file
    data_root_path : str, optional
        Override data root path from config
    output_base_path : str, optional
        Override output base path from config
    slice_numbers : List[int], optional
        Optional list of slice numbers to process
    dry_run : bool
        If True, only validate configuration without running
    """
    # Load configuration
    config = load_config(config_path)
    
    # Override paths if provided
    if data_root_path:
        config.paths.data_root = data_root_path
    if output_base_path:
        config.paths.output_base = output_base_path
    
    # Validate required paths
    if not config.paths.data_root:
        raise ValueError("data_root_path must be provided in config or as argument")
    if not config.paths.output_base:
        raise ValueError("output_base_path must be provided in config or as argument")
    if not config.paths.compressed_base:
        raise ValueError("compressed_base_path must be provided in config")
    
    if dry_run:
        logger.info("Dry run mode - configuration validated successfully")
        logger.info(f"Data root: {config.paths.data_root}")
        logger.info(f"Output base: {config.paths.output_base}")
        logger.info(f"Compressed base: {config.paths.compressed_base}")
        return
    
    # Initialize upload queue if cloud config is provided
    upload_queue = None
    if config.cloud.bucket:
        upload_queue = get_upload_queue_manager(
            max_concurrent=config.cloud.upload["max_concurrent"],
            cli_tool=config.cloud.upload["cli_tool"],
            cli_base_args=config.cloud.upload["cli_base_args"]
        )
    
    # Get Slack config
    slack_config = get_slack_config_dict(config)
    
    # Run experiment flow
    logger.info("Starting experiment processing workflow")
    result = process_experiment_flow(
        data_root_path=config.paths.data_root,
        output_base_path=config.paths.output_base,
        compressed_base_path=config.paths.compressed_base,
        slice_numbers=slice_numbers,
        surface_method=config.processing.surface_method,
        depth=config.processing.depth,
        mask_threshold=config.processing.mask_threshold,
        overlap=config.processing.overlap,
        gamma=0.0,  # TODO: Add to config
        upload_queue=upload_queue,
        slack_config=slack_config
    )
    
    logger.info("Experiment processing workflow completed")
    return result


def create_deployment(
    config_path: str,
    deployment_name: str = "oct-pipeline",
    work_pool_name: str = "default",
    schedule: Optional[str] = None
):
    """
    Create Prefect deployment for the workflow.
    
    Parameters
    ----------
    config_path : str
        Path to configuration YAML file
    deployment_name : str
        Name for the deployment
    work_pool_name : str
        Name of the work pool to use
    schedule : str, optional
        Cron schedule string (e.g., "0 0 * * *" for daily at midnight)
    """
    # Load config to get default parameters
    config = load_config(config_path)
    
    # Create deployment
    deployment = Deployment.build_from_flow(
        flow=process_experiment_flow,
        name=deployment_name,
        work_pool_name=work_pool_name,
        parameters={
            "data_root_path": config.paths.data_root,
            "output_base_path": config.paths.output_base,
            "compressed_base_path": config.paths.compressed_base,
            "surface_method": config.processing.surface_method,
            "depth": config.processing.depth,
            "mask_threshold": config.processing.mask_threshold,
            "overlap": config.processing.overlap,
        },
        schedule=CronSchedule(cron=schedule) if schedule else None
    )
    
    # Apply deployment
    deployment.apply()
    logger.info(f"Deployment '{deployment_name}' created successfully")


def main():
    """Command-line interface entry point."""
    parser = argparse.ArgumentParser(
        description="OCT Pipeline Workflow - Prefect-based workflow orchestration"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to configuration YAML file"
    )
    
    parser.add_argument(
        "--data-root",
        type=str,
        help="Override data root path from config"
    )
    
    parser.add_argument(
        "--output-base",
        type=str,
        help="Override output base path from config"
    )
    
    parser.add_argument(
        "--slices",
        type=int,
        nargs="+",
        help="List of slice numbers to process (if not provided, auto-discover)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without running workflow"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Create Prefect deployment instead of running workflow"
    )
    
    parser.add_argument(
        "--deployment-name",
        type=str,
        default="oct-pipeline",
        help="Name for Prefect deployment (used with --deploy)"
    )
    
    parser.add_argument(
        "--work-pool",
        type=str,
        default="default",
        help="Work pool name for deployment (used with --deploy)"
    )
    
    parser.add_argument(
        "--schedule",
        type=str,
        help="Cron schedule for deployment (e.g., '0 0 * * *' for daily)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    try:
        if args.deploy:
            # Create deployment
            create_deployment(
                config_path=args.config,
                deployment_name=args.deployment_name,
                work_pool_name=args.work_pool,
                schedule=args.schedule
            )
        else:
            # Run workflow
            run_experiment(
                config_path=args.config,
                data_root_path=args.data_root,
                output_base_path=args.output_base,
                slice_numbers=args.slices,
                dry_run=args.dry_run
            )
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

