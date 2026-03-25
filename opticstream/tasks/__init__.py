"""
Tasks module for OCT pipeline workflow.

This module contains all task definitions organized by functionality.

Note: Most tasks have been consolidated into their respective flow files.
Only shared/common tasks are exported from this module.
"""

# Common/shared tasks (used by multiple flows)
from .common_tasks import (
    upload_to_dandi_task,
    upload_to_linc_batch_task,
    upload_to_linc_task,
)

__all__ = [
    "upload_to_dandi_task",
    "upload_to_linc_task",
    "upload_to_linc_batch_task",
]
