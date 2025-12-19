"""
Result collector that waits for and collects results from Ray tasks.
"""

import ray
from typing import Any
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import TaskSubmissionError

logger = get_logger(__name__)


class ResultCollector:
    """Collects results from Ray tasks and handles exceptions."""

    def __init__(self):
        pass

    def collect_result(self, future: ray.ObjectRef, timeout: float = None) -> Any:
        """Wait for and return the result of a Ray task."""
        try:
            # Wait for the result
            result = ray.get(future, timeout=timeout)
            logger.info("Successfully collected task result")
            return result
        except ray.exceptions.RayTaskError as e:
            logger.error(f"Task execution failed: {e}")
            # Re-raise the underlying exception
            raise e.as_instanceof_cause()
        except ray.exceptions.GetTimeoutError:
            logger.error("Task timed out while waiting for result")
            raise TaskSubmissionError("Task timed out while waiting for result")
        except Exception as e:
            logger.error(f"Unexpected error while collecting task result: {e}")
            raise TaskSubmissionError(f"Unexpected error while collecting task result: {e}")