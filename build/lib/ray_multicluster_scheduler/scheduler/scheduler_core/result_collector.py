"""
Result collector that waits for and collects results from Ray tasks.
"""

import ray
import asyncio
from typing import Any, Dict, Optional, Union
from concurrent.futures import ThreadPoolExecutor
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import TaskSubmissionError

logger = get_logger(__name__)


class ResultCollector:
    """Collects results from Ray tasks and handles exceptions."""

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=10)

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
        except ConnectionError as e:
            logger.error(f"Connection error while collecting task result: {e}")
            raise TaskSubmissionError(f"Connection error while collecting task result: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while collecting task result: {e}")
            raise TaskSubmissionError(f"Unexpected error while collecting task result: {e}")

    async def collect_result_async(self, future: ray.ObjectRef, timeout: float = None) -> Any:
        """Asynchronously wait for and return the result of a Ray task."""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                lambda: ray.get(future, timeout=timeout)
            )
            logger.info("Successfully collected task result asynchronously")
            return result
        except ray.exceptions.RayTaskError as e:
            logger.error(f"Task execution failed: {e}")
            # Re-raise the underlying exception
            raise e.as_instanceof_cause()
        except ray.exceptions.GetTimeoutError:
            logger.error("Task timed out while waiting for result")
            raise TaskSubmissionError("Task timed out while waiting for result")
        except ConnectionError as e:
            logger.error(f"Connection error while collecting task result: {e}")
            raise TaskSubmissionError(f"Connection error while collecting task result: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while collecting task result: {e}")
            raise TaskSubmissionError(f"Unexpected error while collecting task result: {e}")

    def is_task_ready(self, future: ray.ObjectRef) -> bool:
        """Check if a task result is ready without blocking."""
        try:
            ready, _ = ray.wait([future], timeout=0)
            return len(ready) > 0
        except Exception as e:
            logger.error(f"Error checking task readiness: {e}")
            return False

    async def wait_for_tasks(self, futures: list, num_returns: int = 1, timeout: float = None) -> tuple:
        """Wait for multiple tasks to complete.

        Args:
            futures: List of Ray ObjectRef futures
            num_returns: Number of tasks that must complete before returning
            timeout: Maximum time to wait in seconds

        Returns:
            Tuple of (ready_futures, remaining_futures)
        """
        try:
            loop = asyncio.get_event_loop()
            ready_futures, remaining_futures = await loop.run_in_executor(
                self.executor,
                lambda: ray.wait(futures, num_returns=num_returns, timeout=timeout)
            )
            logger.info(f"Waiting for tasks completed: {len(ready_futures)} ready, {len(remaining_futures)} remaining")
            return ready_futures, remaining_futures
        except Exception as e:
            logger.error(f"Error waiting for tasks: {e}")
            raise TaskSubmissionError(f"Error waiting for tasks: {e}")

    def get_task_status(self, future: ray.ObjectRef) -> Dict[str, Any]:
        """Get the status of a task without blocking.

        Args:
            future: Ray ObjectRef representing the task

        Returns:
            Dictionary with status information
        """
        try:
            is_ready = self.is_task_ready(future)
            return {
                "ready": is_ready,
                "object_ref": future,
                "status": "COMPLETED" if is_ready else "PENDING"
            }
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return {
                "ready": False,
                "object_ref": future,
                "status": "ERROR",
                "error": str(e)
            }

    async def get_results_batch_async(self, futures: list, timeout_per_task: float = None) -> list:
        """Asynchronously collect results for a batch of tasks.

        Args:
            futures: List of Ray ObjectRef futures
            timeout_per_task: Timeout for each individual task

        Returns:
            List of results in the same order as the input futures
        """
        results = []
        for future in futures:
            result = await self.collect_result_async(future, timeout=timeout_per_task)
            results.append(result)
        return results


class AsyncResultCollector:
    """An async wrapper for result collection that ensures proper Ray context."""

    def __init__(self, ray_address: str = None):
        self.result_collector = ResultCollector()
        self.ray_address = ray_address

    async def collect_with_context(self, future: ray.ObjectRef, timeout: float = None) -> Any:
        """Collect result with proper Ray context handling."""
        # Ensure Ray is initialized
        if not ray.is_initialized():
            logger.warning("Ray is not initialized, initializing with specified address or local mode")
            if self.ray_address:
                ray.init(address=self.ray_address, ignore_reinit_error=True)
            else:
                ray.init(ignore_reinit_error=True)

        try:
            return await self.result_collector.collect_result_async(future, timeout)
        except Exception as e:
            logger.error(f"Error collecting result with context: {e}")
            raise
        finally:
            # Don't shutdown Ray if it was already initialized before this call
            # We only shutdown if we initialized it in this method
            pass

    def get_task_status_with_context(self, future: ray.ObjectRef) -> Dict[str, Any]:
        """Get task status with proper Ray context handling."""
        # Ensure Ray is initialized
        if not ray.is_initialized():
            logger.warning("Ray is not initialized, initializing with specified address or local mode")
            if self.ray_address:
                ray.init(address=self.ray_address, ignore_reinit_error=True)
            else:
                ray.init(ignore_reinit_error=True)

        try:
            return self.result_collector.get_task_status(future)
        except Exception as e:
            logger.error(f"Error getting task status with context: {e}")
            raise


class ClusterResultCollector:
    """A result collector that works with multi-cluster scheduler context."""

    def __init__(self, connection_manager=None):
        self.result_collector = ResultCollector()
        self.connection_manager = connection_manager

    async def collect_from_cluster(self, future: ray.ObjectRef, cluster_name: str = None, timeout: float = None) -> Any:
        """Collect result from a specific cluster."""
        try:
            # If connection manager is provided, ensure connection to the right cluster
            if self.connection_manager and cluster_name:
                success = self.connection_manager.ensure_cluster_connection(cluster_name)
                if not success:
                    logger.warning(f"Could not ensure connection to cluster {cluster_name}")

            return await self.result_collector.collect_result_async(future, timeout)
        except Exception as e:
            logger.error(f"Error collecting result from cluster {cluster_name}: {e}")
            raise

    def get_task_status_from_cluster(self, future: ray.ObjectRef, cluster_name: str = None) -> Dict[str, Any]:
        """Get task status from a specific cluster context."""
        try:
            # If connection manager is provided, ensure connection to the right cluster
            if self.connection_manager and cluster_name:
                success = self.connection_manager.ensure_cluster_connection(cluster_name)
                if not success:
                    logger.warning(f"Could not ensure connection to cluster {cluster_name}")

            return self.result_collector.get_task_status(future)
        except Exception as e:
            logger.error(f"Error getting task status from cluster {cluster_name}: {e}")
            raise