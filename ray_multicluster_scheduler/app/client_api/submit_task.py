"""Submit tasks to the multicluster scheduler."""

import ray
import time
import signal
import sys
from typing import Callable, Dict, List, Optional, Tuple, Any
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    get_unified_scheduler, initialize_scheduler_environment
)

logger = get_logger(__name__)

# Global state for the task scheduler
_task_lifecycle_manager = None
_initialization_attempted = False


def _ensure_scheduler_initialized():
    """Ensure the scheduler is initialized, using lazy initialization if needed."""
    global _task_lifecycle_manager, _initialization_attempted

    # 如果调度器已经初始化，直接返回
    if _task_lifecycle_manager is not None:
        return

    # 如果已经尝试过初始化但失败了，不再尝试
    if _initialization_attempted:
        raise RuntimeError("Scheduler initialization previously failed. Call initialize_scheduler_environment() first.")

    # 标记已尝试初始化
    _initialization_attempted = True

    # 惰性初始化：使用默认配置初始化调度器
    logger.info("Lazy initializing scheduler with default configuration...")
    task_lifecycle_manager = initialize_scheduler_environment()
    initialize_scheduler(task_lifecycle_manager)


def submit_task(func: Callable, args: tuple = (), kwargs: dict = None,
                resource_requirements: Dict[str, float] = None,
                tags: List[str] = None, name: str = "",
                preferred_cluster: Optional[str] = None) -> Tuple[str, Any]:
    """
    Submit a task to the scheduler.

    This function submits a function to be executed on one of the available Ray clusters.
    The task will be scheduled based on resource availability and cluster preferences.

    Args:
        func (Callable): The function to execute remotely
        args (tuple, optional): Arguments to pass to the function. Defaults to ().
        kwargs (dict, optional): Keyword arguments to pass to the function. Defaults to None.
        resource_requirements (Dict[str, float], optional):
            Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1}).
            Defaults to None.
        tags (List[str], optional): List of tags to associate with the task. Defaults to None.
        name (str, optional): Optional name for the task. Defaults to "".
        preferred_cluster (str, optional): Preferred cluster name for task execution.
            If specified cluster is unavailable, scheduler will fallback to other clusters.

    Returns:
        Tuple[str, Any]: A tuple containing (task_id, result) where task_id is the
        unique identifier for the submitted task and result is the execution result.

    Note:
        This function now supports concurrent task submissions. Multiple tasks can be
        submitted simultaneously without interfering with each other.

    Raises:
        Exception: If the scheduler is not initialized or task submission fails
    """
    global _task_lifecycle_manager

    # 确保调度器已初始化
    if _task_lifecycle_manager is None:
        _ensure_scheduler_initialized()

    # 如果仍然没有初始化，则抛出异常
    if _task_lifecycle_manager is None:
        raise RuntimeError("Scheduler not initialized. Call initialize_scheduler_environment() first.")

    if kwargs is None:
        kwargs = {}

    if resource_requirements is None:
        resource_requirements = {}

    if tags is None:
        tags = []

    # Create task description
    task_desc = TaskDescription(
        name=name,
        func_or_class=func,
        args=args,
        kwargs=kwargs,
        resource_requirements=resource_requirements,
        tags=tags,
        is_actor=False,
        preferred_cluster=preferred_cluster
        # 注意：已移除runtime_env参数
    )

    # Submit task to the lifecycle manager and get the future
    future = _task_lifecycle_manager.submit_task_and_get_future(task_desc)

    if future is None:
        raise RuntimeError(f"Failed to submit task {task_desc.task_id}")

    logger.info(f"Submitted task {task_desc.task_id} to scheduler")

    # Wait for the result and return it
    try:
        result = ray.get(future)
        return task_desc.task_id, result
    except Exception as e:
        logger.error(f"Error getting result for task {task_desc.task_id}: {e}")
        raise


def initialize_scheduler(task_lifecycle_manager):
    """
    Initialize the submit_task module with a task lifecycle manager.

    This function must be called before submitting any tasks. It sets up the
    global state needed for task submission.

    Args:
        task_lifecycle_manager: The task lifecycle manager to use for scheduling
    """
    global _task_lifecycle_manager
    _task_lifecycle_manager = task_lifecycle_manager
    logger.info("✅ submit_task scheduler initialized")