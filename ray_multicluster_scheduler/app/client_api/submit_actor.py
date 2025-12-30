"""
Client API for submitting actors to the ray multicluster scheduler.
"""

from typing import Callable, Any, Dict, List, Optional, Tuple, Type
import ray
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager

logger = get_logger(__name__)

# Global task lifecycle manager instance
_task_lifecycle_manager: Optional[Any] = None
# 标记是否已经尝试过初始化
_initialization_attempted = False

# 存储任务结果的字典
_task_results = {}


def initialize_scheduler(task_lifecycle_manager: TaskLifecycleManager):
    """Initialize the scheduler with a task lifecycle manager."""
    global _task_lifecycle_manager
    _task_lifecycle_manager = task_lifecycle_manager
    _task_lifecycle_manager.start()


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

    # 惰性初始化：使用全局配置文件路径初始化调度器
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import UnifiedScheduler
    config_file_path = UnifiedScheduler._config_file_path
    logger.info(f"Lazy initializing scheduler with config file: {config_file_path or 'default'}")
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
    task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)
    initialize_scheduler(task_lifecycle_manager)


def submit_actor(actor_class: Type, args: tuple = (), kwargs: dict = None,
                 resource_requirements: Dict[str, float] = None,
                 tags: List[str] = None, name: str = "",
                 preferred_cluster: Optional[str] = None) -> Tuple[str, Any]:
    """
    Submit an actor to the scheduler.

    Args:
        actor_class: The actor class to instantiate remotely
        args: Positional arguments for the actor constructor
        kwargs: Keyword arguments for the actor constructor
        resource_requirements: Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1})
        tags: List of tags to associate with the actor
        name: Optional name for the actor
        preferred_cluster: Optional preferred cluster name for actor execution

    Returns:
        A tuple containing (task_id, result) where task_id is the unique identifier
        for the submitted actor and result is either the actor instance (if created immediately)
        or the task_id (if queued for later creation).

    Note:
        This function now supports concurrent actor submissions. Multiple actors can be
        submitted simultaneously without interfering with each other's handles.
        Actors may be queued if cluster resources are temporarily unavailable.

    Raises:
        Exception: If the scheduler is not initialized or actor submission fails
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

    # Create actor description
    task_desc = TaskDescription(
        name=name,
        func_or_class=actor_class,
        args=args,
        kwargs=kwargs,
        resource_requirements=resource_requirements,
        tags=tags,
        is_actor=True,
        preferred_cluster=preferred_cluster
        # 注意：已移除runtime_env参数
    )

    # Submit actor to the lifecycle manager and get the future or task ID
    result = _task_lifecycle_manager.submit_task_and_get_future(task_desc)

    if result is None:
        raise RuntimeError(f"Failed to submit actor {task_desc.task_id}")

    logger.info(f"Submitted actor {task_desc.task_id} to scheduler")

    # Result could be either a future (for immediate execution) or task ID (for queued tasks)
    # Store the result for later access
    _task_results[task_desc.task_id] = result

    # If the result is the task ID, return it as both ID and result
    if isinstance(result, str) and result == task_desc.task_id:
        return task_desc.task_id, result
    else:
        # If we got a future back, return task ID and the future
        return task_desc.task_id, result


def get_actor_result(actor_id: str) -> Any:
    """
    Get the result of a previously submitted actor.

    Args:
        actor_id: The ID of the actor to get result for

    Returns:
        The actor instance, or None if actor not found or not completed
    """
    global _task_results
    result = _task_results.get(actor_id)
    # If the actor is queued (result is the actor_id itself), indicate that it's pending
    if result == actor_id:
        return f"Actor {actor_id} is queued and waiting for resources"
    return result


def get_actor_status(actor_id: str) -> str:
    """
    Get the status of a previously submitted actor.

    Args:
        actor_id: The ID of the actor to get status for

    Returns:
        The status of the actor (e.g., "QUEUED", "SUBMITTED", "RUNNING", "COMPLETED", "FAILED")
    """
    global _task_results
    if actor_id in _task_results:
        result = _task_results[actor_id]
        # If the result is the actor_id itself, the actor is queued
        if result == actor_id:
            return "QUEUED"
        else:
            return "COMPLETED"
    else:
        # 这里应该查询实际的任务状态，目前简化处理
        return "UNKNOWN"