"""
Client API for submitting tasks to the ray multicluster scheduler.
"""

import threading
from typing import Callable, Any, Dict, List, Optional, Tuple
import ray
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.common.context_manager import ClusterContextManager

logger = get_logger(__name__)

# Global task lifecycle manager instance
_task_lifecycle_manager: Optional[Any] = None
# 标记是否已经尝试过初始化
_initialization_attempted = False

# 存储任务结果的字典
_task_results = {}


def get_current_cluster_context() -> Optional[str]:
    """获取当前线程的集群上下文"""
    return ClusterContextManager.get_current_cluster()


def set_current_cluster_context(cluster_name: Optional[str]):
    """设置当前线程的集群上下文"""
    ClusterContextManager.set_current_cluster(cluster_name)


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


def submit_task(func: Callable, args: tuple = (), kwargs: dict = None,
                resource_requirements: Dict[str, float] = None,
                tags: List[str] = None, name: str = "",
                preferred_cluster: Optional[str] = None) -> Tuple[str, Any]:
    """
    Submit a task to the scheduler.

    Args:
        func: The function to execute remotely
        args: Positional arguments for the function
        kwargs: Keyword arguments for the function
        resource_requirements: Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1})
        tags: List of tags to associate with the task
        name: Optional name for the task
        preferred_cluster: Optional preferred cluster name for task execution

    Returns:
        A tuple containing (task_id, result) where task_id is the unique identifier
        for the submitted task and result is either the task result (if executed immediately)
        or the task_id (if queued for later execution).

    Note:
        This function now supports concurrent task submissions. Multiple tasks can be
        submitted simultaneously without interfering with each other.
        Tasks may be queued if cluster resources are temporarily unavailable.

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

    # 检查是否有指定的首选集群，如果没有，则尝试从当前集群上下文继承
    final_preferred_cluster = preferred_cluster
    if preferred_cluster is None:
        current_context = get_current_cluster_context()
        if current_context:
            final_preferred_cluster = current_context
        else:
            # 如果没有在上下文管理器中找到集群，尝试从环境变量获取
            import os
            env_cluster = os.environ.get('RAY_MULTICLUSTER_CURRENT_CLUSTER')
            if env_cluster:
                final_preferred_cluster = env_cluster

    # Create task description
    task_desc = TaskDescription(
        name=name,
        func_or_class=func,
        args=args,
        kwargs=kwargs,
        resource_requirements=resource_requirements,
        tags=tags,
        is_actor=False,
        is_top_level_task=False,  # task作为子任务，不受40秒限制
        preferred_cluster=final_preferred_cluster
        # 注意：已移除runtime_env参数
    )

    # Submit task to the lifecycle manager and get the future or task ID
    result = _task_lifecycle_manager.submit_task_and_get_future(task_desc)

    if result is None:
        raise RuntimeError(f"Failed to submit task {task_desc.task_id}")

    logger.info(f"Submitted task {task_desc.task_id} to scheduler")

    # Result could be either a future (for immediate execution) or task ID (for queued tasks)
    # Store the result for later access
    _task_results[task_desc.task_id] = result

    # If the result is the task ID, return it as both ID and result
    if isinstance(result, str) and result == task_desc.task_id:
        return task_desc.task_id, result
    else:
        # If we got a future back, return task ID and the future
        return task_desc.task_id, result


def get_task_result(task_id: str) -> Any:
    """
    Get the result of a previously submitted task.

    Args:
        task_id: The ID of the task to get result for

    Returns:
        The task result, or None if task not found or not completed
    """
    global _task_results
    result = _task_results.get(task_id)
    # If the task is queued (result is the task_id itself), indicate that it's pending
    if result == task_id:
        return f"Task {task_id} is queued and waiting for resources"
    return result


def get_task_status(task_id: str) -> str:
    """
    Get the status of a previously submitted task.

    Args:
        task_id: The ID of the task to get status for

    Returns:
        The status of the task (e.g., "QUEUED", "SUBMITTED", "RUNNING", "COMPLETED", "FAILED")
    """
    global _task_results
    if task_id in _task_results:
        result = _task_results[task_id]
        # If the result is the task_id itself, the task is queued
        if result == task_id:
            return "QUEUED"
        else:
            return "COMPLETED"
    else:
        # 这里应该查询实际的任务状态，目前简化处理
        return "UNKNOWN"