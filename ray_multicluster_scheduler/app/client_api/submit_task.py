"""
Client API for submitting tasks to the ray multicluster scheduler.
"""

from typing import Callable, Any, Dict, List, Optional, Tuple
import ray
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)

# Global task lifecycle manager instance
_task_lifecycle_manager: Optional[Any] = None
# 标记是否已经尝试过初始化
_initialization_attempted = False

# 存储任务结果的字典
_task_results = {}


def initialize_scheduler(task_lifecycle_manager: Any):
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
    
    # 惰性初始化：使用默认配置初始化调度器
    logger.info("Lazy initializing scheduler with default configuration...")
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
    task_lifecycle_manager = initialize_scheduler_environment()
    initialize_scheduler(task_lifecycle_manager)


def submit_task(func: Callable, args: tuple = (), kwargs: dict = None,
                resource_requirements: Dict[str, float] = None,
                tags: List[str] = None, name: str = "",
                preferred_cluster: Optional[str] = None,
                runtime_env: Optional[Dict[str, Any]] = None) -> Tuple[str, Any]:
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
        runtime_env: Optional runtime environment configuration (e.g., {"conda": "k8s", "work_dir": "/path/to/work"})

    Returns:
        A tuple containing (task_id, result) where task_id is the unique identifier
        for the submitted task and result is the execution result.

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
        preferred_cluster=preferred_cluster,  # 新增：传递首选集群名称
        runtime_env=runtime_env  # 新增：传递运行时环境配置
    )

    # Submit task to the lifecycle manager and get the future
    future = _task_lifecycle_manager.submit_task_and_get_future(task_desc)

    if future is None:
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"Failed to submit task {task_desc.task_id}")

    logger.info(f"Submitted task {task_desc.task_id} to scheduler")

    # 等待并返回任务结果
    try:
        result = ray.get(future)
        logger.info(f"Task {task_desc.task_id} completed with result: {result}")
        # 存储任务结果供后续查询
        _task_results[task_desc.task_id] = result
        return task_desc.task_id, result
    except Exception as e:
        logger.error(f"Task {task_desc.task_id} failed with error: {e}")
        raise RuntimeError(f"Task {task_desc.task_id} failed: {e}")


def get_task_result(task_id: str) -> Any:
    """
    Get the result of a previously submitted task.

    Args:
        task_id: The ID of the task to get result for

    Returns:
        The result of the task execution, or None if task not found or not completed
    """
    global _task_results
    return _task_results.get(task_id)


def get_task_status(task_id: str) -> str:
    """
    Get the status of a previously submitted task.

    Args:
        task_id: The ID of the task to get status for

    Returns:
        The status of the task (e.g., "SUBMITTED", "RUNNING", "COMPLETED", "FAILED")
    """
    global _task_results
    if task_id in _task_results:
        return "COMPLETED"
    else:
        # 这里应该查询实际的任务状态，目前简化处理
        return "UNKNOWN"