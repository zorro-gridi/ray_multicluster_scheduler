"""
Client API for submitting jobs to the ray multicluster scheduler.
"""

from typing import Dict, Optional, Any
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler

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


def submit_job(
    entrypoint: str,
    runtime_env: Optional[Dict] = None,
    job_id: Optional[str] = None,
    metadata: Optional[Dict] = None,
    submission_id: Optional[str] = None,
    preferred_cluster: Optional[str] = None,
    resource_requirements: Optional[Dict[str, float]] = None,
    tags: Optional[list] = None
) -> str:
    """
    Submit a job to the multicluster scheduler using JobSubmissionClient.

    This method provides an interface for submitting jobs to the scheduler.
    The scheduler will automatically handle cluster selection, resource allocation,
    and job execution across available Ray clusters using JobSubmissionClient.

    NOTE: The entrypoint must contain absolute paths for all files. The system will
    automatically convert paths based on the target cluster's configuration.

    Args:
        entrypoint (str): The command to run in the job (e.g., "python /absolute/path/train.py")
                          Must contain absolute paths for all files.
        runtime_env (Dict, optional): Runtime environment for the job.
                                   If not provided, will use cluster's default runtime_env from config.
        job_id (str, optional): Unique identifier for the job
        metadata (Dict, optional): Metadata to associate with the job
        submission_id (str, optional): Submission ID for tracking
        preferred_cluster (str, optional): Preferred cluster name for job execution
        resource_requirements (Dict[str, float], optional): Resource requirements for the job
        tags (list, optional): List of tags to associate with the job

    Returns:
        str: Job ID of the submitted job
    """
    # 确保调度器已初始化
    _ensure_scheduler_initialized()

    # 验证entrypoint包含绝对路径
    import re
    # 提取entrypoint中的文件路径部分
    # 例如: "python /absolute/path/script.py --arg value" -> "/absolute/path/script.py"
    path_match = re.search(r'\bpython\s+(/[^\s]+)', entrypoint, re.IGNORECASE)
    if path_match:
        script_path = path_match.group(1)
        if not script_path.startswith('/'):
            raise ValueError(f"Entrypoint must contain absolute path for script. Found relative path: {script_path}")
    else:
        # 如果不是python命令，检查是否包含绝对路径
        # 提取可能的路径（以/开头的单词）
        possible_paths = re.findall(r'(/[^\s]+)', entrypoint)
        if possible_paths:
            # 检查是否有路径不是以/开头的
            for path in possible_paths:
                if not path.startswith('/'):
                    raise ValueError(f"Entrypoint must contain absolute paths. Found relative path: {path}")

    # 如果没有提供runtime_env，尝试从集群配置中获取默认的runtime_env
    if runtime_env is None:
        # 获取集群信息以确定默认runtime_env
        cluster_info = _task_lifecycle_manager.cluster_monitor.get_all_cluster_info()

        # 如果指定了首选集群，使用该集群的runtime_env
        if preferred_cluster and preferred_cluster in cluster_info:
            cluster_metadata = cluster_info[preferred_cluster]['metadata']
            if hasattr(cluster_metadata, 'runtime_env'):
                runtime_env = cluster_metadata.runtime_env
        # 否则，如果没有指定首选集群，可以使用第一个可用集群的runtime_env作为默认值
        elif not preferred_cluster and cluster_info:
            # 获取第一个集群的配置作为默认值
            first_cluster_name = next(iter(cluster_info))
            cluster_metadata = cluster_info[first_cluster_name]['metadata']
            if hasattr(cluster_metadata, 'runtime_env'):
                runtime_env = cluster_metadata.runtime_env

    # 创建Job描述对象
    job_desc = JobDescription(
        job_id=job_id,
        entrypoint=entrypoint,
        runtime_env=runtime_env,
        metadata=metadata or {},
        submission_id=submission_id,
        preferred_cluster=preferred_cluster,
        resource_requirements=resource_requirements or {},
        tags=tags or []
    )

    # 使用调度器提交Job
    job_id_result = _task_lifecycle_manager.submit_job(job_desc)
    return job_id_result


def get_job_status(job_id: str, cluster_name: Optional[str] = None) -> Any:
    """
    Get the status of a submitted job.

    Args:
        job_id (str): The ID of the job to check
        cluster_name (str, optional): The cluster where the job was submitted

    Returns:
        Any: Job status information
    """
    _ensure_scheduler_initialized()
    scheduler = get_unified_scheduler()
    return scheduler.task_lifecycle_manager.cluster_monitor.cluster_manager.health_status.get(job_id)


def stop_job(job_id: str, cluster_name: Optional[str] = None) -> bool:
    """
    Stop a running job.

    Args:
        job_id (str): The ID of the job to stop
        cluster_name (str, optional): The cluster where the job is running

    Returns:
        bool: True if the job was stopped successfully, False otherwise
    """
    _ensure_scheduler_initialized()
    scheduler = get_unified_scheduler()
    # 获取Job客户端并停止作业
    if scheduler.task_lifecycle_manager.connection_manager.job_client_pool:
        if cluster_name:
            job_client = scheduler.task_lifecycle_manager.connection_manager.job_client_pool.get_client(cluster_name)
            if job_client:
                job_client.stop_job(job_id)
                return True
        else:
            # 尝试在所有集群中查找并停止作业
            for cluster_name in scheduler.task_lifecycle_manager.connection_manager.list_registered_clusters():
                job_client = scheduler.task_lifecycle_manager.connection_manager.job_client_pool.get_client(cluster_name)
                if job_client:
                    try:
                        job_client.stop_job(job_id)
                        return True
                    except Exception:
                        continue
    return False