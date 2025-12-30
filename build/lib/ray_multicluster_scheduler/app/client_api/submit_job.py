"""
Client API for submitting jobs to the ray multicluster scheduler.
"""

from typing import Dict, Optional, Any
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
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

    # If cluster_name is not provided, we need to determine which cluster the job was submitted to
    if not cluster_name:
        # For now, try all registered clusters to find the job
        for registered_cluster_name in scheduler.task_lifecycle_manager.connection_manager.list_registered_clusters():
            try:
                job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(registered_cluster_name)
                if job_client:
                    try:
                        # Try to get the job status from this cluster
                        status = job_client.get_job_status(job_id)
                        return status
                    except Exception:
                        # Job might not exist on this cluster, continue to next cluster
                        continue
            except Exception:
                continue
        # If job was not found on any cluster, return UNKNOWN
        return "UNKNOWN"
    else:
        # If cluster_name is provided, get the job status from that specific cluster
        job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
        if not job_client:
            raise ValueError(f"Cannot get JobSubmissionClient for cluster: {cluster_name}")

        return job_client.get_job_status(job_id)


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
    # 使用按需初始化的job client pool
    if cluster_name:
        try:
            job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
            if job_client:
                job_client.stop_job(job_id)
                logger.info(f"Successfully stopped job {job_id} on cluster {cluster_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to stop job {job_id} on cluster {cluster_name}: {e}")
    else:
        # 尝试在所有集群中查找并停止作业
        for registered_cluster_name in scheduler.task_lifecycle_manager.connection_manager.list_registered_clusters():
            try:
                job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(registered_cluster_name)
                if job_client:
                    job_client.stop_job(job_id)
                    logger.info(f"Successfully stopped job {job_id} on cluster {registered_cluster_name}")
                    return True
            except Exception as e:
                logger.debug(f"Failed to stop job {job_id} on cluster {registered_cluster_name}: {e}")
                continue
    return False

def wait_for_all_jobs(job_ids: list, check_interval: int = 5, timeout: Optional[int] = None):
    """
    轮询等待所有jobs完成

    Args:
        job_ids: 要等待的job ID列表
        check_interval: 检查间隔（秒）
        timeout: 超时时间（秒），None表示无超时

    Returns:
        bool: 所有job执行成功时返回True

    Raises:
        RuntimeError: 任一job执行失败时抛出异常，报告失败的job ID
    """
    _ensure_scheduler_initialized()
    import time
    from datetime import datetime, timedelta

    print(f"\n开始轮询 {len(job_ids)} 个job的状态，检查间隔: {check_interval}秒")

    completed_jobs = set()
    total_jobs = len(job_ids)
    start_time = datetime.now()

    while len(completed_jobs) < total_jobs:
        # 检查是否超时
        if timeout is not None:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed >= timeout:
                remaining_jobs = [job_id for job_id in job_ids if job_id not in completed_jobs]
                raise TimeoutError(f"等待jobs完成超时，仍有 {len(remaining_jobs)} 个job未完成: {remaining_jobs}")

        still_running = []

        for job_id in job_ids:
            if job_id in completed_jobs:
                continue  # 已完成的job跳过

            try:
                status = get_job_status(job_id)
                print(f"Job {job_id} 状态: {status}")

                if status == "SUCCEEDED":  # 成功完成
                    completed_jobs.add(job_id)
                    print(f"Job {job_id} 已成功完成")
                elif status in ["FAILED", "STOPPED"]:  # 失败或被停止
                    print(f"Job {job_id} 执行失败，状态: {status}")
                    raise RuntimeError(f"Job {job_id} 执行失败，状态: {status}")
                else:  # 运行中或其他状态
                    still_running.append((job_id, status))

            except Exception as e:
                print(f"获取job {job_id} 状态失败: {e}")
                raise RuntimeError(f"无法获取job {job_id} 的状态: {e}")

        if still_running:
            print(f"仍有 {len(still_running)} 个job在运行，继续等待...")
            print(f"  运行中的job: {[job_id for job_id, _ in still_running]}")
        else:
            print("所有job都已完成！")

        if len(completed_jobs) < total_jobs:
            print(f"等待 {check_interval} 秒后再次检查...")
            time.sleep(check_interval)

    print(f"\n所有 {total_jobs} 个job都已成功完成！")
    return True