"""
Client API for submitting jobs to the ray multicluster scheduler.
"""

from typing import Dict, Optional, Any
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager

import signal
import threading

logger = get_logger(__name__)

# Global task lifecycle manager instance
_task_lifecycle_manager: Optional[Any] = None
# 标记是否已经尝试过初始化
_initialization_attempted = False

# 存储任务结果的字典
_task_results = {}

# 全局中断标志，用于检测中断信号
_interrupted = threading.Event()

# 全局作业ID跟踪器
_submitted_job_ids = []


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


def _setup_signal_handlers():
    """设置信号处理器来捕获中断信号"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, setting interrupt flag...")
        _interrupted.set()

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def _check_interrupt():
    """检查是否收到中断信号"""
    if _interrupted.is_set():
        logger.info("Interrupt signal received, stopping wait operation...")
        raise KeyboardInterrupt("Operation interrupted by user")


def _clear_interrupt():
    """清除中断标志"""
    if _interrupted.is_set():
        _interrupted.clear()
        logger.debug("Interrupt flag cleared")


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
        str: Submission ID of the submitted job (use this for get_job_status)
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

    # 不预先设置runtime_env，让Dispatcher在调度决策后从目标集群获取正确的配置
    # 这样可以确保作业被调度到目标集群时使用该集群的正确配置
    # 如果没有提供runtime_env，将它设置为None，让Dispatcher使用目标集群的配置
    if runtime_env is None:
        # 无论是否指定了preferred_cluster，如果没有提供runtime_env，都设置为None
        # 让Dispatcher在调度决策后获取目标集群的配置
        runtime_env = None

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
    submission_id_result = _task_lifecycle_manager.submit_job(job_desc)

    # 将作业ID添加到全局跟踪器中
    _submitted_job_ids.append(submission_id_result)

    # 添加调试日志
    logger.info(f"任务提交完成，返回 submission_id: {submission_id_result}")
    logger.info(f"请使用此 submission_id 查询任务状态: get_job_status('{submission_id_result}')")

    return submission_id_result


def get_job_status(submission_id: str, cluster_name: str) -> Any:
    """
    Get the status of a submitted job using submission_id.

    Args:
        submission_id (str): The submission ID returned by submit_job()
        cluster_name (str): The cluster where the job was submitted

    Returns:
        Any: Job status information
    """
    _ensure_scheduler_initialized()
    scheduler = get_unified_scheduler()

    logger.info(f"查询任务状态，submission_id: {submission_id}, cluster: {cluster_name}")

    # Get the job status from the specific cluster
    job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
    if not job_client:
        raise ValueError(f"Cannot get JobSubmissionClient for cluster: {cluster_name}")

    logger.info(f"从指定集群 {cluster_name} 查询任务 {submission_id}")
    status = job_client.get_job_status(submission_id)
    logger.info(f"任务 {submission_id} 在集群 {cluster_name} 的状态: {status}")
    return status


def stop_job(submission_id: str, cluster_name: str) -> bool:
    """
    Stop a running job.

    Args:
        submission_id (str): The submission ID of the job to stop
        cluster_name (str): The cluster where the job is running

    Returns:
        bool: True if the job was stopped successfully, False otherwise
    """
    _ensure_scheduler_initialized()
    scheduler = get_unified_scheduler()

    logger.info(f"停止任务，submission_id: {submission_id}, cluster: {cluster_name}")

    # 获取Job客户端并停止作业
    # 使用按需初始化的job client pool
    try:
        job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
        if job_client:
            job_client.stop_job(submission_id)
            logger.info(f"Successfully stopped job {submission_id} on cluster {cluster_name}")
            return True
    except Exception as e:
        logger.error(f"Failed to stop job {submission_id} on cluster {cluster_name}: {e}")
    return False

def wait_for_all_jobs(submission_ids: list, check_interval: int = 5, timeout: Optional[int] = None):
    """
    轮询等待所有jobs完成

    Args:
        submission_ids: 要等待的submission ID列表
        check_interval: 检查间隔（秒）
        timeout: 超时时间（秒），None表示无超时

    Returns:
        bool: 所有job执行成功时返回True

    Raises:
        RuntimeError: 任一job执行失败时抛出异常，报告失败的submission ID
        KeyboardInterrupt: 用户中断操作时抛出
    """
    _ensure_scheduler_initialized()
    import time
    from datetime import datetime, timedelta

    # 设置信号处理器
    _setup_signal_handlers()

    print(f"\n开始轮询 {len(submission_ids)} 个job的状态，检查间隔: {check_interval}秒")

    completed_jobs = set()
    total_jobs = len(submission_ids)
    start_time = datetime.now()

    while len(completed_jobs) < total_jobs:
        # 检查是否收到中断信号
        _check_interrupt()

        # 检查是否超时
        if timeout is not None:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed >= timeout:
                remaining_jobs = [submission_id for submission_id in submission_ids if submission_id not in completed_jobs]
                raise TimeoutError(f"等待jobs完成超时，仍有 {len(remaining_jobs)} 个job未完成: {remaining_jobs}")

        still_running = []

        for submission_id in submission_ids:
            # 检查中断信号
            _check_interrupt()

            if submission_id in completed_jobs:
                continue  # 已完成的job跳过

            try:
                # 首先尝试从调度器获取作业的集群信息
                _ensure_scheduler_initialized()
                scheduler = get_unified_scheduler()

                cluster_name = None
                if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                    cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(submission_id)

                if cluster_name:
                    status = get_job_status(submission_id, cluster_name)
                else:
                    # 如果无法获取集群信息，则需要遍历所有集群查找作业
                    status = _get_job_status_from_all_clusters(submission_id)

                print(f"Job {submission_id} 状态: {status}")

                if status == "SUCCEEDED":  # 成功完成
                    completed_jobs.add(submission_id)
                    print(f"Job {submission_id} 已成功完成")
                elif status in ["FAILED", "STOPPED"]:  # 失败或被停止
                    print(f"Job {submission_id} 执行失败，状态: {status}")
                    # 尝试获取失败作业的详细信息
                    try:
                        if cluster_name:
                            job_info = get_job_info(submission_id, cluster_name)
                            if job_info:
                                print(f"Job {submission_id} 详细信息: {job_info}")
                    except Exception as info_e:
                        logger.debug(f"无法获取作业 {submission_id} 的详细信息: {info_e}")
                    raise RuntimeError(f"Job {submission_id} 执行失败，状态: {status}")
                elif status == "UNKNOWN":  # 状态未知，可能作业已不存在
                    print(f"Job {submission_id} 状态未知，可能作业已不存在或无法找到")
                    # 记录为失败状态
                    raise RuntimeError(f"Job {submission_id} 状态未知，可能作业已不存在")
                else:  # 运行中或其他状态
                    still_running.append((submission_id, status))

            except KeyboardInterrupt:
                # 如果收到中断信号，清理中断标志并重新抛出
                _interrupted.clear()
                logger.info("等待操作被用户中断")
                raise
            except Exception as e:
                print(f"获取job {submission_id} 状态失败: {e}")
                raise RuntimeError(f"无法获取job {submission_id} 的状态: {e}")

        if still_running:
            print(f"仍有 {len(still_running)} 个job在运行，继续等待...")
            print(f"  运行中的job: {[submission_id for submission_id, _ in still_running]}")
        else:
            print("所有job都已完成！")

        if len(completed_jobs) < total_jobs:
            print(f"等待 {check_interval} 秒后再次检查...")
            # 使用更安全的sleep，定期检查中断信号
            slept = 0
            while slept < check_interval and not _interrupted.is_set():
                time.sleep(min(0.5, check_interval - slept))
                slept += 0.5
            # 检查是否收到中断信号
            _check_interrupt()

    print(f"\n所有 {total_jobs} 个job都已成功完成！")
    return True


def get_job_info(submission_id: str, cluster_name: str) -> Any:
    """
    Get detailed information about a submitted job using submission_id.

    Args:
        submission_id (str): The submission ID returned by submit_job()
        cluster_name (str): The cluster where the job was submitted

    Returns:
        Any: Job information including status, logs, etc.
    """
    _ensure_scheduler_initialized()
    scheduler = get_unified_scheduler()

    logger.debug(f"查询任务详细信息，submission_id: {submission_id}, cluster: {cluster_name}")

    # Get the job info from the specific cluster
    job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
    if not job_client:
        raise ValueError(f"Cannot get JobSubmissionClient for cluster: {cluster_name}")

    logger.debug(f"从指定集群 {cluster_name} 查询任务 {submission_id} 详细信息")
    try:
        job_info = job_client.get_job_info(submission_id)
        logger.debug(f"任务 {submission_id} 在集群 {cluster_name} 的详细信息: {job_info}")
        return job_info
    except AttributeError:
        # get_job_info might not be available, try to get logs instead
        try:
            logs = job_client.get_job_logs(submission_id)
            logger.debug(f"获取到任务 {submission_id} 的日志")
            return {"logs": logs, "status": job_client.get_job_status(submission_id)}
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"任务 {submission_id} 在集群 {cluster_name} 获取详细信息失败: {e}")
        return None


def _get_job_status_from_all_clusters(submission_id: str) -> str:
    """
    在所有集群中查找作业状态，当无法确定作业提交到哪个集群时使用。

    Args:
        submission_id (str): The submission ID to look for

    Returns:
        str: Job status or 'UNKNOWN' if not found
    """
    _ensure_scheduler_initialized()
    scheduler = get_unified_scheduler()

    # Try all registered clusters to find the job
    for registered_cluster_name in scheduler.task_lifecycle_manager.connection_manager.list_registered_clusters():
        try:
            job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(registered_cluster_name)
            if job_client:
                try:
                    # Try to get the job status from this cluster using submission_id
                    logger.debug(f"尝试从集群 {registered_cluster_name} 查询任务 {submission_id}")
                    status = job_client.get_job_status(submission_id)
                    logger.info(f"在集群 {registered_cluster_name} 找到任务 {submission_id}，状态: {status}")
                    return status
                except Exception as e:
                    # Job might not exist on this cluster, continue to next cluster
                    logger.debug(f"集群 {registered_cluster_name} 中未找到任务 {submission_id}: {e}")
                    continue
        except Exception as e:
            logger.debug(f"无法连接到集群 {registered_cluster_name}: {e}")
            continue

    # If job was not found on any cluster, return UNKNOWN
    logger.warning(f"在所有集群中都未找到任务 {submission_id}")
    return "UNKNOWN"


def graceful_shutdown_on_keyboard_interrupt(func):
    """
    装饰器：自动为用户函数添加键盘中断优雅关闭功能
    当用户按Ctrl+C时，自动停止所有已提交的Ray作业
    """
    import functools
    import traceback

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # 用于存储提交的作业ID
        original_job_ids = _submitted_job_ids.copy()  # 保存当前作业ID列表的副本

        def signal_handler(signum, frame):
            """信号处理器，用于优雅地停止远程作业"""
            print(f"\n收到信号 {signum}，正在停止远程作业...")

            # 获取当前新增的作业ID
            current_job_ids = [job_id for job_id in _submitted_job_ids if job_id not in original_job_ids]

            # 停止所有新增的作业
            for job_id in current_job_ids:
                try:
                    scheduler = get_unified_scheduler()

                    cluster_name = None
                    if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                        cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

                    if not cluster_name:
                        # 如果无法确定集群，尝试从所有集群查找
                        from ray_multicluster_scheduler.control_plane.config import ConfigManager
                        config_manager = ConfigManager()
                        cluster_configs = config_manager.get_cluster_configs()

                        for cluster_config in cluster_configs:
                            try:
                                job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_config.name)
                                if job_client:
                                    try:
                                        status = job_client.get_job_status(job_id)
                                        if status in ['PENDING', 'RUNNING', 'STOPPED', 'FAILED', 'SUCCEEDED']:
                                            cluster_name = cluster_config.name
                                            print(f"在集群 {cluster_name} 上找到作业 {job_id}")
                                            break
                                    except Exception:
                                        continue
                            except Exception:
                                continue

                    if cluster_name:
                        print(f"正在停止集群 {cluster_name} 上的作业 {job_id}...")
                        success = stop_job(job_id, cluster_name)
                        if success:
                            print(f"成功发送停止命令到作业 {job_id}")
                        else:
                            print(f"停止作业 {job_id} 失败")
                    else:
                        print(f"无法确定作业 {job_id} 所在的集群")

                except Exception as e:
                    print(f"停止作业 {job_id} 时出错: {e}")
                    import traceback
                    traceback.print_exc()

            print("中断处理完成，退出程序")
            import sys
            sys.exit(0)

        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # 执行用户函数
            result = func(*args, **kwargs)
            return result
        except KeyboardInterrupt:
            signal_handler(signal.SIGINT, None)
        except Exception as e:
            print(f"执行用户函数时出现错误: {e}")
            traceback.print_exc()
            # 发生异常时也停止新增的作业
            current_job_ids = [job_id for job_id in _submitted_job_ids if job_id not in original_job_ids]
            for job_id in current_job_ids:
                try:
                    scheduler = get_unified_scheduler()
                    cluster_name = None
                    if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                        cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

                    if cluster_name:
                        stop_job(job_id, cluster_name)
                except Exception as stop_error:
                    print(f"停止作业 {job_id} 时出错: {stop_error}")
                    traceback.print_exc()
            raise

    return wrapper