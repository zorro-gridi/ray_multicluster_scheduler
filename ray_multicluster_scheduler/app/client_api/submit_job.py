"""Submit jobs to the multicluster scheduler."""

import ray
import time
import signal
import sys
import atexit
from typing import Dict, List, Optional, Tuple, Any
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    get_unified_scheduler, initialize_scheduler_environment
)

logger = get_logger(__name__)

# Global state for the job scheduler
_task_lifecycle_manager = None
_initialization_attempted = False

# Track active jobs for signal handling
active_jobs = set()


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


def _stop_all_active_jobs():
    """Stop all active jobs when the process exits."""
    global active_jobs
    for job_cluster_pair in active_jobs:
        try:
            cluster_name, job_id = job_cluster_pair
            # Get the job client for this cluster
            scheduler = get_unified_scheduler()
            job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
            if job_client:
                job_client.stop_job(job_id)
                logger.info(f"Stopped job {job_id} on cluster {cluster_name} during cleanup")
        except Exception as e:
            logger.error(f"Error stopping job during cleanup: {e}")


# Register cleanup function
atexit.register(_stop_all_active_jobs)


def _handle_sigint(sig, frame):
    """Handle SIGINT (Ctrl+C) to stop all active jobs."""
    global active_jobs
    logger.info("Received SIGINT, stopping all active jobs...")
    _stop_all_active_jobs()
    sys.exit(130)  # Standard exit code for Ctrl+C


# Register signal handler
signal.signal(signal.SIGINT, _handle_sigint)


class BatchJobFailed(RuntimeError):
    """Exception raised when a batch of jobs fails."""
    pass


def wait_batch_jobs(job_client, job_ids: List[str], cluster_name: str):
    """
    Wait for all jobs in a batch to complete, then determine success/failure.

    Args:
        job_client: The JobSubmissionClient to use
        job_ids: List of job IDs to wait for
        cluster_name: Name of the cluster where jobs are running

    Returns:
        Dict: Job status for each job ID

    Raises:
        BatchJobFailed: If any job in the batch fails
    """
    job_status = {job_id: None for job_id in job_ids}
    TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED"}
    FAIL_STATES = {"FAILED"}

    try:
        while True:
            all_terminal = True

            for job_id in job_ids:
                status = job_client.get_job_status(job_id)
                job_status[job_id] = status

                if status not in TERMINAL_STATES:
                    all_terminal = False

            if all_terminal:
                failed_jobs = [
                    job_id for job_id, status in job_status.items()
                    if status in FAIL_STATES
                ]

                if failed_jobs:
                    raise BatchJobFailed(
                        f"Batch failed, failed jobs: {failed_jobs}"
                    )

                # 全部成功
                return job_status

            time.sleep(2)

    except KeyboardInterrupt:
        # Ctrl+C：终止整批 job
        for job_id in job_ids:
            try:
                job_client.stop_job(job_id)
                logger.info(f"Stopped job {job_id} due to KeyboardInterrupt")
            except Exception:
                pass
        raise


def submit_job(
    entrypoint: str,
    runtime_env: Optional[Dict] = None,
    job_id: Optional[str] = None,
    metadata: Optional[Dict] = None,
    submission_id: Optional[str] = None,
    preferred_cluster: Optional[str] = None,
    resource_requirements: Optional[Dict[str, float]] = None,
    tags: Optional[List[str]] = None
) -> str:
    """
    Submit a job to the multicluster scheduler using JobSubmissionClient.

    This function submits a command to be executed as a Ray job on one of the available clusters.
    The job will be scheduled based on resource availability and cluster preferences.

    Args:
        entrypoint (str): The command to run in the job (e.g., "python train.py")
        runtime_env (Dict, optional): Runtime environment for the job
        job_id (str, optional): Unique identifier for the job
        metadata (Dict, optional): Metadata to associate with the job
        submission_id (str, optional): Submission ID for tracking
        preferred_cluster (str, optional): Preferred cluster name for job execution
        resource_requirements (Dict[str, float], optional): Resource requirements for the job
        tags (List[str], optional): List of tags to associate with the job

    Returns:
        str: Job ID of the submitted job

    Note:
        This function now supports concurrent job submissions. Multiple jobs can be
        submitted simultaneously without interfering with each other.

    Raises:
        RuntimeError: If the scheduler is not initialized or job submission fails
    """
    global _task_lifecycle_manager, active_jobs

    # 确保调度器已初始化
    if _task_lifecycle_manager is None:
        _ensure_scheduler_initialized()

    # 如果仍然没有初始化，则抛出异常
    if _task_lifecycle_manager is None:
        raise RuntimeError("Scheduler not initialized. Call initialize_scheduler_environment() first.")

    # Get cluster information for scheduling decision
    cluster_info = _task_lifecycle_manager.cluster_monitor.get_all_cluster_info()

    # 如果指定了首选集群，使用该集群的runtime_env
    if preferred_cluster and preferred_cluster in cluster_info:
        cluster_metadata = cluster_info[preferred_cluster]['metadata']
        if hasattr(cluster_metadata, 'runtime_env'):
            runtime_env = cluster_metadata.runtime_env
    # 否则保持runtime_env为传入的值（可能是None）

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


def submit_batch_jobs(
    jobs: List[Dict],
    preferred_cluster: Optional[str] = None
) -> List[str]:
    """
    Submit a batch of jobs and wait for all to complete before determining success/failure.

    Args:
        jobs: List of job dictionaries, each containing entrypoint, runtime_env, etc.
        preferred_cluster: Preferred cluster for all jobs in the batch

    Returns:
        List[str]: List of job IDs for the submitted jobs

    Raises:
        BatchJobFailed: If any job in the batch fails
    """
    global _task_lifecycle_manager, active_jobs

    if not jobs:
        return []

    # 确保调度器已初始化
    if _task_lifecycle_manager is None:
        _ensure_scheduler_initialized()

    if _task_lifecycle_manager is None:
        raise RuntimeError("Scheduler not initialized. Call initialize_scheduler_environment() first.")

    job_ids = []
    cluster_name = None

    # Submit all jobs in the batch
    for job_params in jobs:
        job_id = submit_job(
            entrypoint=job_params.get('entrypoint'),
            runtime_env=job_params.get('runtime_env'),
            job_id=job_params.get('job_id'),
            metadata=job_params.get('metadata'),
            submission_id=job_params.get('submission_id'),
            preferred_cluster=preferred_cluster or job_params.get('preferred_cluster'),
            resource_requirements=job_params.get('resource_requirements'),
            tags=job_params.get('tags')
        )
        job_ids.append(job_id)

        # Get the cluster for this job (we'll assume all jobs in batch go to same cluster for now)
        if not cluster_name:
            # Get cluster info to determine where the job was submitted
            cluster_info = _task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
            # For now, we'll use the preferred cluster or first available
            if preferred_cluster:
                cluster_name = preferred_cluster
            else:
                cluster_name = next(iter(cluster_info.keys()))

    # Add jobs to active jobs set for cleanup
    for job_id in job_ids:
        active_jobs.add((cluster_name, job_id))

    # Get the job client for this cluster
    job_client = _task_lifecycle_manager.connection_manager.get_job_client(cluster_name)

    # Wait for all jobs to complete and check for failures
    try:
        job_status = wait_batch_jobs(job_client, job_ids, cluster_name)
        logger.info(f"All jobs in batch completed successfully: {list(job_status.keys())}")
        # Remove jobs from active set after completion
        for job_id in job_ids:
            active_jobs.discard((cluster_name, job_id))
        return job_ids
    except BatchJobFailed:
        logger.error(f"Batch job failed: {job_ids}")
        # Remove jobs from active set after failure
        for job_id in job_ids:
            active_jobs.discard((cluster_name, job_id))
        raise
    except KeyboardInterrupt:
        logger.info("Batch job interrupted by user")
        # Remove jobs from active set after interruption
        for job_id in job_ids:
            active_jobs.discard((cluster_name, job_id))
        raise


def get_job_status(job_id: str, cluster_name: Optional[str] = None) -> Any:
    """
    Get the status of a submitted job.

    Args:
        job_id (str): The ID of the job to check
        cluster_name (str, optional): The cluster where the job is running

    Returns:
        Any: The status of the job
    """
    if _task_lifecycle_manager is None:
        _ensure_scheduler_initialized()

    if _task_lifecycle_manager is None:
        raise RuntimeError("Scheduler not initialized. Call initialize_scheduler_environment() first.")

    # If cluster name is not provided, we need to determine which cluster the job is on
    # For now, assume caller provides the cluster name
    if not cluster_name:
        raise ValueError("cluster_name must be provided to get job status")

    job_client = _task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
    if not job_client:
        raise ValueError(f"No job client available for cluster {cluster_name}")

    return job_client.get_job_status(job_id)


def initialize_scheduler(task_lifecycle_manager):
    """
    Initialize the submit_job module with a task lifecycle manager.

    This function must be called before submitting any jobs. It sets up the
    global state needed for job submission.

    Args:
        task_lifecycle_manager: The task lifecycle manager to use for scheduling
    """
    global _task_lifecycle_manager
    _task_lifecycle_manager = task_lifecycle_manager
    logger.info("✅ submit_job scheduler initialized")