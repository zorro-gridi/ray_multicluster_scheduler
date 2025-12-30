"""
Task queue implementation for the ray multicluster scheduler.
"""

import threading
import time
from collections import deque, defaultdict
from typing import Optional, Union, List
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TaskQueue:
    """A task queue implementation with both global and per-cluster queues, supporting both Task and Job types."""

    def __init__(self, max_size: int = 100):
        """
        Initialize the task queue.

        Args:
            max_size: Maximum number of tasks that can be queued in global queue
        """
        self.max_size = max_size
        self.global_queue = deque()  # Global queue for tasks not tied to specific cluster
        self.cluster_queues = defaultdict(deque)  # Per-cluster queues
        # New: Job queues for JobSubmissionClient tasks
        self.global_job_queue = deque()  # Global queue for jobs not tied to specific cluster
        self.cluster_job_queues = defaultdict(deque)  # Per-cluster job queues
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

        # Sets to track task/job IDs for deduplication
        self.global_task_ids = set()  # Track task IDs in global queue
        self.cluster_task_ids = defaultdict(set)  # Track task IDs in cluster queues
        self.global_job_ids = set()  # Track job IDs in global queue
        self.cluster_job_ids = defaultdict(set)  # Track job IDs in cluster queues

    def _is_task_in_global_queue(self, task_id: str) -> bool:
        """Check if a task with the given ID is already in the global queue."""
        return task_id in self.global_task_ids

    def _is_task_in_cluster_queue(self, task_id: str, cluster_name: str) -> bool:
        """Check if a task with the given ID is already in the specified cluster queue."""
        return task_id in self.cluster_task_ids[cluster_name]

    def _is_job_in_global_queue(self, job_id: str) -> bool:
        """Check if a job with the given ID is already in the global job queue."""
        return job_id in self.global_job_ids

    def _is_job_in_cluster_queue(self, job_id: str, cluster_name: str) -> bool:
        """Check if a job with the given ID is already in the specified cluster job queue."""
        return job_id in self.cluster_job_ids[cluster_name]

    def _is_duplicate_task(self, task_desc: TaskDescription) -> bool:
        """Check if a task with the same function and arguments is already in any queue."""
        # Check global queue
        for task in self.global_queue:
            if self._tasks_have_same_content(task, task_desc):
                return True
        # Check cluster queues
        for cluster_queue in self.cluster_queues.values():
            for task in cluster_queue:
                if self._tasks_have_same_content(task, task_desc):
                    return True
        return False

    def _is_duplicate_job(self, job_desc: JobDescription) -> bool:
        """Check if a job with the same entrypoint and parameters is already in any queue."""
        # Check global job queue
        for job in self.global_job_queue:
            if self._jobs_have_same_content(job, job_desc):
                return True
        # Check cluster job queues
        for cluster_queue in self.cluster_job_queues.values():
            for job in cluster_queue:
                if self._jobs_have_same_content(job, job_desc):
                    return True
        return False

    def _tasks_have_same_content(self, task1: TaskDescription, task2: TaskDescription) -> bool:
        """Check if two tasks have the same function and arguments (ignoring task ID)."""
        return (task1.func_or_class == task2.func_or_class and
                task1.args == task2.args and
                task1.kwargs == task2.kwargs and
                task1.resource_requirements == task2.resource_requirements and
                task1.tags == task2.tags and
                task1.preferred_cluster == task2.preferred_cluster)

    def _jobs_have_same_content(self, job1: JobDescription, job2: JobDescription) -> bool:
        """Check if two jobs have the same entrypoint and parameters (ignoring job ID)."""
        return (job1.entrypoint == job2.entrypoint and
                job1.runtime_env == job2.runtime_env and
                job1.metadata == job2.metadata and
                job1.preferred_cluster == job2.preferred_cluster)

    def enqueue_global(self, task_desc: TaskDescription) -> bool:
        """
        Add a task to the global queue.

        Args:
            task_desc: The task description to enqueue

        Returns:
            True if the task was successfully enqueued, False if the queue is full
        """
        with self.condition:
            # Check for duplicates by task ID first
            if self._is_task_in_global_queue(task_desc.task_id):
                logger.info(f"任务 {task_desc.task_id} 已存在于全局队列中，跳过去重添加")
                return True  # Return True as it's already queued, no error

            # Also check for duplicate by content to prevent duplicate user submissions
            if self._is_duplicate_task(task_desc):
                logger.info(f"任务 {task_desc.task_id} 的内容已存在于队列中，跳过去重添加")
                return True  # Return True as a similar task is already queued, no error

            if len(self.global_queue) >= self.max_size:
                logger.warning(f"全局任务队列已满，无法添加任务 {task_desc.task_id}")
                return False

            self.global_queue.append(task_desc)
            self.global_task_ids.add(task_desc.task_id)
            logger.info(f"任务 {task_desc.task_id} 已加入全局队列，当前全局队列大小: {len(self.global_queue)}")
            self.condition.notify()  # Notify waiting threads that a task is available
            return True

    def enqueue_global_job(self, job_desc: JobDescription) -> bool:
        """
        Add a job to the global job queue.

        Args:
            job_desc: The job description to enqueue

        Returns:
            True if the job was successfully enqueued, False if the queue is full
        """
        with self.condition:
            # Check for duplicates by job ID first
            if self._is_job_in_global_queue(job_desc.job_id):
                logger.info(f"作业 {job_desc.job_id} 已存在于全局作业队列中，跳过去重添加")
                return True  # Return True as it's already queued, no error

            # Also check for duplicate by content to prevent duplicate user submissions
            if self._is_duplicate_job(job_desc):
                logger.info(f"作业 {job_desc.job_id} 的内容已存在于队列中，跳过去重添加")
                return True  # Return True as a similar job is already queued, no error

            if len(self.global_job_queue) >= self.max_size:
                logger.warning(f"全局作业队列已满，无法添加作业 {job_desc.job_id}")
                return False

            self.global_job_queue.append(job_desc)
            self.global_job_ids.add(job_desc.job_id)
            logger.info(f"作业 {job_desc.job_id} 已加入全局作业队列，当前全局作业队列大小: {len(self.global_job_queue)}")
            self.condition.notify()  # Notify waiting threads that a job is available
            return True

    def enqueue_cluster(self, task_desc: TaskDescription, cluster_name: str) -> bool:
        """
        Add a task to a specific cluster's queue.

        Args:
            task_desc: The task description to enqueue
            cluster_name: The name of the cluster queue to add to

        Returns:
            True if the task was successfully enqueued
        """
        with self.condition:
            # Check for duplicates in this cluster queue
            if self._is_task_in_cluster_queue(task_desc.task_id, cluster_name):
                logger.info(f"任务 {task_desc.task_id} 已存在于集群 {cluster_name} 队列中，跳过去重添加")
                return True  # Return True as it's already queued, no error

            # Also check for duplicate by content to prevent duplicate user submissions
            if self._is_duplicate_task(task_desc):
                logger.info(f"任务 {task_desc.task_id} 的内容已存在于队列中，跳过去重添加")
                return True  # Return True as a similar task is already queued, no error

            self.cluster_queues[cluster_name].append(task_desc)
            self.cluster_task_ids[cluster_name].add(task_desc.task_id)
            logger.info(f"任务 {task_desc.task_id} 已加入集群 {cluster_name} 的队列，"
                       f"当前 {cluster_name} 队列大小: {len(self.cluster_queues[cluster_name])}")
            self.condition.notify()  # Notify waiting threads that a task is available
            return True

    def enqueue_cluster_job(self, job_desc: JobDescription, cluster_name: str) -> bool:
        """
        Add a job to a specific cluster's job queue.

        Args:
            job_desc: The job description to enqueue
            cluster_name: The name of the cluster queue to add to

        Returns:
            True if the job was successfully enqueued
        """
        with self.condition:
            # Check for duplicates in this cluster queue
            if self._is_job_in_cluster_queue(job_desc.job_id, cluster_name):
                logger.info(f"作业 {job_desc.job_id} 已存在于集群 {cluster_name} 作业队列中，跳过去重添加")
                return True  # Return True as it's already queued, no error

            # Also check for duplicate by content to prevent duplicate user submissions
            if self._is_duplicate_job(job_desc):
                logger.info(f"作业 {job_desc.job_id} 的内容已存在于队列中，跳过去重添加")
                return True  # Return True as a similar job is already queued, no error

            self.cluster_job_queues[cluster_name].append(job_desc)
            self.cluster_job_ids[cluster_name].add(job_desc.job_id)
            logger.info(f"作业 {job_desc.job_id} 已加入集群 {cluster_name} 的作业队列，"
                       f"当前 {cluster_name} 作业队列大小: {len(self.cluster_job_queues[cluster_name])}")
            self.condition.notify()  # Notify waiting threads that a job is available
            return True

    def enqueue(self, task_desc: TaskDescription, cluster_name: str = None) -> bool:
        """
        Add a task to the appropriate queue.
        If cluster_name is provided, add to that cluster's queue; otherwise add to global queue.

        Args:
            task_desc: The task description to enqueue
            cluster_name: Optional cluster name for cluster-specific queue

        Returns:
            True if the task was successfully enqueued
        """
        if cluster_name:
            return self.enqueue_cluster(task_desc, cluster_name)
        else:
            return self.enqueue_global(task_desc)

    def enqueue_job(self, job_desc: JobDescription, cluster_name: str = None) -> bool:
        """
        Add a job to the appropriate queue.
        If cluster_name is provided, add to that cluster's job queue; otherwise add to global job queue.

        Args:
            job_desc: The job description to enqueue
            cluster_name: Optional cluster name for cluster-specific queue

        Returns:
            True if the job was successfully enqueued
        """
        if cluster_name:
            return self.enqueue_cluster_job(job_desc, cluster_name)
        else:
            return self.enqueue_global_job(job_desc)

    def dequeue_from_cluster(self, cluster_name: str) -> Optional[TaskDescription]:
        """
        Remove and return a task from a specific cluster's queue.

        Args:
            cluster_name: The name of the cluster queue to dequeue from

        Returns:
            The oldest task in the cluster queue, or None if the queue is empty
        """
        with self.condition:
            if cluster_name in self.cluster_queues and self.cluster_queues[cluster_name]:
                task_desc = self.cluster_queues[cluster_name].popleft()
                # Remove the task ID from the tracking set
                self.cluster_task_ids[cluster_name].discard(task_desc.task_id)
                logger.info(f"任务 {task_desc.task_id} 已从集群 {cluster_name} 队列中取出，"
                           f"剩余 {cluster_name} 队列大小: {len(self.cluster_queues[cluster_name])}")
                return task_desc
            return None

    def dequeue_from_cluster_job(self, cluster_name: str) -> Optional[JobDescription]:
        """
        Remove and return a job from a specific cluster's job queue.

        Args:
            cluster_name: The name of the cluster job queue to dequeue from

        Returns:
            The oldest job in the cluster job queue, or None if the queue is empty
        """
        with self.condition:
            if cluster_name in self.cluster_job_queues and self.cluster_job_queues[cluster_name]:
                job_desc = self.cluster_job_queues[cluster_name].popleft()
                # Remove the job ID from the tracking set
                self.cluster_job_ids[cluster_name].discard(job_desc.job_id)
                logger.info(f"作业 {job_desc.job_id} 已从集群 {cluster_name} 作业队列中取出，"
                           f"剩余 {cluster_name} 作业队列大小: {len(self.cluster_job_queues[cluster_name])}")
                return job_desc
            return None

    def dequeue_global(self) -> Optional[TaskDescription]:
        """
        Remove and return a task from the global queue.

        Returns:
            The oldest task in the global queue, or None if the queue is empty
        """
        with self.condition:
            # Wait for a task to become available if the queue is empty
            while len(self.global_queue) == 0:
                # Wait for 1 second and check again to avoid indefinite blocking
                if not self.condition.wait(timeout=1.0):
                    return None

            task_desc = self.global_queue.popleft()
            # Remove the task ID from the tracking set
            self.global_task_ids.discard(task_desc.task_id)
            logger.info(f"任务 {task_desc.task_id} 已从全局队列中取出，剩余全局队列大小: {len(self.global_queue)}")
            return task_desc

    def dequeue_global_job(self) -> Optional[JobDescription]:
        """
        Remove and return a job from the global job queue.

        Returns:
            The oldest job in the global job queue, or None if the queue is empty
        """
        with self.condition:
            # Wait for a job to become available if the queue is empty
            while len(self.global_job_queue) == 0:
                # Wait for 1 second and check again to avoid indefinite blocking
                if not self.condition.wait(timeout=1.0):
                    return None

            job_desc = self.global_job_queue.popleft()
            # Remove the job ID from the tracking set
            self.global_job_ids.discard(job_desc.job_id)
            logger.info(f"作业 {job_desc.job_id} 已从全局作业队列中取出，剩余全局作业队列大小: {len(self.global_job_queue)}")
            return job_desc

    def dequeue(self, cluster_name: str = None) -> Optional[TaskDescription]:
        """
        Remove and return a task from the appropriate queue.
        If cluster_name is provided, try to dequeue from that cluster's queue;
        otherwise, dequeue from the global queue.

        Args:
            cluster_name: Optional cluster name for cluster-specific dequeue

        Returns:
            The oldest task in the specified queue, or None if the queue is empty
        """
        if cluster_name:
            # First try to get from the specific cluster queue
            task = self.dequeue_from_cluster(cluster_name)
            if task:
                return task
            # If cluster queue is empty, try global queue as fallback
            if self.global_queue:
                return self.dequeue_global()
            return None
        else:
            return self.dequeue_global()

    def dequeue_job(self, cluster_name: str = None) -> Optional[JobDescription]:
        """
        Remove and return a job from the appropriate queue.
        If cluster_name is provided, try to dequeue from that cluster's job queue;
        otherwise, dequeue from the global job queue.

        Args:
            cluster_name: Optional cluster name for cluster-specific dequeue

        Returns:
            The oldest job in the specified queue, or None if the queue is empty
        """
        if cluster_name:
            # First try to get from the specific cluster job queue
            job = self.dequeue_from_cluster_job(cluster_name)
            if job:
                return job
            # If cluster job queue is empty, try global job queue as fallback
            if self.global_job_queue:
                return self.dequeue_global_job()
            return None
        else:
            return self.dequeue_global_job()

    def size(self, cluster_name: str = None) -> int:
        """
        Get the current size of the task queue.
        If cluster_name is provided, return size of that cluster's queue; otherwise return global queue size.

        Args:
            cluster_name: Optional cluster name for cluster-specific size

        Returns:
            The number of tasks currently in the specified queue
        """
        with self.lock:
            if cluster_name:
                return len(self.cluster_queues[cluster_name])
            else:
                return len(self.global_queue)

    def job_size(self, cluster_name: str = None) -> int:
        """
        Get the current size of the job queue.
        If cluster_name is provided, return size of that cluster's job queue; otherwise return global job queue size.

        Args:
            cluster_name: Optional cluster name for cluster-specific size

        Returns:
            The number of jobs currently in the specified queue
        """
        with self.lock:
            if cluster_name:
                return len(self.cluster_job_queues[cluster_name])
            else:
                return len(self.global_job_queue)

    def total_size(self) -> int:
        """
        Get the total size of all task queues (global + all cluster queues).

        Returns:
            The total number of tasks in all queues
        """
        with self.lock:
            total = len(self.global_queue)
            for cluster_queue in self.cluster_queues.values():
                total += len(cluster_queue)
            return total

    def total_job_size(self) -> int:
        """
        Get the total size of all job queues (global + all cluster job queues).

        Returns:
            The total number of jobs in all queues
        """
        with self.lock:
            total = len(self.global_job_queue)
            for cluster_job_queue in self.cluster_job_queues.values():
                total += len(cluster_job_queue)
            return total

    def total_combined_size(self) -> int:
        """
        Get the total size of all queues (task queues + job queues).

        Returns:
            The total number of tasks and jobs in all queues
        """
        return self.total_size() + self.total_job_size()

    def is_empty(self, cluster_name: str = None) -> bool:
        """
        Check if the specified task queue is empty.
        If cluster_name is provided, check that cluster's queue; otherwise check global queue.

        Args:
            cluster_name: Optional cluster name for cluster-specific check

        Returns:
            True if the specified queue is empty, False otherwise
        """
        with self.lock:
            if cluster_name:
                return len(self.cluster_queues[cluster_name]) == 0
            else:
                return len(self.global_queue) == 0

    def is_full(self, cluster_name: str = None) -> bool:
        """
        Check if the specified task queue is full.
        If cluster_name is provided, check that cluster's queue; otherwise check global queue.

        Args:
            cluster_name: Optional cluster name for cluster-specific check

        Returns:
            True if the specified queue is full, False otherwise
        """
        with self.lock:
            if cluster_name:
                return len(self.cluster_queues[cluster_name]) >= self.max_size
            else:
                return len(self.global_queue) >= self.max_size

    def is_job_empty(self, cluster_name: str = None) -> bool:
        """
        Check if the specified job queue is empty.
        If cluster_name is provided, check that cluster's job queue; otherwise check global job queue.

        Args:
            cluster_name: Optional cluster name for cluster-specific check

        Returns:
            True if the specified job queue is empty, False otherwise
        """
        with self.lock:
            if cluster_name:
                return len(self.cluster_job_queues[cluster_name]) == 0
            else:
                return len(self.global_job_queue) == 0

    def clear(self, cluster_name: str = None):
        """Clear all tasks from the specified queue.
        If cluster_name is provided, clear that cluster's queue; otherwise clear global queue."""
        with self.lock:
            if cluster_name:
                cleared_count = len(self.cluster_queues[cluster_name])
                # Clear the tracking set as well
                self.cluster_task_ids[cluster_name].clear()
                self.cluster_queues[cluster_name].clear()
                logger.info(f"已清空集群 {cluster_name} 任务队列，清除 {cleared_count} 个任务")
            else:
                cleared_count = len(self.global_queue)
                # Clear the tracking set as well
                self.global_task_ids.clear()
                self.global_queue.clear()
                logger.info(f"已清空全局任务队列，清除 {cleared_count} 个任务")

    def clear_jobs(self, cluster_name: str = None):
        """Clear all jobs from the specified queue.
        If cluster_name is provided, clear that cluster's job queue; otherwise clear global job queue."""
        with self.lock:
            if cluster_name:
                cleared_count = len(self.cluster_job_queues[cluster_name])
                # Clear the tracking set as well
                self.cluster_job_ids[cluster_name].clear()
                self.cluster_job_queues[cluster_name].clear()
                logger.info(f"已清空集群 {cluster_name} 作业队列，清除 {cleared_count} 个作业")
            else:
                cleared_count = len(self.global_job_queue)
                # Clear the tracking set as well
                self.global_job_ids.clear()
                self.global_job_queue.clear()
                logger.info(f"已清空全局作业队列，清除 {cleared_count} 个作业")

    def get_cluster_queue_names(self) -> list:
        """Get a list of all cluster queue names that have tasks."""
        with self.lock:
            return [name for name, queue in self.cluster_queues.items() if len(queue) > 0]

    def get_cluster_job_queue_names(self) -> list:
        """Get a list of all cluster job queue names that have jobs."""
        with self.lock:
            return [name for name, queue in self.cluster_job_queues.items() if len(queue) > 0]