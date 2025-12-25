"""
Task queue implementation for the ray multicluster scheduler.
"""

import threading
import time
from collections import deque, defaultdict
from typing import Optional
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TaskQueue:
    """A task queue implementation with both global and per-cluster queues."""

    def __init__(self, max_size: int = 1000):
        """
        Initialize the task queue.

        Args:
            max_size: Maximum number of tasks that can be queued in global queue
        """
        self.max_size = max_size
        self.global_queue = deque()  # Global queue for tasks not tied to specific cluster
        self.cluster_queues = defaultdict(deque)  # Per-cluster queues
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def enqueue_global(self, task_desc: TaskDescription) -> bool:
        """
        Add a task to the global queue.

        Args:
            task_desc: The task description to enqueue

        Returns:
            True if the task was successfully enqueued, False if the queue is full
        """
        with self.condition:
            if len(self.global_queue) >= self.max_size:
                logger.warning(f"全局任务队列已满，无法添加任务 {task_desc.task_id}")
                return False

            self.global_queue.append(task_desc)
            logger.info(f"任务 {task_desc.task_id} 已加入全局队列，当前全局队列大小: {len(self.global_queue)}")
            self.condition.notify()  # Notify waiting threads that a task is available
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
            self.cluster_queues[cluster_name].append(task_desc)
            logger.info(f"任务 {task_desc.task_id} 已加入集群 {cluster_name} 的队列，"
                       f"当前 {cluster_name} 队列大小: {len(self.cluster_queues[cluster_name])}")
            self.condition.notify()  # Notify waiting threads that a task is available
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
                logger.info(f"任务 {task_desc.task_id} 已从集群 {cluster_name} 队列中取出，"
                           f"剩余 {cluster_name} 队列大小: {len(self.cluster_queues[cluster_name])}")
                return task_desc
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
            logger.info(f"任务 {task_desc.task_id} 已从全局队列中取出，剩余全局队列大小: {len(self.global_queue)}")
            return task_desc

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

    def size(self, cluster_name: str = None) -> int:
        """
        Get the current size of the queue.
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

    def total_size(self) -> int:
        """
        Get the total size of all queues (global + all cluster queues).

        Returns:
            The total number of tasks in all queues
        """
        with self.lock:
            total = len(self.global_queue)
            for cluster_queue in self.cluster_queues.values():
                total += len(cluster_queue)
            return total

    def is_empty(self, cluster_name: str = None) -> bool:
        """
        Check if the specified queue is empty.
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

    def clear(self, cluster_name: str = None):
        """Clear all tasks from the specified queue.
        If cluster_name is provided, clear that cluster's queue; otherwise clear global queue."""
        with self.lock:
            if cluster_name:
                cleared_count = len(self.cluster_queues[cluster_name])
                self.cluster_queues[cluster_name].clear()
                logger.info(f"已清空集群 {cluster_name} 任务队列，清除 {cleared_count} 个任务")
            else:
                cleared_count = len(self.global_queue)
                self.global_queue.clear()
                logger.info(f"已清空全局任务队列，清除 {cleared_count} 个任务")

    def get_cluster_queue_names(self) -> list:
        """Get a list of all cluster queue names that have tasks."""
        with self.lock:
            return [name for name, queue in self.cluster_queues.items() if len(queue) > 0]