"""
Task queue implementation for the ray multicluster scheduler.
"""

import threading
import time
from collections import deque
from typing import Optional
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TaskQueue:
    """A simple FIFO task queue implementation."""

    def __init__(self, max_size: int = 1000):
        """
        Initialize the task queue.

        Args:
            max_size: Maximum number of tasks that can be queued
        """
        self.max_size = max_size
        self.queue = deque()
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def enqueue(self, task_desc: TaskDescription) -> bool:
        """
        Add a task to the queue.

        Args:
            task_desc: The task description to enqueue

        Returns:
            True if the task was successfully enqueued, False if the queue is full
        """
        with self.condition:
            if len(self.queue) >= self.max_size:
                logger.warning(f"任务队列已满，无法添加任务 {task_desc.task_id}")
                return False

            self.queue.append(task_desc)
            logger.info(f"任务 {task_desc.task_id} 已加入队列，当前队列大小: {len(self.queue)}")
            self.condition.notify()  # Notify waiting threads that a task is available
            return True

    def dequeue(self) -> Optional[TaskDescription]:
        """
        Remove and return a task from the queue.

        Returns:
            The oldest task in the queue, or None if the queue is empty
        """
        with self.condition:
            # Wait for a task to become available if the queue is empty
            while len(self.queue) == 0:
                # Wait for 1 second and check again to avoid indefinite blocking
                if not self.condition.wait(timeout=1.0):
                    return None

            task_desc = self.queue.popleft()
            logger.info(f"任务 {task_desc.task_id} 已从队列中取出，剩余队列大小: {len(self.queue)}")
            return task_desc

    def size(self) -> int:
        """
        Get the current size of the queue.

        Returns:
            The number of tasks currently in the queue
        """
        with self.lock:
            return len(self.queue)

    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise
        """
        with self.lock:
            return len(self.queue) == 0

    def clear(self):
        """Clear all tasks from the queue."""
        with self.lock:
            cleared_count = len(self.queue)
            self.queue.clear()
            logger.info(f"已清空任务队列，清除 {cleared_count} 个任务")