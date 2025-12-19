"""
Task queue implementation with FIFO ordering.
"""

import queue
import threading
from typing import Optional
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TaskQueue:
    """Thread-safe task queue with configurable maximum size."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._queue = queue.Queue(maxsize=max_size)
        self._lock = threading.Lock()

    def enqueue(self, task: TaskDescription) -> bool:
        """Add a task to the queue. Returns True if successful, False if queue is full."""
        try:
            self._queue.put_nowait(task)
            logger.debug(f"Enqueued task {task.task_id}")
            return True
        except queue.Full:
            logger.warning(f"Task queue is full, cannot enqueue task {task.task_id}")
            return False

    def dequeue(self) -> Optional[TaskDescription]:
        """Remove and return a task from the queue. Returns None if queue is empty."""
        try:
            task = self._queue.get_nowait()
            logger.debug(f"Dequeued task {task.task_id}")
            return task
        except queue.Empty:
            return None

    def size(self) -> int:
        """Return the current size of the queue."""
        return self._queue.qsize()

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def is_full(self) -> bool:
        """Check if the queue is full."""
        return self._queue.full()