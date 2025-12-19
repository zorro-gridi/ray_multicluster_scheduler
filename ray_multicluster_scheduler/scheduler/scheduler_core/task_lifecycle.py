"""
Task lifecycle manager that orchestrates the complete task execution flow.
"""

import time
import threading
from typing import Dict, Any, Optional
import ray  # 添加ray导入
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.queue.backpressure_controller import BackpressureController
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.scheduler_core.result_collector import ResultCollector
from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError
from ray_multicluster_scheduler.common.circuit_breaker import ClusterCircuitBreakerManager

logger = get_logger(__name__)


class TaskLifecycleManager:
    """Manages the complete lifecycle of tasks from submission to result collection."""

    def __init__(self, task_queue: TaskQueue, backpressure_controller: BackpressureController,
                 dispatcher: Dispatcher, result_collector: ResultCollector, cluster_registry: ClusterRegistry,
                 circuit_breaker_manager: ClusterCircuitBreakerManager = None):
        self.task_queue = task_queue
        self.backpressure_controller = backpressure_controller
        self.dispatcher = dispatcher
        self.result_collector = result_collector
        self.cluster_registry = cluster_registry
        self.circuit_breaker_manager = circuit_breaker_manager or ClusterCircuitBreakerManager()
        self.running = False
        self.worker_thread = None
        # 添加存储任务结果的字典
        self.task_results = {}

    def start(self):
        """Start the task lifecycle manager."""
        if self.running:
            logger.warning("Task lifecycle manager is already running")
            return

        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("Task lifecycle manager started")

    def stop(self):
        """Stop the task lifecycle manager."""
        if not self.running:
            logger.warning("Task lifecycle manager is not running")
            return

        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)  # Wait up to 5 seconds for thread to finish
        logger.info("Task lifecycle manager stopped")

    def submit_task(self, task_desc: TaskDescription) -> bool:
        """Submit a task to the queue."""
        # 直接处理任务而不是仅仅加入队列
        try:
            # Refresh cluster resource snapshots
            self.cluster_registry.refresh_resource_snapshots()

            # Get cluster snapshots
            cluster_info = self.cluster_registry.get_all_cluster_info()
            cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                              if info['snapshot'] is not None}

            # Process the task immediately
            self._process_task(task_desc, cluster_snapshots)
            return True
        except Exception as e:
            logger.error(f"Task {task_desc.task_id} submission failed: {e}")
            return False

    def _worker_loop(self):
        """Main worker loop that processes tasks from the queue."""
        while self.running:
            try:
                # Refresh cluster resource snapshots
                self.cluster_registry.refresh_resource_snapshots()

                # Check if backpressure should be applied
                cluster_info = self.cluster_registry.get_all_cluster_info()
                cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                                  if info['snapshot'] is not None}

                if self.backpressure_controller.should_apply_backpressure(cluster_snapshots):
                    # Apply backpressure by sleeping
                    backoff_time = self.backpressure_controller.get_backoff_time()
                    logger.info(f"Applying backpressure, sleeping for {backoff_time} seconds")
                    time.sleep(backoff_time)
                    continue

                # Dequeue a task
                task_desc = self.task_queue.dequeue()
                if not task_desc:
                    # No tasks in queue, sleep briefly
                    time.sleep(0.1)
                    continue

                # Process the task
                self._process_task(task_desc, cluster_snapshots)

            except Exception as e:
                logger.error(f"Error in task lifecycle worker loop: {e}")
                time.sleep(1.0)  # Sleep to avoid tight loop on repeated errors

    def _process_task(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]):
        """Process a single task."""
        try:
            logger.info(f"Processing task {task_desc.task_id}")

            # Dispatch the task
            future = self.dispatcher.dispatch_task(task_desc, cluster_snapshots)

            # Collect the result
            # Note: In a real implementation, you might want to handle this asynchronously
            # rather than blocking here
            result = ray.get(future)  # 使用ray.get获取结果而不是自定义的result_collector

            logger.info(f"Task {task_desc.task_id} completed successfully with result: {result}")
            # 存储结果以便后续查询
            self.task_results[task_desc.task_id] = result

        except NoHealthyClusterError as e:
            logger.error(f"Task {task_desc.task_id} failed due to no healthy clusters: {e}")
            # In a real implementation, you might want to requeue the task or notify the caller
        except TaskSubmissionError as e:
            logger.error(f"Task {task_desc.task_id} failed during submission: {e}")
            # In a real implementation, you might want to requeue the task or notify the caller
        except Exception as e:
            logger.error(f"Task {task_desc.task_id} failed unexpectedly: {e}")
            # In a real implementation, you might want to requeue the task or notify the caller