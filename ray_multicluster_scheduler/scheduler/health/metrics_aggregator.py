"""
Metrics aggregator for collecting and exposing scheduler metrics.
"""

import time
from typing import Dict, List
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class MetricsAggregator:
    """Aggregates metrics from various scheduler components."""

    def __init__(self, health_checker: HealthChecker, task_queue: TaskQueue, cluster_registry: ClusterRegistry):
        self.health_checker = health_checker
        self.task_queue = task_queue
        self.cluster_registry = cluster_registry
        self.metrics = {}
        self.last_collection_time = 0

    def collect_metrics(self) -> Dict[str, float]:
        """Collect metrics from all components."""
        current_time = time.time()

        # Collect cluster metrics
        cluster_metrics = self._collect_cluster_metrics()

        # Collect queue metrics
        queue_metrics = self._collect_queue_metrics()

        # Collect timing metrics
        timing_metrics = {
            "metrics_collection_duration": current_time - self.last_collection_time
        }

        # Combine all metrics
        self.metrics = {**cluster_metrics, **queue_metrics, **timing_metrics}
        self.last_collection_time = current_time

        logger.debug(f"Collected metrics: {self.metrics}")
        return self.metrics

    def _collect_cluster_metrics(self) -> Dict[str, float]:
        """Collect metrics from cluster components."""
        metrics = {}

        # Get cluster snapshots
        snapshots = self.health_checker.check_health()

        # Collect metrics for each cluster
        for cluster_name, snapshot in snapshots.items():
            # CPU metrics
            cpu_available = snapshot.available_resources.get("CPU", 0)
            cpu_total = snapshot.total_resources.get("CPU", 0)
            cpu_utilization = 1.0 - (cpu_available / cpu_total) if cpu_total > 0 else 0.0

            metrics[f"cluster_{cluster_name}_cpu_utilization"] = cpu_utilization
            metrics[f"cluster_{cluster_name}_node_count"] = float(snapshot.node_count)

            # Memory metrics (if available)
            memory_available = snapshot.available_resources.get("memory", 0)
            memory_total = snapshot.total_resources.get("memory", 0)
            memory_utilization = 1.0 - (memory_available / memory_total) if memory_total > 0 else 0.0

            metrics[f"cluster_{cluster_name}_memory_utilization"] = memory_utilization

            # GPU metrics (if available)
            gpu_available = snapshot.available_resources.get("GPU", 0)
            gpu_total = snapshot.total_resources.get("GPU", 0)
            gpu_utilization = 1.0 - (gpu_available / gpu_total) if gpu_total > 0 else 0.0

            metrics[f"cluster_{cluster_name}_gpu_utilization"] = gpu_utilization

        return metrics

    def _collect_queue_metrics(self) -> Dict[str, float]:
        """Collect metrics from the task queue."""
        return {
            "task_queue_size": float(self.task_queue.size()),
            "task_queue_is_empty": float(self.task_queue.is_empty()),
            "task_queue_is_full": float(self.task_queue.is_full())
        }

    def get_prometheus_format(self) -> str:
        """Get metrics in Prometheus exposition format."""
        if not self.metrics:
            self.collect_metrics()

        lines = []
        lines.append("# HELP ray_multicluster_scheduler_metrics Ray Multi-Cluster Scheduler Metrics")
        lines.append("# TYPE ray_multicluster_scheduler_metrics gauge")

        for key, value in self.metrics.items():
            # Replace invalid characters in metric names
            prometheus_key = key.replace(".", "_").replace("-", "_")
            lines.append(f"{prometheus_key} {value}")

        return "\n".join(lines)