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

    def __init__(self, health_checker: HealthChecker, task_queue: TaskQueue, cluster_registry: ClusterRegistry, cluster_monitor):
        self.health_checker = health_checker
        self.task_queue = task_queue
        self.cluster_registry = cluster_registry
        self.cluster_monitor = cluster_monitor
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

        # Get cluster snapshots from the global snapshots stored in cluster_monitor
        cluster_info = self.cluster_monitor.get_all_cluster_info()

        # Collect metrics for each cluster
        for cluster_name, cluster_data in cluster_info.items():
            snapshot = cluster_data['snapshot']
            if snapshot:
                # Use the new ResourceSnapshot fields
                metrics[f"cluster_{cluster_name}_cpu_utilization"] = snapshot.cluster_cpu_usage_percent / 100.0  # Convert percentage to ratio
                metrics[f"cluster_{cluster_name}_node_count"] = float(snapshot.node_count)
                metrics[f"cluster_{cluster_name}_memory_utilization"] = snapshot.cluster_mem_usage_percent / 100.0  # Convert percentage to ratio

                # Additional metrics from the new ResourceSnapshot structure
                metrics[f"cluster_{cluster_name}_cpu_used_cores"] = snapshot.cluster_cpu_used_cores
                metrics[f"cluster_{cluster_name}_cpu_total_cores"] = snapshot.cluster_cpu_total_cores
                metrics[f"cluster_{cluster_name}_memory_used_mb"] = snapshot.cluster_mem_used_mb
                metrics[f"cluster_{cluster_name}_memory_total_mb"] = snapshot.cluster_mem_total_mb

        return metrics

    def _collect_queue_metrics(self) -> Dict[str, float]:
        """Collect metrics from the task queue."""
        return {
            "task_queue_size": float(self.task_queue.size()),
            "task_queue_is_empty": float(self.task_queue.is_empty()),
            "task_queue_is_full": float(self.task_queue.is_full())
        }
