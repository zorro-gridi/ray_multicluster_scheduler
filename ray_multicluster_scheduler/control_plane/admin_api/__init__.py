"""
Admin API for managing and monitoring the ray multicluster scheduler.
"""

from typing import Dict, List, Any
from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
from ray_multicluster_scheduler.scheduler.health.metrics_aggregator import MetricsAggregator
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class AdminAPI:
    """Provides administrative APIs for monitoring and managing the scheduler."""

    def __init__(self, cluster_registry: ClusterRegistry, task_queue: TaskQueue, health_checker: HealthChecker, metrics_aggregator: MetricsAggregator, cluster_monitor):
        self.cluster_registry = cluster_registry
        self.task_queue = task_queue
        self.health_checker = health_checker
        self.metrics_aggregator = metrics_aggregator
        self.cluster_monitor = cluster_monitor

    def get_cluster_status(self) -> Dict[str, Dict]:
        """Get status information for all clusters."""
        return self.cluster_registry.get_all_cluster_info()

    def get_queue_status(self) -> Dict[str, int]:
        """Get status information for the task queue."""
        return {
            "queue_size": self.task_queue.size(),
            "queue_max_size": self.task_queue.max_size,
            "is_empty": self.task_queue.is_empty(),
            "is_full": self.task_queue.is_full()
        }

    def get_health_status(self) -> Dict[str, str]:
        """Get health status of all clusters."""
        # Get snapshots from global state
        cluster_info = self.cluster_monitor.get_all_cluster_info()
        snapshots = {name: info['snapshot'] for name, info in cluster_info.items() if info['snapshot'] is not None}

        # Determine health status for each cluster
        health_status = {}
        for cluster_name in self.cluster_registry.metadata_manager.list_clusters():
            if cluster_name.name in snapshots:
                health_status[cluster_name.name] = "HEALTHY"
            else:
                health_status[cluster_name.name] = "UNHEALTHY"

        return health_status

    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get overall scheduler statistics."""
        cluster_status = self.get_cluster_status()
        queue_status = self.get_queue_status()
        health_status = self.get_health_status()

        return {
            "clusters": {
                "count": len(cluster_status),
                "details": cluster_status
            },
            "queue": queue_status,
            "health": health_status
        }