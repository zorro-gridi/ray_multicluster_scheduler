"""
================================================================================
DEPRECATED - This module is scheduled for removal
================================================================================

This module (BackpressureController) has been deprecated and is no longer used
in the system. It will be removed in a future release.

REASON FOR DEPRECATION:
- This module implemented a GLOBAL backpressure strategy that only triggered
  backpressure when ALL clusters exceeded the resource threshold
- This approach has been replaced by a more fine-grained PER-CLUSTER resource
  management system implemented in PolicyEngine
- The 40-second rule (同一集群连续提交间隔) provides additional backpressure
  mechanism, making this global controller redundant

REPLACEMENT:
- PolicyEngine._make_scheduling_decision() implements per-cluster resource
  threshold checking and queuing decisions
- Each cluster is independently evaluated based on its CPU/GPU/Memory utilization
- See: ray_multicluster_scheduler/scheduler/policy/policy_engine.py

HISTORY:
- Originally implemented as a global backpressure mechanism
- Deprecated in: task_lifecycle_manager.py (2025-01)
- No active usage found in codebase as of 2025-01

For new backpressure requirements, please modify PolicyEngine instead.

================================================================================

Backpressure controller to manage task submission rate based on cluster load.
"""

import time
from typing import Dict, List
from ray_multicluster_scheduler.common.model import ResourceSnapshot
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class BackpressureController:
    """Controls task submission rate based on cluster resource utilization."""

    def __init__(self, threshold: float = 0.7):
        """
        Initialize the backpressure controller.

        Args:
            threshold: Resource utilization threshold (0.0-1.0) above which backpressure is applied.
                      Default is 0.7 (70%).
        """
        self.threshold = threshold
        self.last_check_time = 0
        self.backpressure_active = False

    def should_apply_backpressure(self, cluster_snapshots: Dict[str, ResourceSnapshot]) -> bool:
        """Determine if backpressure should be applied based on cluster resource utilization."""
        # Check if we should re-evaluate (at most once per second)
        # NOTE: 每秒检查当前系统的资源快照信息
        current_time = time.time()
        if current_time - self.last_check_time < 1.0:
            return self.backpressure_active

        self.last_check_time = current_time

        try:
            # Check if ANY cluster has high resource utilization
            # We'll use a more granular approach: if ALL clusters are above threshold, then apply backpressure
            any_cluster_available = False
            all_clusters_above_threshold = True

            for cluster_name, snapshot in cluster_snapshots.items():
                # Calculate utilization for this specific cluster
                cpu_utilization = snapshot.cluster_cpu_used_cores / snapshot.cluster_cpu_total_cores if snapshot.cluster_cpu_total_cores > 0 else 0
                memory_utilization = snapshot.cluster_mem_used_mb / snapshot.cluster_mem_total_mb if snapshot.cluster_mem_total_mb > 0 else 0

                # Use the higher utilization of CPU or memory for this cluster
                cluster_max_utilization = max(cpu_utilization, memory_utilization)

                # If this cluster's utilization is below threshold, it's available
                if cluster_max_utilization <= self.threshold:
                    any_cluster_available = True
                    all_clusters_above_threshold = False
                    break  # At least one cluster is available, so no backpressure needed

            # Apply backpressure only if ALL clusters are above threshold
            self.backpressure_active = all_clusters_above_threshold

            if self.backpressure_active:
                logger.warning(f"Backpressure activated: ALL clusters exceed threshold {self.threshold}")
            elif any_cluster_available:
                logger.debug(f"At least one cluster is available, no backpressure")
            else:
                logger.debug(f"All clusters unavailable but not above threshold, no backpressure")

            return self.backpressure_active
        except Exception as e:
            logger.error(f"Error calculating backpressure: {e}")
            import traceback
            traceback.print_exc()
            # Default to no backpressure on error
            self.backpressure_active = False
            return False

    def get_backoff_time(self) -> float:
        """Get the recommended backoff time when backpressure is active."""
        if self.backpressure_active:
            # Fixed backoff time of 30 seconds when backpressure is active
            return 30.0
        return 0.0