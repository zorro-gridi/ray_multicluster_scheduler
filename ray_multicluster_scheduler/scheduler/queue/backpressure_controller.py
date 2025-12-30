"""
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
            # Calculate overall resource utilization across all clusters
            # Using ResourceSnapshot data directly
            total_cpu_used = 0
            total_cpu_total = 0
            total_mem_used = 0
            total_mem_total = 0

            for snapshot in cluster_snapshots.values():
                # Use ResourceSnapshot fields directly
                total_cpu_used += snapshot.cluster_cpu_used_cores
                total_cpu_total += snapshot.cluster_cpu_total_cores
                total_mem_used += snapshot.cluster_mem_used_mb
                total_mem_total += snapshot.cluster_mem_total_mb

            # If no resources are available, skip calculation
            if total_cpu_total == 0 and total_mem_total == 0:
                self.backpressure_active = False
                return False

            # Calculate utilization ratios
            cpu_utilization = total_cpu_used / total_cpu_total if total_cpu_total > 0 else 0
            memory_utilization = total_mem_used / total_mem_total if total_mem_total > 0 else 0

            # Use the higher utilization of CPU or memory
            max_utilization = max(cpu_utilization, memory_utilization)

            # Apply backpressure if utilization exceeds threshold
            self.backpressure_active = max_utilization > self.threshold

            if self.backpressure_active:
                logger.warning(f"Backpressure activated: cluster utilization CPU={cpu_utilization:.2%}, "
                              f"Memory={memory_utilization:.2%} exceeds threshold {self.threshold}")
            else:
                logger.debug(f"Cluster utilization CPU={cpu_utilization:.2f}, Memory={memory_utilization:.2f} "
                            f"below threshold {self.threshold}, backpressure inactive")

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