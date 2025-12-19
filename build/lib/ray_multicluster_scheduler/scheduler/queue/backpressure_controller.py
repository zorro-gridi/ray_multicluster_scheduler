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

    def __init__(self, threshold: float = 0.8):
        """
        Initialize the backpressure controller.

        Args:
            threshold: Resource utilization threshold (0.0-1.0) above which backpressure is applied.
                      Default is 0.8 (80%).
        """
        self.threshold = threshold
        self.last_check_time = 0
        self.backpressure_active = False

    def should_apply_backpressure(self, cluster_snapshots: Dict[str, ResourceSnapshot]) -> bool:
        """Determine if backpressure should be applied based on cluster resource utilization."""
        # Check if we should re-evaluate (at most once per second)
        current_time = time.time()
        if current_time - self.last_check_time < 1.0:
            return self.backpressure_active

        self.last_check_time = current_time

        try:
            # Calculate overall resource utilization across all clusters
            total_available_cpu = 0
            total_total_cpu = 0
            total_available_memory = 0
            total_total_memory = 0

            for snapshot in cluster_snapshots.values():
                # Sum up CPU resources
                cpu_available = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)

                # Sum up memory resources (in bytes)
                memory_available = snapshot.available_resources.get("memory", 0)
                memory_total = snapshot.total_resources.get("memory", 0)

                total_available_cpu += cpu_available
                total_total_cpu += cpu_total
                total_available_memory += memory_available
                total_total_memory += memory_total

            # If no resources are available, skip calculation
            if total_total_cpu == 0 and total_total_memory == 0:
                self.backpressure_active = False
                return False

            # Calculate utilization ratios
            cpu_utilization = 1.0 - (total_available_cpu / total_total_cpu) if total_total_cpu > 0 else 0
            memory_utilization = 1.0 - (total_available_memory / total_total_memory) if total_total_memory > 0 else 0

            # Use the higher utilization of CPU or memory
            max_utilization = max(cpu_utilization, memory_utilization)

            # Apply backpressure if utilization exceeds threshold
            self.backpressure_active = max_utilization > self.threshold

            if self.backpressure_active:
                logger.warning(f"Backpressure activated: cluster utilization CPU={cpu_utilization:.2f}, "
                              f"Memory={memory_utilization:.2f} exceeds threshold {self.threshold}")
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
            # Exponential backoff starting at 0.1 seconds
            # In a real implementation, this could be more sophisticated
            return 0.1 * (2 ** int(time.time() % 5))  # Vary backoff time
        return 0.0