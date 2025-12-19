"""
Score-based scheduling policy.
"""

from typing import Dict, List
from ray_multicluster_scheduler.common.model import TaskDescription, SchedulingDecision, ResourceSnapshot
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class ScoreBasedPolicy:
    """Scheduling policy based on resource availability scoring."""

    def __init__(self):
        pass

    def evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision:
        """Evaluate clusters based on resource availability and return the best one."""

        # Calculate scores for each cluster based on available resources
        scores = {}

        for cluster_name, snapshot in cluster_snapshots.items():
            # Calculate a simple score based on available CPU and memory
            # This is a basic implementation - a production system would have more sophisticated scoring
            cpu_score = snapshot.available_resources.get("CPU", 0)
            memory_score = snapshot.available_resources.get("memory", 0) / (1024 * 1024 * 1024)  # Convert to GB

            # Normalize scores (0-1 range) based on typical cluster sizes
            # These values would need to be tuned based on actual cluster sizes
            normalized_cpu_score = min(cpu_score / 32.0, 1.0)  # Assume 32 CPUs is a large cluster
            normalized_memory_score = min(memory_score / 128.0, 1.0)  # Assume 128GB is a large cluster

            # Combine scores (simple average)
            total_score = (normalized_cpu_score + normalized_memory_score) / 2.0
            scores[cluster_name] = total_score

        # Check if we have any clusters to evaluate
        if not scores:
            logger.warning("没有可用的集群进行评分评估")
            return SchedulingDecision(
                task_id=task_desc.task_id,
                cluster_name="",
                reason="没有可用的集群进行评分评估"
            )

        # Select the cluster with the highest score
        best_cluster = max(scores, key=scores.get)
        best_score = scores[best_cluster]

        logger.debug(f"Score-based policy evaluation: {scores}")

        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name=best_cluster,
            reason=f"Selected cluster {best_cluster} with score {best_score:.2f} based on resource availability"
        )