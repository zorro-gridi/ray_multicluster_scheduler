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
            # Calculate available resources from new metrics
            cpu_available = snapshot.cluster_cpu_total_cores - snapshot.cluster_cpu_used_cores
            memory_available_gb = (snapshot.cluster_mem_total_mb - snapshot.cluster_mem_used_mb) / 1024.0  # Convert MB to GB

            # Use absolute resource values for scoring instead of normalized values
            # This approach values actual resource availability rather than relative usage
            total_score = cpu_available + memory_available_gb  # Simple combination of absolute available resources
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