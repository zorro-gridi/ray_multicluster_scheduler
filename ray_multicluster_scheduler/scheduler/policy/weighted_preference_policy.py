"""
Weighted preference scheduling policy.
"""

from typing import Dict, List
from ray_multicluster_scheduler.common.model import TaskDescription, SchedulingDecision, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class WeightedPreferencePolicy:
    """Scheduling policy based on cluster weights and resource availability."""

    def __init__(self, cluster_metadata: Dict[str, ClusterMetadata]):
        self.cluster_metadata = cluster_metadata

    def evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision:
        """Evaluate clusters based on weights and resource availability."""

        # Calculate weighted scores for each cluster
        scores = {}

        for cluster_name, snapshot in cluster_snapshots.items():
            if cluster_name not in self.cluster_metadata:
                continue

            cluster_meta = self.cluster_metadata[cluster_name]

            # Calculate resource availability score (0-1)
            cpu_available = snapshot.available_resources.get("CPU", 0)
            cpu_total = snapshot.total_resources.get("CPU", 0)
            cpu_score = min(cpu_available / cpu_total, 1.0) if cpu_total > 0 else 0.0

            # Calculate memory availability score (0-1)
            memory_available = snapshot.available_resources.get("memory", 0)
            memory_total = snapshot.total_resources.get("memory", 0)
            memory_score = min(memory_available / memory_total, 1.0) if memory_total > 0 else 0.0

            # Combine resource scores
            resource_score = (cpu_score + memory_score) / 2.0

            # Apply cluster weight
            weighted_score = resource_score * cluster_meta.weight

            scores[cluster_name] = weighted_score

        if not scores:
            return SchedulingDecision(
                task_id=task_desc.task_id,
                cluster_name="",
                reason="No suitable clusters found for weighted preference policy"
            )

        # Select the cluster with the highest weighted score
        best_cluster = max(scores, key=scores.get)
        best_score = scores[best_cluster]

        logger.debug(f"Weighted preference policy evaluation: {scores}")

        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name=best_cluster,
            reason=f"Selected cluster {best_cluster} with weighted score {best_score:.2f} (weight: {self.cluster_metadata[best_cluster].weight})"
        )