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

            # Calculate resource availability using absolute values
            cpu_used_cores = snapshot.cluster_cpu_used_cores
            cpu_total_cores = snapshot.cluster_cpu_total_cores
            cpu_available = cpu_total_cores - cpu_used_cores

            # Calculate memory availability using absolute values
            mem_used_mb = snapshot.cluster_mem_used_mb
            mem_total_mb = snapshot.cluster_mem_total_mb
            memory_available = mem_total_mb - mem_used_mb
            memory_available_gib = memory_available / 1024.0  # Convert MB to GiB

            # Combine absolute resource values with weights
            resource_score = cpu_available + memory_available_gib  # Sum of absolute available resources

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