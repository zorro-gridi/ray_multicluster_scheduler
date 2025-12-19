"""
Tag affinity scheduling policy.
"""

from typing import Dict, List
from ray_multicluster_scheduler.common.model import TaskDescription, SchedulingDecision, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TagAffinityPolicy:
    """Scheduling policy based on tag affinity."""

    def __init__(self, cluster_metadata: Dict[str, ClusterMetadata]):
        self.cluster_metadata = cluster_metadata

    def evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision:
        """Evaluate clusters based on tag affinity and return the best one."""

        # If task has no tags, we can't apply tag affinity
        if not task_desc.tags:
            # Return a decision with no specific cluster recommendation
            return SchedulingDecision(
                task_id=task_desc.task_id,
                cluster_name="",
                reason="Task has no tags for affinity matching"
            )

        # Find clusters that match task tags
        matching_clusters = []

        for cluster_name in cluster_snapshots.keys():
            if cluster_name in self.cluster_metadata:
                cluster_meta = self.cluster_metadata[cluster_name]
                # Check if any task tag matches any cluster tag
                if any(tag in cluster_meta.tags for tag in task_desc.tags):
                    matching_clusters.append(cluster_name)

        if not matching_clusters:
            # No clusters match the task tags
            return SchedulingDecision(
                task_id=task_desc.task_id,
                cluster_name="",
                reason=f"No clusters found matching task tags {task_desc.tags}"
            )

        # If we have matching clusters, select the first one
        # In a more sophisticated implementation, we might combine this with other policies
        selected_cluster = matching_clusters[0]

        logger.debug(f"Tag affinity policy evaluation: task tags {task_desc.tags} matched clusters {matching_clusters}")

        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name=selected_cluster,
            reason=f"Selected cluster {selected_cluster} due to tag affinity with task tags {task_desc.tags}"
        )