"""
Cluster registry that combines static metadata with dynamic resource snapshots.
"""

from typing import Dict, List, Optional
from ray_multicluster_scheduler.common.model import ClusterMetadata, ResourceSnapshot, SchedulingDecision
from ray_multicluster_scheduler.scheduler.cluster.cluster_metadata import ClusterMetadataManager
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class ClusterRegistry:
    """Registry that maintains cluster metadata and resource snapshots."""

    def __init__(self, metadata_manager: ClusterMetadataManager, health_checker: HealthChecker):
        self.metadata_manager = metadata_manager
        self.health_checker = health_checker
        self.resource_snapshots: Dict[str, ResourceSnapshot] = {}

    def get_cluster_metadata(self, name: str) -> Optional[ClusterMetadata]:
        """Get cluster metadata by name."""
        return self.metadata_manager.get_cluster(name)

    def get_resource_snapshot(self, name: str) -> Optional[ResourceSnapshot]:
        """Get the latest resource snapshot for a cluster."""
        return self.resource_snapshots.get(name)

    def get_all_cluster_info(self) -> Dict[str, Dict]:
        """Get combined metadata and resource snapshot for all clusters."""
        result = {}
        for cluster in self.metadata_manager.list_clusters():
            result[cluster.name] = {
                'metadata': cluster,
                'snapshot': self.resource_snapshots.get(cluster.name)
            }
        return result

    def get_healthy_clusters(self) -> List[str]:
        """Get names of clusters with recent resource snapshots."""
        # Clusters with snapshots from the last 2 minutes are considered healthy
        healthy_clusters = []
        for cluster in self.metadata_manager.list_clusters():
            snapshot = self.resource_snapshots.get(cluster.name)
            if snapshot:
                healthy_clusters.append(cluster.name)
        return healthy_clusters

    def select_cluster_for_task(self, task_tags: List[str]) -> Optional[SchedulingDecision]:
        """Select the best cluster for a task based on tags."""
        # Simple selection algorithm:
        # 1. If task has tags, find clusters with matching tags
        # 2. Among matching clusters, select the one with the most available resources
        # 3. If no tagged clusters are available, select any healthy cluster

        candidate_clusters = []

        if task_tags:
            # Find clusters with matching tags
            for tag in task_tags:
                tagged_clusters = self.metadata_manager.get_clusters_by_tag(tag)
                for cluster in tagged_clusters:
                    if cluster.name in self.get_healthy_clusters():
                        candidate_clusters.append(cluster.name)

        # If no tagged clusters found, consider all healthy clusters
        if not candidate_clusters:
            candidate_clusters = self.get_healthy_clusters()

        # If still no candidates, return None
        if not candidate_clusters:
            return None

        # Select the cluster with highest weight among candidates
        best_cluster = None
        best_weight = -1

        for cluster_name in candidate_clusters:
            metadata = self.metadata_manager.get_cluster(cluster_name)
            if metadata.weight > best_weight:
                best_weight = metadata.weight
                best_cluster = cluster_name

        if best_cluster:
            return SchedulingDecision(
                task_id="",  # Will be filled by caller
                cluster_name=best_cluster,
                reason=f"Selected cluster {best_cluster} based on tags {task_tags} and weight preference"
            )

        return None