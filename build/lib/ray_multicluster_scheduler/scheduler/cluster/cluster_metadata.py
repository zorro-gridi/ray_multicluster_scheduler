"""
Cluster metadata management.
"""

from ray_multicluster_scheduler.common.model import ClusterMetadata
from typing import List, Dict

class ClusterMetadataManager:
    """Manages static cluster metadata."""

    def __init__(self, cluster_configs: List[ClusterMetadata]):
        self.clusters = {cluster.name: cluster for cluster in cluster_configs}

    def get_cluster(self, name: str) -> ClusterMetadata:
        """Get cluster metadata by name."""
        return self.clusters.get(name)

    def list_clusters(self) -> List[ClusterMetadata]:
        """List all cluster metadata."""
        return list(self.clusters.values())

    def get_clusters_by_tag(self, tag: str) -> List[ClusterMetadata]:
        """Get clusters that have a specific tag."""
        return [cluster for cluster in self.clusters.values() if tag in cluster.tags]