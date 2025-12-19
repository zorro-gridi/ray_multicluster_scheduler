"""
Connection lifecycle management.
"""

from typing import Dict
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class ConnectionLifecycleManager:
    """Manages the lifecycle of connections to Ray clusters."""

    def __init__(self, client_pool: RayClientPool):
        self.client_pool = client_pool
        self.cluster_metadata: Dict[str, ClusterMetadata] = {}

    def register_cluster(self, cluster_metadata: ClusterMetadata):
        """Register a cluster and establish a connection."""
        try:
            self.cluster_metadata[cluster_metadata.name] = cluster_metadata
            self.client_pool.add_cluster(cluster_metadata)
            logger.info(f"Registered and connected to cluster {cluster_metadata.name}")
        except Exception as e:
            logger.error(f"Failed to register cluster {cluster_metadata.name}: {e}")
            raise

    def unregister_cluster(self, cluster_name: str):
        """Unregister a cluster and close its connection."""
        if cluster_name in self.cluster_metadata:
            del self.cluster_metadata[cluster_name]
            self.client_pool.remove_cluster(cluster_name)
            logger.info(f"Unregistered cluster {cluster_name}")

    def reconnect_cluster(self, cluster_name: str):
        """Reconnect to a cluster."""
        if cluster_name in self.cluster_metadata:
            cluster_metadata = self.cluster_metadata[cluster_name]
            self.client_pool.reconnect_cluster(cluster_name, cluster_metadata)
            logger.info(f"Reconnected to cluster {cluster_name}")
        else:
            logger.warning(f"Cannot reconnect to unknown cluster {cluster_name}")

    def get_connection(self, cluster_name: str):
        """Get a connection to a specific cluster."""
        # 首先检查连接是否有效
        if self.client_pool.is_connection_valid(cluster_name):
            return self.client_pool.get_connection(cluster_name)
        else:
            logger.warning(f"Connection to cluster {cluster_name} is invalid, attempting to reconnect...")
            try:
                self.reconnect_cluster(cluster_name)
                return self.client_pool.get_connection(cluster_name)
            except Exception as e:
                logger.error(f"Failed to reconnect to cluster {cluster_name}: {e}")
                return None

    def list_registered_clusters(self):
        """List all registered clusters."""
        return list(self.cluster_metadata.keys())

    def is_connection_healthy(self, cluster_name: str) -> bool:
        """Check if a connection is healthy."""
        return self.client_pool.is_connection_valid(cluster_name)

    def get_connection_age(self, cluster_name: str) -> float:
        """Get the age of a connection in seconds."""
        return self.client_pool.get_connection_age(cluster_name)