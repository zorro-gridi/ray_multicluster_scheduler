"""
Health checker for Ray clusters.
"""

import time
import ray
from typing import Dict, List
from ray_multicluster_scheduler.common.model import ClusterMetadata, ResourceSnapshot
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import ClusterConnectionError

logger = get_logger(__name__)


class HealthChecker:
    """Checks the health and resource status of Ray clusters."""

    def __init__(self, cluster_metadata: List[ClusterMetadata]):
        self.cluster_metadata = cluster_metadata
        self.clients = {}
        self._initialize_clients()

    def _initialize_clients(self):
        """Initialize Ray clients for all clusters."""
        for cluster in self.cluster_metadata:
            try:
                # Connect to Ray cluster using ray.client() API
                # Note: In a real implementation, we would need to handle authentication
                # and secure connections properly
                client = ray.util.connect(
                    cluster.head_address,
                    ray_job_timeout=30
                )
                self.clients[cluster.name] = client
                logger.info(f"Connected to cluster {cluster.name} at {cluster.head_address}")
            except Exception as e:
                logger.error(f"Failed to connect to cluster {cluster.name}: {e}")
                # We'll retry connection later in the health check

    def check_health(self) -> Dict[str, ResourceSnapshot]:
        """Check the health of all clusters and return resource snapshots."""
        snapshots = {}

        for cluster in self.cluster_metadata:
            try:
                # Connect to the cluster and get resources
                if cluster.name not in self.clients:
                    # Try to reconnect if not connected
                    try:
                        client = ray.util.connect(
                            cluster.head_address,
                            ray_job_timeout=30
                        )
                        self.clients[cluster.name] = client
                        logger.info(f"Reconnected to cluster {cluster.name} at {cluster.head_address}")
                    except Exception as e:
                        logger.error(f"Failed to reconnect to cluster {cluster.name}: {e}")
                        continue

                # Get cluster resources by connecting to the specific cluster
                try:
                    # Connect to the specific cluster
                    ray.util.connect(cluster.head_address, ray_job_timeout=10)

                    # Get available resources
                    available_resources = ray.available_resources()

                    # Get cluster resources
                    cluster_resources = ray.cluster_resources()

                    # Get node count
                    nodes = ray.nodes()
                    node_count = len(nodes)

                    # Create resource snapshot
                    snapshot = ResourceSnapshot(
                        cluster_name=cluster.name,
                        available_resources=available_resources,
                        total_resources=cluster_resources,
                        node_count=node_count,
                        timestamp=time.time()
                    )

                    snapshots[cluster.name] = snapshot
                    logger.debug(f"Health check for {cluster.name}: {snapshot}")

                    # Disconnect to avoid conflicts
                    ray.util.disconnect()
                except Exception as e:
                    logger.error(f"Failed to get resources for cluster {cluster.name}: {e}")
                    if cluster.name in self.clients:
                        del self.clients[cluster.name]

            except Exception as e:
                logger.error(f"Health check failed for cluster {cluster.name}: {e}")
                # Remove the client if connection is broken
                if cluster.name in self.clients:
                    del self.clients[cluster.name]

        return snapshots

    def reconnect_cluster(self, cluster_name: str):
        """Attempt to reconnect to a cluster."""
        for cluster in self.cluster_metadata:
            if cluster.name == cluster_name:
                try:
                    # Disconnect any existing connection
                    try:
                        ray.util.disconnect()
                    except:
                        pass

                    client = ray.util.connect(
                        cluster.head_address,
                        ray_job_timeout=30
                    )
                    self.clients[cluster.name] = client
                    logger.info(f"Reconnected to cluster {cluster.name}")
                except Exception as e:
                    logger.error(f"Failed to reconnect to cluster {cluster.name}: {e}")
                    raise ClusterConnectionError(f"Could not reconnect to cluster {cluster.name}: {e}")