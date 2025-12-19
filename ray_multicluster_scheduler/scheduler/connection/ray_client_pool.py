import ray
import time
from typing import Dict, Optional
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import ClusterConnectionError

logger = get_logger(__name__)


class RayClientPool:
    """Manages a pool of Ray client connections to different clusters."""

    def __init__(self):
        self.connections: Dict[str, any] = {}
        self.active_connections: Dict[str, bool] = {}
        # 存储连接时间戳，用于检测连接是否过期
        self.connection_timestamps: Dict[str, float] = {}

    def add_cluster(self, cluster_metadata: ClusterMetadata):
        """Add a cluster to the connection pool."""
        try:
            # Connect to the remote Ray cluster using Ray client
            # Format: ray://<head_node_host>:<port>
            ray_client_address = f"ray://{cluster_metadata.head_address}"

            # 检查是否已经存在连接
            if cluster_metadata.name in self.connections:
                # 如果已有连接，先断开
                try:
                    ray.shutdown()
                except:
                    pass

            # 建立新连接
            # 注意：这里我们不直接调用ray.init()，而是存储连接信息
            # 实际的连接将在需要时建立
            self.connections[cluster_metadata.name] = ray_client_address
            self.active_connections[cluster_metadata.name] = True
            self.connection_timestamps[cluster_metadata.name] = time.time()
            logger.info(f"Added cluster {cluster_metadata.name} to connection pool")
        except Exception as e:
            logger.error(f"Failed to add cluster {cluster_metadata.name} to connection pool: {e}")
            raise ClusterConnectionError(f"Could not connect to cluster {cluster_metadata.name}: {e}")

    def get_connection(self, cluster_name: str) -> Optional[any]:
        """Get a Ray client connection for a specific cluster."""
        if cluster_name in self.connections and self.active_connections.get(cluster_name, False):
            return self.connections[cluster_name]
        return None

    def release_connection(self, cluster_name: str):
        """Release a connection back to the pool."""
        # In this simple implementation, we don't actually close connections
        # but mark them as inactive if needed
        if cluster_name in self.connections:
            # Mark as inactive if needed
            # In a more sophisticated implementation, we might actually close the connection
            # or return it to a pool of available connections
            pass

    def remove_cluster(self, cluster_name: str):
        """Remove a cluster from the connection pool."""
        if cluster_name in self.connections:
            try:
                # Disconnect from the cluster
                ray.shutdown()
                del self.connections[cluster_name]
                if cluster_name in self.active_connections:
                    del self.active_connections[cluster_name]
                if cluster_name in self.connection_timestamps:
                    del self.connection_timestamps[cluster_name]
                logger.info(f"Removed cluster {cluster_name} from connection pool")
            except Exception as e:
                logger.error(f"Error removing cluster {cluster_name} from connection pool: {e}")

    def reconnect_cluster(self, cluster_name: str, cluster_metadata: ClusterMetadata):
        """Reconnect to a cluster."""
        # Remove the old connection if it exists
        if cluster_name in self.connections:
            self.remove_cluster(cluster_name)

        # Add the cluster again
        self.add_cluster(cluster_metadata)

    def is_connection_valid(self, cluster_name: str) -> bool:
        """Check if a connection is still valid."""
        try:
            # 检查连接是否存在
            if cluster_name not in self.connections or not self.active_connections.get(cluster_name, False):
                return False

            # 对于每个集群，我们需要单独检查其连接状态
            # 这里我们假设连接有效，实际验证将在任务提交时进行
            return True
        except Exception as e:
            logger.warning(f"Connection to cluster {cluster_name} is invalid: {e}")
            return False

    def get_connection_age(self, cluster_name: str) -> float:
        """Get the age of a connection in seconds."""
        if cluster_name in self.connection_timestamps:
            return time.time() - self.connection_timestamps[cluster_name]
        return float('inf')  # 表示连接不存在