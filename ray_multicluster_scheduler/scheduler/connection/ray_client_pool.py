import ray
import time
import logging
import threading
from typing import Dict, Optional, Any, Callable
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import ClusterConnectionError
from ray_multicluster_scheduler.control_plane.config import ConfigManager
import ray.exceptions

logger = get_logger(__name__)


class RayClientPool:
    """Manages a pool of Ray client connections to different clusters."""

    def __init__(self, config_manager: ConfigManager):
        self.connections: Dict[str, Any] = {}
        self.active_connections: Dict[str, bool] = {}
        # 存储连接时间戳，用于检测连接是否过期
        self.connection_timestamps: Dict[str, float] = {}
        # 存储每个集群的连接状态
        self.cluster_states: Dict[str, Dict] = {}
        # 连接超时时间（秒）
        self.connection_timeout = 300  # 5分钟
        self.config_manager = config_manager
        # 跟踪当前连接的集群
        self.current_cluster = None

        # 添加主线程检查
        self.main_thread_id = threading.get_ident()
        self._main_thread_only = True  # 标记是否只允许在主线程中执行连接操作

        # 添加线程锁保护连接操作
        self._lock = threading.RLock()

    def add_cluster(self, cluster_metadata: ClusterMetadata):
        """Add a cluster to the connection pool."""
        try:
            # Connect to the remote Ray cluster using Ray client
            # Format: ray://<head_node_host>:<port>
            ray_client_address = f"ray://{cluster_metadata.head_address}"

            # 检查是否已经存在连接
            cluster_already_exists = cluster_metadata.name in self.connections
            if cluster_already_exists:
                logger.debug(f"Cluster {cluster_metadata.name} already in pool, updating configuration")

            # 存储连接信息，但不立即建立连接
            # 实际的连接将在需要时建立
            self.connections[cluster_metadata.name] = {
                'address': ray_client_address,
                'metadata': cluster_metadata,
                'connected': False,
                'client': None,
                'last_used': time.time()
            }
            self.active_connections[cluster_metadata.name] = True
            self.connection_timestamps[cluster_metadata.name] = time.time()
            self.cluster_states[cluster_metadata.name] = {
                'last_used': time.time(),
                'connection_count': 0,
                'success_count': 0,
                'failure_count': 0
            }

            if not cluster_already_exists:
                logger.info(f"Added cluster {cluster_metadata.name} to connection pool")
        except Exception as e:
            logger.error(f"Failed to add cluster {cluster_metadata.name} to connection pool: {e}")
            raise ClusterConnectionError(f"Could not connect to cluster {cluster_metadata.name}: {e}")

    def get_connection(self, cluster_name: str) -> Optional[Any]:
        """Get a Ray client connection for a specific cluster."""
        if cluster_name in self.connections and self.active_connections.get(cluster_name, False):
            connection_info = self.connections[cluster_name]
            # 检查连接是否有效
            if self.is_connection_valid(cluster_name):
                # 更新最后使用时间
                connection_info['last_used'] = time.time()
                if cluster_name in self.cluster_states:
                    self.cluster_states[cluster_name]['last_used'] = time.time()
                return connection_info
            else:
                # 连接无效，需要重新建立
                logger.warning(f"Connection to cluster {cluster_name} is invalid, will reconnect on demand")
                return None
        return None

    def establish_ray_connection(self, cluster_name: str) -> bool:
        """Establish a Ray connection to the specified cluster using runtime_env from cluster config."""
        if cluster_name not in self.connections:
            logger.error(f"Cluster {cluster_name} not found in connection pool")
            return False

        # 检查当前线程是否为主线程
        current_thread_id = threading.get_ident()
        if current_thread_id != self.main_thread_id:
            logger.warning(f"Ray connection being established from non-main thread. Main thread ID: {self.main_thread_id}, Current thread ID: {current_thread_id}")
            logger.warning("SIGTERM handler will not be set in non-main thread")

        try:
            # 首先检查当前是否已连接到其他集群，如果有则断开
            if self.current_cluster is not None and self.current_cluster != cluster_name:
                logger.info(f"Disconnecting from cluster {self.current_cluster} before connecting to {cluster_name}")
                try:
                    if ray.is_initialized():
                        ray.shutdown()
                except Exception as shutdown_error:
                    logger.warning(f"Error during ray shutdown: {shutdown_error}")

            connection_info = self.connections[cluster_name]
            ray_address = connection_info['address']

            # 从集群管理器获取runtime_env
            runtime_env = None
            cluster_config = self.config_manager.get_cluster_config(cluster_name)
            if cluster_config and hasattr(cluster_config, 'runtime_env'):
                runtime_env = cluster_config.runtime_env

            # 初始化Ray连接，支持从集群配置获取的runtime_env参数
            if runtime_env:
                ray.init(
                    address=ray_address,
                    runtime_env=runtime_env,
                    ignore_reinit_error=True,
                    logging_level=logging.WARNING
                )
            else:
                ray.init(
                    address=ray_address,
                    ignore_reinit_error=True,
                    logging_level=logging.WARNING
                )

            # 等待连接稳定
            time.sleep(0.5)

            if ray.is_initialized():
                connection_info['connected'] = True
                connection_info['last_used'] = time.time()
                self.current_cluster = cluster_name  # 记录当前连接的集群
                logger.info(f"Successfully connected to cluster {cluster_name}")
                return True
            else:
                logger.error(f"Failed to initialize connection to cluster {cluster_name}")
                self.current_cluster = None  # 重置current_cluster
                return False

        except ray.exceptions.RaySystemError as e:
            logger.error(f"Ray system error when connecting to cluster {cluster_name}: {e}")
            # 如果是客户端已经断开的错误，尝试清理连接后重试
            if "already been disconnected" in str(e) or "reconnect a session that has already been cleaned up" in str(e):
                logger.info(f"Attempting to clean up and reconnect to cluster {cluster_name}")
                try:
                    if ray.is_initialized():
                        ray.shutdown()
                except:
                    pass  # 如果shutdown失败，继续尝试
                # 更新连接状态
                self.mark_cluster_disconnected(cluster_name)
            self.current_cluster = None  # 重置current_cluster
            return False
        except Exception as e:
            logger.error(f"Failed to establish connection to cluster {cluster_name}: {e}")
            import traceback
            traceback.print_exc()
            # 检查是否是连接相关的特定错误
            if "already been disconnected" in str(e) or "reconnect a session that has already been cleaned up" in str(e):
                logger.info(f"Cleaning up connection state for cluster {cluster_name}")
                try:
                    if ray.is_initialized():
                        ray.shutdown()
                except:
                    pass  # 如果shutdown失败，继续尝试
                # 更新连接状态
                self.mark_cluster_disconnected(cluster_name)
            self.current_cluster = None  # 重置current_cluster
            return False

    def ensure_cluster_connection(self, cluster_name: str) -> bool:
        """Ensure we are connected to the specified cluster, connecting if necessary."""
        with self._lock:  # 使用锁保护整个操作
            if cluster_name not in self.connections:
                logger.error(f"Cluster {cluster_name} not found in connection pool")
                return False

            # 检查是否已连接到目标集群
            if self.current_cluster == cluster_name:
                # 已经连接到正确的集群，检查连接是否仍然有效
                connection_info = self.connections[cluster_name]
                if connection_info.get('connected', False):
                    # 验证连接健康状态
                    if self._verify_connection_health(cluster_name):
                        # 更新最后使用时间
                        connection_info['last_used'] = time.time()
                        if cluster_name in self.cluster_states:
                            self.cluster_states[cluster_name]['last_used'] = time.time()
                        return True
                    else:
                        # 连接不健康，标记为未连接
                        logger.warning(f"Connection to cluster {cluster_name} is unhealthy, will reconnect")
                        connection_info['connected'] = False

            # 需要连接到指定集群
            return self.establish_ray_connection(cluster_name)


    def release_connection(self, cluster_name: str):
        """Release a connection back to the pool."""
        # 更新连接使用统计
        if cluster_name in self.cluster_states:
            self.cluster_states[cluster_name]['last_used'] = time.time()

    def remove_cluster(self, cluster_name: str):
        """Remove a cluster from the connection pool."""
        if cluster_name in self.connections:
            try:
                # 如果当前连接的是此集群，断开连接
                if self.current_cluster == cluster_name:
                    if ray.is_initialized():
                        ray.shutdown()
                        logger.info(f"Disconnected from cluster {cluster_name} during removal")
                        self.current_cluster = None

                del self.connections[cluster_name]
                if cluster_name in self.active_connections:
                    del self.active_connections[cluster_name]
                if cluster_name in self.connection_timestamps:
                    del self.connection_timestamps[cluster_name]
                if cluster_name in self.cluster_states:
                    del self.cluster_states[cluster_name]
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

            connection_info = self.connections[cluster_name]

            # 检查连接是否超时
            if time.time() - connection_info['last_used'] > self.connection_timeout:
                logger.info(f"Connection to cluster {cluster_name} timed out, marking as invalid")
                return False

            # 快速检查：如果连接标记为未连接，直接返回False
            is_connected = connection_info.get('connected', False)
            if not is_connected:
                return False

            # 只有当前集群才需要检查Ray是否初始化
            if cluster_name == self.current_cluster:
                if not ray.is_initialized():
                    logger.debug(f"Connection to cluster {cluster_name} marked as connected but Ray is not active")
                    connection_info['connected'] = False
                    return False
                # 验证连接健康状态
                return self._verify_connection_health(cluster_name)
            else:
                # 非当前集群的连接标记为无效
                return False

        except Exception as e:
            logger.warning(f"Connection to cluster {cluster_name} is invalid: {e}")
            return False

    def get_connection_age(self, cluster_name: str) -> float:
        """Get the age of a connection in seconds."""
        if cluster_name in self.connection_timestamps:
            return time.time() - self.connection_timestamps[cluster_name]
        return float('inf')  # 表示连接不存在

    def mark_cluster_connected(self, cluster_name: str):
        """标记集群为已连接状态"""
        if cluster_name in self.connections:
            self.connections[cluster_name]['connected'] = True
            self.connections[cluster_name]['last_used'] = time.time()
            if cluster_name in self.cluster_states:
                self.cluster_states[cluster_name]['connection_count'] += 1
                self.cluster_states[cluster_name]['success_count'] += 1

    def mark_cluster_disconnected(self, cluster_name: str):
        """标记集群为已断开连接状态"""
        if cluster_name in self.connections:
            self.connections[cluster_name]['connected'] = False
            if cluster_name in self.cluster_states:
                self.cluster_states[cluster_name]['failure_count'] += 1

    def get_cluster_stats(self, cluster_name: str) -> Dict:
        """获取集群连接统计信息"""
        if cluster_name in self.cluster_states:
            return self.cluster_states[cluster_name].copy()
        return {}

    def cleanup_expired_connections(self):
        """清理过期的连接"""
        current_time = time.time()
        expired_clusters = []

        for cluster_name, connection_info in self.connections.items():
            if current_time - connection_info['last_used'] > self.connection_timeout:
                expired_clusters.append(cluster_name)

        for cluster_name in expired_clusters:
            logger.info(f"Cleaning up expired connection for cluster {cluster_name}")
            self.mark_cluster_disconnected(cluster_name)

    def _verify_connection_health(self, cluster_name: str) -> bool:
        """验证连接是否真正有效"""
        try:
            if cluster_name == self.current_cluster and ray.is_initialized():
                # 尝试执行简单操作验证连接
                ray.get_runtime_context()
                return True
        except Exception as e:
            logger.warning(f"Connection health check failed for {cluster_name}: {e}")
            self.mark_cluster_disconnected(cluster_name)
        return False

    def _reset_current_cluster_if_invalid(self):
        """如果当前集群连接无效，重置current_cluster"""
        if self.current_cluster:
            if not self._verify_connection_health(self.current_cluster):
                logger.warning(f"Current cluster {self.current_cluster} is invalid, resetting")
                self.current_cluster = None