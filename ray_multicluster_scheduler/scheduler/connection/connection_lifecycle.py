"""
Connection lifecycle management.
"""

import ray
import threading
import signal
import time
from typing import Dict, Optional, Any
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.connection.job_client_pool import JobClientPool
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager

logger = get_logger(__name__)


class ConnectionLifecycleManager:
    """Manages the lifecycle of connections to Ray clusters."""

    def __init__(self, client_pool: RayClientPool, initialize_job_client_pool_on_init: bool = False):
        self.client_pool = client_pool
        self._job_client_pool_initialized = False
        self._job_client_pool: Optional[JobClientPool] = None
        self.cluster_metadata: Dict[str, ClusterMetadata] = {}
        # 确保cluster_metadata永远不会为None，即使为空字典也始终存在
        self.config_manager = None  # Will be set when initialize_job_client_pool is called

        # 连接状态管理
        self._connection_states: Dict[str, str] = {}  # cluster_name -> state
        self._last_health_check: Dict[str, float] = {}  # cluster_name -> timestamp
        self._main_thread_id = threading.get_ident()  # 记录初始化时的主线程ID

        # 信号处理 - 只在主线程中设置，避免重复注册
        if threading.current_thread() is threading.main_thread():
            # 检查是否已经注册了信号处理器，避免重复注册
            if signal.getsignal(signal.SIGTERM) != signal.SIG_DFL:
                logger.debug("SIGTERM handler already registered, skipping")
            else:
                signal.signal(signal.SIGTERM, self._handle_sigterm)

            if signal.getsignal(signal.SIGINT) != signal.SIG_DFL:
                logger.debug("SIGINT handler already registered, skipping")
            else:
                signal.signal(signal.SIGINT, self._handle_sigterm)

            logger.info("SIGTERM and SIGINT handlers set in main thread")
        else:
            logger.warning(f"Current thread is not the main thread, SIGTERM handler is not set. Main thread ID: {self._main_thread_id}, Current thread ID: {threading.get_ident()}")

        # 根据参数决定是否初始化job_client_pool
        if initialize_job_client_pool_on_init:
            self._ensure_job_client_pool_initialized()

    def _handle_sigterm(self, signum, frame):
        """处理 SIGTERM 信号"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown()

    def _ensure_job_client_pool_initialized(self):
        """Ensure the Job client pool is initialized."""
        if not self._job_client_pool_initialized and self._job_client_pool is None:
            # Only initialize if config_manager is available
            if self.config_manager is None:
                logger.warning("Cannot initialize JobClientPool: config_manager not set")
                return

            from ray_multicluster_scheduler.scheduler.connection.job_client_pool import JobClientPool
            self._job_client_pool = JobClientPool(self.config_manager)

            # Add all existing clusters to the job client pool
            for cluster_name, cluster_metadata in self.cluster_metadata.items():
                try:
                    self._job_client_pool.add_cluster(cluster_metadata)
                except Exception as e:
                    logger.error(f"Failed to add cluster {cluster_name} to job client pool: {e}")

            logger.info(f"Initialized job client pool with {len(self.cluster_metadata)} clusters")
            self._job_client_pool_initialized = True

    def register_cluster(self, cluster_metadata: ClusterMetadata):
        """Register a cluster and establish a connection."""
        try:
            cluster_already_registered = cluster_metadata.name in self.cluster_metadata
            self.cluster_metadata[cluster_metadata.name] = cluster_metadata
            self.client_pool.add_cluster(cluster_metadata)
            # If job client pool exists, add cluster to it as well
            if self._job_client_pool:
                self._job_client_pool.add_cluster(cluster_metadata)

            # 初始化连接状态
            self._connection_states[cluster_metadata.name] = 'registered'

            if not cluster_already_registered:
                logger.info(f"Registered and connected to cluster {cluster_metadata.name}")
            else:
                logger.debug(f"Cluster {cluster_metadata.name} already registered, updating connection")
        except Exception as e:
            logger.error(f"Failed to register cluster {cluster_metadata.name}: {e}")
            raise

    def unregister_cluster(self, cluster_name: str):
        """Unregister a cluster and close its connection."""
        if cluster_name in self.cluster_metadata:
            del self.cluster_metadata[cluster_name]
            self.client_pool.remove_cluster(cluster_name)
            # If job client pool exists, remove cluster from it as well
            if self._job_client_pool:
                self._job_client_pool.remove_cluster(cluster_name)
            # 清理连接状态
            if cluster_name in self._connection_states:
                del self._connection_states[cluster_name]
            if cluster_name in self._last_health_check:
                del self._last_health_check[cluster_name]
            logger.info(f"Unregistered cluster {cluster_name}")

    def reconnect_cluster(self, cluster_name: str):
        """Reconnect to a cluster."""
        if cluster_name in self.cluster_metadata:
            cluster_metadata = self.cluster_metadata[cluster_name]
            self.client_pool.reconnect_cluster(cluster_name, cluster_metadata)
            # If job client pool exists, reconnect to it as well
            if self._job_client_pool:
                # For job client pool, we need to re-add the cluster
                self._job_client_pool.remove_cluster(cluster_name)
                self._job_client_pool.add_cluster(cluster_metadata)
            # 更新连接状态
            self._connection_states[cluster_name] = 'reconnected'
            logger.info(f"Reconnected to cluster {cluster_name}")
        else:
            logger.warning(f"Cannot reconnect to unknown cluster {cluster_name}")

    def get_connection(self, cluster_name: str) -> Optional[Any]:
        """Get a Ray connection to a specific cluster."""
        # 首先检查连接是否有效
        connection = self.client_pool.get_connection(cluster_name)
        if connection:
            return connection
        else:
            logger.warning(f"Connection to cluster {cluster_name} is invalid or not established")
            return None

    def get_job_client(self, cluster_name: str) -> Optional[Any]:
        """Get a JobSubmissionClient for a specific cluster."""
        # 在按需初始化job_client_pool之前，需要确保config_manager已设置
        # 从集群元数据中获取配置管理器
        if not self.cluster_metadata or cluster_name not in self.cluster_metadata:
            logger.warning(f"No cluster metadata found for {cluster_name}, cannot initialize job client")
            return None

        # 尝试从集群元数据获取配置，但实际的config_manager需要在调用initialize_job_client_pool时设置
        # 按需初始化job_client_pool
        self._ensure_job_client_pool_initialized()

        if not self._job_client_pool:
            logger.warning("Job client pool not initialized")
            return None

        job_client = self._job_client_pool.get_client(cluster_name)
        if job_client:
            return job_client
        else:
            logger.warning(f"Job client for cluster {cluster_name} is not available")
            return None

    def ensure_cluster_connection(self, cluster_name: str) -> bool:
        """Ensure we are connected to the specified cluster, connecting if necessary."""
        success = self.client_pool.ensure_cluster_connection(cluster_name)
        if success:
            self._connection_states[cluster_name] = 'connected'
        else:
            self._connection_states[cluster_name] = 'disconnected'
        return success

    def establish_ray_connection(self, cluster_name: str) -> bool:
        """Establish a Ray connection to the specified cluster using runtime_env from cluster config."""
        success = self.client_pool.establish_ray_connection(cluster_name)
        if success:
            self._connection_states[cluster_name] = 'connected'
        else:
            self._connection_states[cluster_name] = 'failed'
        return success

    def list_registered_clusters(self):
        """List all registered clusters."""
        return list(self.cluster_metadata.keys())

    def initialize_job_client_pool(self, config_manager):
        """Initialize the Job client pool."""
        # Update config_manager reference and ensure initialization
        self.config_manager = config_manager
        self._ensure_job_client_pool_initialized()

    def is_connection_healthy(self, cluster_name: str) -> bool:
        """Check if a Ray connection is healthy."""
        is_valid = self.client_pool.is_connection_valid(cluster_name)
        if is_valid:
            self._connection_states[cluster_name] = 'healthy'
        else:
            self._connection_states[cluster_name] = 'unhealthy'
        return is_valid

    def is_job_client_healthy(self, cluster_name: str) -> bool:
        """Check if a JobSubmissionClient is healthy."""
        # 按需初始化job_client_pool
        self._ensure_job_client_pool_initialized()

        if not self._job_client_pool:
            return False

        job_client = self._job_client_pool.get_client(cluster_name)
        if not job_client:
            return False

        try:
            # Try to get a simple status to verify the connection
            job_client.list_jobs()
            return True
        except Exception:
            return False

    def get_connection_age(self, cluster_name: str) -> float:
        """Get the age of a connection in seconds."""
        return self.client_pool.get_connection_age(cluster_name)

    def mark_cluster_connected(self, cluster_name: str):
        """标记集群为已连接状态"""
        self.client_pool.mark_cluster_connected(cluster_name)
        self._connection_states[cluster_name] = 'connected'

    def mark_cluster_disconnected(self, cluster_name: str):
        """标记集群为已断开连接状态"""
        self.client_pool.mark_cluster_disconnected(cluster_name)
        self._connection_states[cluster_name] = 'disconnected'

    def get_cluster_stats(self, cluster_name: str):
        """获取集群连接统计信息"""
        return self.client_pool.get_cluster_stats(cluster_name)

    def cleanup_expired_connections(self):
        """清理过期的连接"""
        self.client_pool.cleanup_expired_connections()

    def get_connection_state(self, cluster_name: str) -> str:
        """获取集群连接状态"""
        return self._connection_states.get(cluster_name, 'unknown')

    def force_shutdown_current_connection(self):
        """强制关闭当前的 Ray 连接"""
        try:
            if ray.is_initialized():
                logger.info("Shutting down current Ray connection")
                ray.shutdown()
                logger.info("Ray connection shut down successfully")
        except Exception as e:
            logger.error(f"Error during Ray shutdown: {e}")

    def shutdown(self):
        """关闭所有连接和清理资源"""
        logger.info("Initiating graceful shutdown of ConnectionLifecycleManager")

        # 关闭当前的 Ray 连接
        self.force_shutdown_current_connection()

        # 清理连接池
        for cluster_name in list(self.cluster_metadata.keys()):  # 使用list()创建副本以避免在迭代时修改字典
            try:
                self.client_pool.remove_cluster(cluster_name)
            except Exception as e:
                logger.warning(f"Error removing cluster {cluster_name} from pool during shutdown: {e}")

        # 清理job client pool
        if self._job_client_pool:
            try:
                for cluster_name in list(self._job_client_pool.clients.keys()):  # 使用list()创建副本以避免在迭代时修改字典
                    self._job_client_pool.remove_cluster(cluster_name)
            except Exception as e:
                logger.warning(f"Error cleaning up job client pool during shutdown: {e}")

        logger.info("ConnectionLifecycleManager shutdown completed")