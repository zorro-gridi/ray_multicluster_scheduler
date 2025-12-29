"""
Cluster monitor that integrates configuration management, metadata management, and health checking.
"""

import time
import os
from typing import Optional, Dict, Any, List
from ray_multicluster_scheduler.common.model import ResourceSnapshot, ClusterHealth
from ray_multicluster_scheduler.common.logging import get_logger

from ray_multicluster_scheduler.common.model import ClusterMetadata, ResourceSnapshot, ClusterHealth
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker

logger = get_logger(__name__)

# Cache timeout in seconds
CACHE_TIMEOUT = 10.0  # Cache snapshots for 10 seconds to reduce HealthChecker calls


class ClusterMonitor:
    """Monitors cluster health and manages cluster registry."""

    def __init__(self, config_file_path: Optional[str] = None):
        """
        Initialize the cluster monitor.

        Args:
            config_file_path (str, optional): Path to the cluster configuration YAML file.
                If not provided, the system will attempt to locate the configuration file
                in common locations or fall back to default configuration.
        """
        # Load cluster configurations from file if provided
        if not config_file_path:
            # Try to load default configuration file
            default_config_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "clusters.yaml")
            self.config_manager = ConfigManager(default_config_path)
            if os.path.exists(default_config_path):
                logger.info(f"📁 加载默认集群配置文件: {default_config_path}")
            else:
                logger.info("📁 使用默认集群配置")
                # 添加默认的测试集群配置
                self.config_manager = ConfigManager()
        else:
            self.config_manager = ConfigManager(config_file_path)

        self.cluster_metadata =  self.config_manager.get_cluster_configs()
        # Initialize cluster manager
        from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager
        self.cluster_manager = ClusterManager(self.cluster_metadata)

        # Initialize client pool
        from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
        self.client_pool = RayClientPool(self.config_manager)

        # Initialize health checker once
        self.health_checker = self._create_health_checker()

        # Initialize health status for all clusters
        for name in self.cluster_manager.clusters:
            self.cluster_manager.health_status[name] = ClusterHealth()

        # Initialize cache for resource snapshots to avoid repeated HealthChecker calls
        self._cached_snapshots = {}
        self._last_cache_update = 0.0

    def _create_health_checker(self) -> HealthChecker:
        """Create a HealthChecker instance with current cluster metadata and client pool."""
        # 获取所有集群的元数据
        cluster_metadata_list = []
        for name, config in self.cluster_manager.clusters.items():
            cluster_metadata = ClusterMetadata(
                name=config.name,
                head_address=config.head_address,
                dashboard=config.dashboard,
                prefer=config.prefer,
                weight=config.weight,
                runtime_env=config.runtime_env,
                tags=config.tags
            )
            cluster_metadata_list.append(cluster_metadata)
        logger.info("✅ 集群监视器初始化成功")
        # 创建HealthChecker实例
        return HealthChecker(cluster_metadata_list, self.client_pool)


    def get_all_cluster_info(self) -> Dict[str, Dict[str, Any]]:
        """Get combined metadata and resource snapshot for all clusters."""
        # 获取最新的资源快照
        snapshots = self.get_latest_snapshots()

        result = {}

        # 如果没有集群配置，尝试重新加载
        if not self.cluster_manager.clusters:
            logger.warning("没有找到集群配置，尝试重新加载默认配置")
            self.config_manager.get_cluster_configs()

        for name, config in self.cluster_manager.clusters.items():
            # 获取最新的快照
            snapshot = snapshots.get(name)

            # 如果没有快照，则创建一个默认快照
            if not snapshot:
                snapshot = ResourceSnapshot(
                    cluster_name=name,
                    cluster_cpu_usage_percent=0.0,
                    cluster_mem_usage_percent=0.0,
                    cluster_cpu_used_cores=0.0,
                    cluster_cpu_total_cores=0.0,
                    cluster_mem_used_mb=0.0,
                    cluster_mem_total_mb=0.0,
                    node_count=0,
                    timestamp=time.time(),
                    node_stats=None
                )

            result[name] = {
                'metadata': config,
                'snapshot': snapshot
            }

        return result

    def get_resource_snapshot(self, cluster_name: str) -> Optional[ResourceSnapshot]:
        """Get the latest resource snapshot for a cluster."""
        # 获取最新的资源快照
        snapshots = self.get_latest_snapshots()
        return snapshots.get(cluster_name)

    def update_resource_snapshot(self, cluster_name: str, snapshot: ResourceSnapshot):
        """Update the resource snapshot for a cluster."""
        # 在简化版本中，我们不再维护全局快照存储
        # 这个方法保留是为了兼容性，但实际的快照获取将在调用时直接从HealthChecker获取
        pass

    def get_latest_snapshots(self):
        """Get the latest resource snapshots for all clusters directly from HealthChecker with caching."""

        # Check if we have a recent cache
        current_time = time.time()

        # Check if we have cached snapshots and they are still valid
        if (current_time - self._last_cache_update) < CACHE_TIMEOUT and self._cached_snapshots:
            # Check if all clusters are over the resource threshold
            all_clusters_over_threshold = True

            # Check if any cluster has resources available
            for cluster_name, snapshot in self._cached_snapshots.items():
                # 使用新的资源指标
                cpu_used_cores = snapshot.cluster_cpu_used_cores
                cpu_total_cores = snapshot.cluster_cpu_total_cores
                cpu_utilization = cpu_used_cores / cpu_total_cores if cpu_total_cores > 0 else 0

                # 检查内存使用率
                mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                # GPU资源暂时不可用，使用默认值
                gpu_utilization = 0

                # 检查是否任一资源使用率未超过阈值
                if cpu_utilization <= 0.8 and gpu_utilization <= 0.8 and mem_utilization <= 0.8:
                    all_clusters_over_threshold = False
                    break

            # 如果所有集群都超过阈值（即没有可用资源），则强制刷新资源状态
            if all_clusters_over_threshold:
                logger.debug("所有集群资源都超过阈值，强制刷新资源状态")
            else:
                logger.debug(f"使用缓存的快照 (年龄: {current_time - self._last_cache_update:.2f}s)")
                return self._cached_snapshots

        # 获取所有集群的元数据
        cluster_metadata_list = []
        for name, config in self.cluster_manager.clusters.items():
            cluster_metadata = ClusterMetadata(
                name=config.name,
                head_address=config.head_address,
                dashboard=config.dashboard,
                prefer=config.prefer,
                weight=config.weight,
                runtime_env=config.runtime_env,
                tags=config.tags
            )
            cluster_metadata_list.append(cluster_metadata)

        # 使用已初始化的HealthChecker获取最新的快照
        # 重新设置health_checker的cluster_metadata，因为集群配置可能已更新
        self.health_checker.cluster_metadata = cluster_metadata_list
        snapshots = self.health_checker.check_health()

        # Update cache
        self._cached_snapshots = snapshots
        self._last_cache_update = current_time

        return snapshots

    def _initialize_cluster_snapshots(self):
        """Initialize cluster snapshots if needed. In simplified version, this is a no-op."""
        logger.info("Cluster snapshots initialization is simplified - snapshots are fetched on demand")


    def get_best_cluster(self, requirements: Optional[Dict] = None) -> str:
        """Get the best cluster based on current health and requirements."""
        return self.cluster_manager.get_best_cluster(requirements)