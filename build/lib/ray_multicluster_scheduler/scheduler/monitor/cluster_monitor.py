"""
Cluster monitor that integrates configuration management, metadata management, and health checking.
"""

import time
import os
from typing import Optional, Dict, Any, List
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager, ClusterConfig, ClusterHealth
from ray_multicluster_scheduler.common.model import ResourceSnapshot
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


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
        # Initialize cluster manager
        self.cluster_manager = ClusterManager()

        # Load cluster configurations from file if provided
        if config_file_path:
            self._load_cluster_configurations(config_file_path)
        else:
            # Try to load default configuration file
            default_config_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "clusters.yaml")
            if os.path.exists(default_config_path):
                logger.info(f"📁 加载默认集群配置文件: {default_config_path}")
                self._load_cluster_configurations(default_config_path)
            else:
                logger.info("📁 使用默认集群配置")
                # 添加默认的测试集群配置
                self._add_default_clusters()

        # Initialize health status for all clusters
        for name in self.cluster_manager.clusters:
            self.cluster_manager.health_status[name] = ClusterHealth()

        # 立即刷新集群状态，确保在初始化时就能获取到集群信息
        try:
            self.refresh_resource_snapshots(force=True)
            logger.info("✅ 集群状态初始化刷新完成")
        except Exception as e:
            logger.warning(f"❌ 集群状态初始化刷新失败: {e}")

        logger.info("✅ 集群监视器初始化成功")
        self.cluster_manager._log_cluster_configurations()

        # 标记是否已经刷新过集群状态
        self._clusters_refreshed = True

    def _add_default_clusters(self):
        """Add default cluster configurations if no custom configuration is provided."""
        logger.info("📁 使用默认集群配置")

        # Create default cluster configurations using ClusterMetadata
        from ray_multicluster_scheduler.common.model import ClusterMetadata

        default_clusters = [
            ClusterMetadata(
                name="centos",
                head_address="192.168.5.7:32546",
                dashboard="http://192.168.5.7:31591",
                prefer=False,
                weight=1.0,
                runtime_env={
                    "conda": "ts",
                    "env_vars": {
                        "home_dir": "/home/zorro"
                    }
                },
                tags=["linux", "x86_64"]
            ),
            ClusterMetadata(
                name="mac",
                head_address="192.168.5.2:32546",
                dashboard="http://192.168.5.2:8265",
                prefer=True,
                weight=1.2,
                runtime_env={
                    "conda": "k8s",
                    "env_vars": {
                        "home_dir": "/Users/zorro"
                    }
                },
                tags=["macos", "arm64"]
            )
        ]

        # Register default clusters with the cluster manager
        for cluster_meta in default_clusters:
            self.cluster_manager.clusters[cluster_meta.name] = ClusterConfig(
                name=cluster_meta.name,
                head_address=cluster_meta.head_address,
                dashboard=cluster_meta.dashboard,
                prefer=cluster_meta.prefer,
                weight=cluster_meta.weight,
                runtime_env=cluster_meta.runtime_env,
                tags=cluster_meta.tags
            )

    def _load_cluster_configurations(self, config_file_path: str):
        """Load cluster configurations from YAML file."""
        try:
            import yaml
            with open(config_file_path, 'r') as f:
                config_data = yaml.safe_load(f)

            # Update cluster configurations in the manager
            for cluster_config in config_data.get('clusters', []):
                # 直接使用runtime_env属性
                runtime_env = cluster_config.get('runtime_env', {})

                self.cluster_manager.clusters[cluster_config['name']] = ClusterConfig(
                    name=cluster_config['name'],
                    head_address=cluster_config['head_address'],
                    dashboard=cluster_config['dashboard'],
                    prefer=cluster_config.get('prefer', False),
                    weight=cluster_config.get('weight', 1.0),
                    runtime_env=runtime_env,
                    tags=cluster_config.get('tags', [])
                )

        except FileNotFoundError:
            logger.warning(f"Cluster config file not found: {config_file_path}, using default configuration")
        except Exception as e:
            logger.error(f"Failed to load cluster configurations: {e}")
            import traceback
            traceback.print_exc()

    def refresh_resource_snapshots(self, force: bool = False):
        """Refresh resource snapshots from health checker."""
        # 如果是强制刷新，或者还没有刷新过，则执行刷新
        if force or not self._clusters_refreshed:
            try:
                self.cluster_manager.refresh_all_clusters()
                self._clusters_refreshed = True
                logger.info("✅ 集群状态已刷新")
            except Exception as e:
                logger.error(f"❌ 刷新集群资源快照失败: {e}")
                import traceback
                traceback.print_exc()
                # 即使刷新失败，也标记为已刷新，避免重复尝试
                self._clusters_refreshed = True

    def get_all_cluster_info(self) -> Dict[str, Dict[str, Any]]:
        """Get combined metadata and resource snapshot for all clusters."""
        # 确保集群状态已刷新
        self.refresh_resource_snapshots()

        result = {}

        # 如果没有集群配置，尝试重新加载
        if not self.cluster_manager.clusters:
            logger.warning("没有找到集群配置，尝试重新加载默认配置")
            self._add_default_clusters()

        for name, config in self.cluster_manager.clusters.items():
            health = self.cluster_manager.health_status.get(name)

            # Create a ResourceSnapshot from health data
            # 即使集群不可用也要创建snapshot，但资源信息为空
            if health:
                snapshot = ResourceSnapshot(
                    cluster_name=name,
                    available_resources=health.resources.get("available", {}) if health.available else {},
                    total_resources=health.resources.get("total", {}) if health.available else {},
                    node_count=health.resources.get("node_count", 0) if health.available else 0,
                    timestamp=time.time()
                )
            else:
                snapshot = ResourceSnapshot(
                    cluster_name=name,
                    available_resources={},
                    total_resources={},
                    node_count=0,
                    timestamp=time.time()
                )

            result[name] = {
                'metadata': config,
                'snapshot': snapshot
            }

        return result

    def get_resource_snapshot(self, cluster_name: str) -> Optional[ResourceSnapshot]:
        """Get the latest resource snapshot for a cluster."""
        cluster_info = self.get_all_cluster_info()
        cluster_data = cluster_info.get(cluster_name)
        return cluster_data['snapshot'] if cluster_data else None

    def get_best_cluster(self, requirements: Optional[Dict] = None) -> str:
        """Get the best cluster based on current health and requirements."""
        # 确保集群状态已刷新
        self.refresh_resource_snapshots()
        return self.cluster_manager.get_best_cluster(requirements)

    def get_or_create_connection(self, cluster_name: str):
        """Get or create a connection to a cluster."""
        return self.cluster_manager.get_or_create_connection(cluster_name)

    def get_connected_clusters(self) -> List[str]:
        """Get list of currently connected clusters."""
        return self.cluster_manager.get_connected_clusters()

    def force_refresh_clusters(self):
        """Force refresh cluster states - for testing or special cases."""
        self._clusters_refreshed = False
        self.refresh_resource_snapshots()
