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

logger = get_logger(__name__)


class ClusterMonitor:
    """Monitors cluster health and manages cluster registry."""

    def __init__(self, config_file_path: Optional[str] = None, detached_monitor=None):
        """
        Initialize the cluster monitor.

        Args:
            config_file_path (str, optional): Path to the cluster configuration YAML file.
                If not provided, the system will attempt to locate the configuration file
                in common locations or fall back to default configuration.
            detached_monitor: Reference to the global ClusterResourceMonitor detached actor
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

        # Store reference to detached monitor
        self.detached_monitor = detached_monitor

        self.cluster_metadata =  self.config_manager.get_cluster_configs()
        # Initialize cluster manager
        from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager
        self.cluster_manager = ClusterManager(self.cluster_metadata)

        # Initialize client pool
        from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
        self.client_pool = RayClientPool(self.config_manager)

        # Initialize health status for all clusters
        for name in self.cluster_manager.clusters:
            self.cluster_manager.health_status[name] = ClusterHealth()

        logger.info("✅ 集群监视器初始化成功")

        # 初始化全局资源快照存储
        self._resource_snapshots: Dict[str, ResourceSnapshot] = {}

        # 注意：不在这里创建初始快照，因为 detached_monitor 还未创建
        # 初始快照将在 detached_monitor 创建后通过 initialize_snapshots_from_monitor() 调用


    def get_all_cluster_info(self) -> Dict[str, Dict[str, Any]]:
        """Get combined metadata and resource snapshot for all clusters."""
        result = {}

        # 如果没有集群配置，尝试重新加载
        if not self.cluster_manager.clusters:
            logger.warning("没有找到集群配置，尝试重新加载默认配置")
            self.config_manager.get_cluster_configs()

        for name, config in self.cluster_manager.clusters.items():
            # 直接从全局快照状态管理器读取ResourceSnapshot
            snapshot = self._resource_snapshots.get(name)

            # 如果没有全局快照，则创建一个默认快照
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
        # Directly return from the global snapshots storage
        return self._resource_snapshots.get(cluster_name)

    def update_resource_snapshot(self, cluster_name: str, snapshot: ResourceSnapshot):
        """Update the resource snapshot for a cluster."""
        # Update the global snapshots storage
        self._resource_snapshots[cluster_name] = snapshot

        # Also update the cluster manager's health status with the new snapshot
        health = self.cluster_manager.health_status.get(cluster_name)
        if health:
            # Update health resources based on the new snapshot
            cpu_total = snapshot.cluster_cpu_total_cores
            cpu_used = snapshot.cluster_cpu_used_cores
            cpu_free = cpu_total - cpu_used
            mem_total = snapshot.cluster_mem_total_mb
            mem_used = snapshot.cluster_mem_used_mb
            mem_free = mem_total - mem_used
            cpu_utilization = snapshot.cluster_cpu_usage_percent / 100.0 if cpu_total > 0 else 0
            mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if mem_total > 0 else 0

            config = self.config_manager.get_cluster_config(cluster_name)
            if config:
                if cpu_free <= 0:
                    score = -1
                else:
                    # 基础评分 = 可用CPU * 集群权重
                    base_score = cpu_free * config.weight

                    # 内存资源加成 = 可用内存(GB) * 0.1 * 集群权重
                    memory_available_gb = mem_free / 1024.0  # Convert MB to GB
                    memory_bonus = memory_available_gb * 0.1 * config.weight

                    # GPU 资源加成（由于ResourceSnapshot不提供GPU信息，暂时设为0）
                    gpu_bonus = 0 * 5  # GPU资源更宝贵

                    # 偏好集群加成
                    preference_bonus = 1.2 if config.prefer else 1.0

                    # 负载均衡因子：资源利用率越低得分越高
                    load_balance_factor = 1.0 - cpu_utilization  # 负载越低因子越高

                    # 最终评分
                    score = (base_score + memory_bonus + gpu_bonus) * preference_bonus * load_balance_factor
            else:
                score = 0.0  # Default score if config is not found

            # Update health resources
            health.resources = {
                "available": {
                    "CPU": cpu_free,
                    "memory": mem_free * 1024 * 1024  # Convert MB to bytes to match old format
                },
                "total": {
                    "CPU": cpu_total,
                    "memory": mem_total * 1024 * 1024  # Convert MB to bytes to match old format
                },
                "cpu_free": cpu_free,
                "cpu_total": cpu_total,
                "cpu_utilization": cpu_utilization,
                "mem_utilization": mem_utilization,
                "node_count": snapshot.node_count
            }
            health.score = score  # Update the score
            health.available = True  # Mark as available if we have a snapshot
            health.last_checked = time.time()
        else:
            # Create new health status if it doesn't exist
            health = ClusterHealth()
            self.cluster_manager.health_status[cluster_name] = health

    def _initialize_cluster_snapshots(self):
        """Initialize cluster snapshots by checking which clusters don't have snapshots yet and using HealthChecker to create them.

        This method ensures that all clusters have initial resource snapshots when the cluster monitor starts up.
        It only creates snapshots for clusters that don't already have them, using the HealthChecker to perform
        the actual health checks and resource data collection.
        """
        logger.info("Initializing cluster snapshots for startup")

        # Get cluster metadata from the cluster manager, only for clusters that don't have snapshots yet
        from ray_multicluster_scheduler.common.model import ClusterMetadata
        cluster_metadata_list = []
        for name, config in self.cluster_manager.clusters.items():
            # Check if a snapshot already exists for this cluster
            if name not in self._resource_snapshots:
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
            else:
                logger.debug(f"Snapshot already exists for cluster {name}, skipping initial snapshot creation")

        # Create HealthChecker and update snapshots only for clusters without snapshots
        if cluster_metadata_list:  # Only create HealthChecker if there are clusters without snapshots
            # Use the cluster monitor's client pool for health checking
            from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
            health_checker = HealthChecker(
                cluster_metadata_list,
                self.client_pool,
                detached_monitor=self.detached_monitor
            )
            health_checker._log_cluster_status()
            snapshots = health_checker.check_health()

            # Update cluster monitor with new snapshots
            for cluster_name, snapshot in snapshots.items():
                self.update_resource_snapshot(cluster_name, snapshot)

            logger.info(f"Initialized snapshots for {len(snapshots)} clusters")
        else:
            logger.info("All clusters already have snapshots, no initialization needed")

    def initialize_snapshots_from_monitor(self):
        """
        初始化快照：从 ClusterResourceMonitor 读取快照

        注意：
        - 不再前台收集快照，只从全局存储读取
        - 如果 ClusterResourceMonitor 为空，等待 BackgroundHealthChecker 后台推送
        - BackgroundHealthChecker 启动时会立即执行首次收集
        """
        if self.detached_monitor:
            logger.info("尝试从 ClusterResourceMonitor 读取初始快照...")
            try:
                import ray

                # 直接读取，不等待
                global_snapshots = ray.get(self.detached_monitor.get_latest_snapshots.remote())

                if global_snapshots:
                    logger.info(f"✅ 从 ClusterResourceMonitor 获取到 {len(global_snapshots)} 个集群快照")
                    # 更新到本地存储
                    for cluster_name, snapshot in global_snapshots.items():
                        self.update_resource_snapshot(cluster_name, snapshot)
                    return
                else:
                    logger.info("ℹ️  ClusterResourceMonitor 还没有快照数据")
                    logger.info("ℹ️  BackgroundHealthChecker 将在后台自动收集并推送快照")
                    logger.info("ℹ️  首次收集已在后台启动，请稍候...")
            except Exception as e:
                logger.warning(f"从 ClusterResourceMonitor 读取失败: {e}")
                logger.info("ℹ️  BackgroundHealthChecker 将在后台自动收集并推送快照")
                import traceback
                logger.warning(traceback.format_exc())
        else:
            logger.warning("⚠️  detached_monitor 不可用")
            logger.warning("⚠️  无法获取全局快照，请检查 ClusterResourceMonitor 是否启动")

    def refresh_cluster_health(self):
        """
        刷新集群健康状态：从 ClusterResourceMonitor 读取最新快照

        注意：不再前台收集快照，只从全局存储读取
        快照由 BackgroundHealthChecker 后台定期更新
        """
        logger.info("刷新集群健康状态：从 ClusterResourceMonitor 读取最新快照")

        # 尝试从 detached_monitor 读取最新快照
        if self.detached_monitor:
            try:
                import ray
                global_snapshots = ray.get(self.detached_monitor.get_latest_snapshots.remote())

                if global_snapshots:
                    logger.info(f"✅ 从 ClusterResourceMonitor 获取到 {len(global_snapshots)} 个集群快照")
                    # 更新到本地存储
                    for cluster_name, snapshot in global_snapshots.items():
                        if cluster_name in self.cluster_manager.clusters:
                            self.update_resource_snapshot(cluster_name, snapshot)

                            # 更新集群健康状态
                            from ray_multicluster_scheduler.scheduler.health.health_status import HealthStatus
                            health = HealthStatus(
                                is_healthy=True,
                                score=self._calculate_cluster_score(snapshot),
                                last_check_time=snapshot.timestamp,
                                error_message=None
                            )
                            self.cluster_manager.health_status[cluster_name] = health

                    logger.info("✅ 集群健康状态刷新完成（使用全局快照）")
                    return
                else:
                    logger.warning("⚠️  ClusterResourceMonitor 返回空快照，跳过刷新")
            except Exception as e:
                logger.warning(f"⚠️  从 ClusterResourceMonitor 读取失败: {e}，跳过刷新")
                import traceback
                logger.warning(traceback.format_exc())
        else:
            logger.warning("⚠️  detached_monitor 不可用，无法刷新集群健康状态")

    def _calculate_cluster_score(self, snapshot: 'ResourceSnapshot') -> float:
        """
        计算集群评分

        Args:
            snapshot: 资源快照

        Returns:
            集群评分 (0-100)
        """
        # 简单的评分算法：基于 CPU 和内存空闲率
        cpu_free_percent = 100 - snapshot.cluster_cpu_usage_percent
        mem_free_percent = 100 - snapshot.cluster_mem_usage_percent

        # 加权平均：CPU 60%，内存 40%
        score = cpu_free_percent * 0.6 + mem_free_percent * 0.4
        return max(0.0, min(100.0, score))

    def get_best_cluster(self, requirements: Optional[Dict] = None) -> str:
        """Get the best cluster based on current health and requirements."""
        # 确保集群状态已刷新
        self.refresh_cluster_health()
        return self.cluster_manager.get_best_cluster(requirements)