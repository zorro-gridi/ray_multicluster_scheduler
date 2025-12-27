"""
Detached actor for cluster resource monitoring that runs independently of user tasks.
This actor is meant to be started once and run as a daemon service for the entire system.
"""
import ray
import time
import logging
from typing import Dict, Any, Optional
from ray_multicluster_scheduler.common.model import ResourceSnapshot
from ray_multicluster_scheduler.scheduler.monitor.resource_statistics import get_cluster_level_stats
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


@ray.remote(name="cluster_resource_monitor", lifetime="detached", max_concurrency=10)
class ClusterResourceMonitor:
    """
    A detached actor that continuously monitors cluster resources across all connected clusters.
    This actor runs independently of user tasks and provides a centralized resource snapshot service.

    Features:
    - Automatic background refresh every 20 seconds
    - Immediate snapshot retrieval (non-blocking)
    - Thread-safe snapshot updates
    """

    def __init__(self, cluster_configs: Dict[str, Any]):
        """
        Initialize the cluster resource monitor with cluster configurations.

        Args:
            cluster_configs: Dictionary of cluster configurations
        """
        self.cluster_configs = cluster_configs
        self.snapshots: Dict[str, ResourceSnapshot] = {}
        self.last_update_time = time.time()
        self.running = True
        self._initial_collection_done = True  # 不再自动收集，标记为完成
        logger.info("ClusterResourceMonitor initialized as global snapshot storage")
        logger.info(f"Configured for {len(cluster_configs)} clusters: {list(cluster_configs.keys())}")
        logger.info("⚠️  注意：ClusterResourceMonitor 不再后台自动刷新，需要外部定期调用 update_snapshot()")

    def update_snapshot(self, cluster_name: str, snapshot: ResourceSnapshot):
        """
        更新单个集群的快照（由外部调用）

        Args:
            cluster_name: 集群名称
            snapshot: 资源快照对象
        """
        self.snapshots[cluster_name] = snapshot
        self.last_update_time = time.time()
        logger.debug(f"更新快照: {cluster_name} - CPU={snapshot.cluster_cpu_usage_percent:.1f}%, "
                   f"MEM={snapshot.cluster_mem_usage_percent:.1f}%")

    def update_snapshots(self, snapshots: Dict[str, ResourceSnapshot]):
        """
        批量更新多个集群的快照

        Args:
            snapshots: 集群名称到快照的映射
        """
        self.snapshots.update(snapshots)
        self.last_update_time = time.time()
        logger.info(f"批量更新快照: 更新了 {len(snapshots)} 个集群")

    def get_latest_snapshots(self) -> Dict[str, ResourceSnapshot]:
        """
        Get the latest collected resource snapshots.

        Returns:
            Dictionary mapping cluster names to their latest resource snapshots
        """
        logger.debug(f"📚 下游读取全局快照: 当前有 {len(self.snapshots)} 个集群快照")
        return self.snapshots

    def get_snapshot(self, cluster_name: str) -> Optional[ResourceSnapshot]:
        """
        Get the latest resource snapshot for a specific cluster.

        Args:
            cluster_name: Name of the cluster

        Returns:
            ResourceSnapshot for the cluster or None if not found
        """
        return self.snapshots.get(cluster_name)

    def update_cluster_configs(self, cluster_configs: Dict[str, Any]):
        """
        Update the cluster configurations used for monitoring.

        Args:
            cluster_configs: New cluster configurations
        """
        self.cluster_configs = cluster_configs
        logger.info("Cluster configurations updated for monitoring")

    def get_status(self) -> Dict[str, Any]:
        """
        获取监控状态信息（轻量级，快速返回）

        Returns:
            包含状态信息的字典，包括：
            - last_update_time: 最后更新时间戳
            - snapshot_count: 快照数量
            - clusters: 集群列表
            - running: 是否运行中
            - is_active: 是否活跃（60秒内有更新）
            - inactive_seconds: 距离上次更新的秒数
        """
        current_time = time.time()
        inactive_seconds = current_time - self.last_update_time
        is_active = inactive_seconds <= 60  # 60秒内有更新视为活跃

        return {
            "last_update_time": self.last_update_time,
            "snapshot_count": len(self.snapshots),
            "clusters": list(self.snapshots.keys()),
            "running": self.running,
            "is_active": is_active,
            "inactive_seconds": round(inactive_seconds, 1)
        }

    def ping(self) -> str:
        """
        Health check method to verify the monitor is running.

        Returns:
            A simple response string
        """
        return f"ClusterResourceMonitor alive. Last update: {self.last_update_time}. Snapshots: {len(self.snapshots)} clusters"