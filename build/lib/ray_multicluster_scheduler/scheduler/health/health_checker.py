"""
Health checker for Ray clusters.
"""

import time
import ray
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from ray_multicluster_scheduler.common.model import ClusterMetadata, ResourceSnapshot, ClusterHealth
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import ClusterConnectionError
# Delayed import to avoid circular imports
get_cluster_resource_snapshot = None
get_cluster_health_status = None


def _import_prometheus_client():
    """Import prometheus client functions to avoid circular imports."""
    global get_cluster_resource_snapshot, get_cluster_health_status
    if get_cluster_resource_snapshot is None or get_cluster_health_status is None:
        from ray_multicluster_scheduler.scheduler.monitor.prometheus_client import get_cluster_resource_snapshot as _get_cluster_resource_snapshot, get_cluster_health_status as _get_cluster_health_status
        get_cluster_resource_snapshot = _get_cluster_resource_snapshot
        get_cluster_health_status = _get_cluster_health_status
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager


logger = get_logger(__name__)


# ClusterHealth class is now imported from common.model, so we don't need to define it here

class HealthChecker:
    """Checks the health and resource status of Ray clusters."""

    def __init__(self, cluster_metadata: List[ClusterMetadata], client_pool: RayClientPool):
        self.cluster_metadata = cluster_metadata
        self.client_pool = client_pool
        self.health_status = {}  # Add health status dictionary

        # Add clusters to the client pool
        for cluster in self.cluster_metadata:
            try:
                self.client_pool.add_cluster(cluster)
                logger.info(f"Added cluster {cluster.name} to health checker")
            except Exception as e:
                logger.error(f"Failed to add cluster {cluster.name} to health checker: {e}")

    def _calculate_score(self, cluster, resource_snapshot):
        """Calculate score for a cluster based on its resources and configuration."""
        # Extract resource information from the ResourceSnapshot
        cpu_total = resource_snapshot.cluster_cpu_total_cores
        cpu_used = resource_snapshot.cluster_cpu_used_cores
        cpu_free = cpu_total - cpu_used
        cpu_utilization = resource_snapshot.cluster_cpu_usage_percent / 100.0 if cpu_total > 0 else 0

        # 内存信息
        mem_total = resource_snapshot.cluster_mem_total_mb
        mem_used = resource_snapshot.cluster_mem_used_mb
        mem_free = mem_total - mem_used
        mem_utilization = resource_snapshot.cluster_mem_usage_percent / 100.0 if mem_total > 0 else 0

        # 获取集群配置信息
        # 遍历cluster_metadata列表找到对应集群的配置信息
        cluster_config = None
        for config in self.cluster_metadata:
            if config.name == cluster.name:
                cluster_config = config

        # 如果没有找到对应的配置，使用默认值
        weight = cluster_config.weight if cluster_config else 1.0
        prefer = cluster_config.prefer if cluster_config else False

        gpu_free = 0  # ResourceSnapshot不包含GPU信息
        gpu_total = 0  # ResourceSnapshot不包含GPU信息

        if cpu_free <= 0:
            score = -1
        else:
            # 基础评分 = 可用CPU * 集群权重
            base_score = cpu_free * weight

            # 内存资源加成 = 可用内存(GiB) * 0.1 * 集群权重
            memory_available_gib = mem_free / 1024.0  # Convert MB to GiB
            memory_bonus = memory_available_gib * 0.1 * weight

            # GPU 资源加成（由于ResourceSnapshot不提供GPU信息，暂时设为0）
            gpu_bonus = gpu_free * 5  # GPU资源更宝贵

            # 偏好集群加成
            preference_bonus = 1.2 if prefer else 1.0

            # 负载均衡因子：资源利用率越低得分越高
            load_balance_factor = 1.0 - cpu_utilization  # 负载越低因子越高

            # 最终评分
            score = (base_score + memory_bonus + gpu_bonus) * preference_bonus * load_balance_factor

        # 记录详细的资源信息
        logger.info(f"集群 [{cluster.name}] 资源状态: "
                   f"CPU={cpu_free:.2f}/{cpu_total:.2f} ({cpu_utilization:.2%} 已使用), "
                   f"内存={mem_free/1024.0:.2f}/{mem_total/1024.0:.2f}GiB ({mem_utilization:.2%} 已使用), "
                   f"GPU={gpu_free}/{gpu_total}, "
                   f"节点数={resource_snapshot.node_count}, "
                   f"评分={score:.1f}")

        return score


    def check_health(self) -> Dict[str, ResourceSnapshot]:
        """Check the health of all clusters and return resource snapshots with scores."""
        snapshots = {}

        for cluster in self.cluster_metadata:
            try:
                logger.info(f"Starting health check for cluster {cluster.name}")

                # Import prometheus client functions to avoid circular imports
                _import_prometheus_client()

                # Check cluster health status using Prometheus
                cluster_available = get_cluster_health_status(cluster.name)
                if not cluster_available:
                    logger.warning(f"Cluster {cluster.name} is not available according to Prometheus")
                    continue

                # Get resource snapshot from Prometheus
                snapshot = get_cluster_resource_snapshot(cluster.name)

                # Calculate score for the cluster
                score = self._calculate_score(cluster, snapshot)

                logger.info(f"Created snapshot for {cluster.name} with stats: CPU usage={snapshot.cluster_cpu_usage_percent}%, Mem usage={snapshot.cluster_mem_usage_percent}%, Score={score:.1f}")

            except Exception as e:
                logger.warning(f"Failed to get detailed stats for {cluster.name}, using defaults: {e}")
                import traceback
                traceback.print_exc()
                # Fallback to default values if detailed stats fail
                snapshot = ResourceSnapshot(
                    cluster_name=cluster.name,
                    cluster_cpu_usage_percent=0.0,
                    cluster_mem_usage_percent=0.0,
                    cluster_cpu_used_cores=0.0,
                    cluster_cpu_total_cores=0.0,
                    cluster_mem_used_mb=0.0,
                    cluster_mem_total_mb=0.0,
                    node_count=0,  # 使用0作为默认节点数，因为不再使用ray.nodes()
                    timestamp=time.time(),
                    node_stats=None
                )

                logger.debug(f"Health check for {cluster.name}: {snapshot}")
            except Exception as e:
                logger.error(f"Health check failed for cluster {cluster.name}: {e}")
                import traceback
                traceback.print_exc()
                # Mark the client as disconnected but keep the entry
                if self.client_pool:
                    self.client_pool.mark_cluster_disconnected(cluster.name)

            # NOTE: 用户更新，不要修改
            snapshots[cluster.name] = snapshot
        return snapshots

    def update_cluster_manager_health(self, cluster_manager: ClusterManager):
        """Update cluster manager's health status with current health check results."""
        # This method is maintained for compatibility but the simplified approach
        # relies on direct snapshot fetching by PolicyEngine
        logger.info("Health checker compatibility method called - health updates now handled directly by PolicyEngine")
        return {}  # Return empty dict as health updates are now handled by PolicyEngine directly

    def reconnect_cluster(self, cluster_name: str):
        """Attempt to reconnect to a cluster."""
        for cluster in self.cluster_metadata:
            if cluster.name == cluster_name:
                try:
                    # Use the client pool to reconnect
                    success = self.client_pool.establish_ray_connection(cluster.name)
                    if not success:
                        raise Exception(f"Failed to establish connection to cluster {cluster.name}")

                    logger.info(f"Reconnected to cluster {cluster.name}")
                except Exception as e:
                    logger.error(f"Failed to reconnect to cluster {cluster.name}: {e}")
                    raise ClusterConnectionError(f"Could not reconnect to cluster {cluster.name}: {e}")

    def _log_cluster_status(self):
        """Log cluster configurations for debugging."""
        logger.info("📋 集群信息和资源使用情况:")
        logger.info("=" * 50)

        for cluster_metadata in self.cluster_metadata:
            name = cluster_metadata.name
            runtime_env = cluster_metadata.runtime_env
            head_address = cluster_metadata.head_address
            prefer = cluster_metadata.prefer
            weight = cluster_metadata.weight
            tags = cluster_metadata.tags

            # 为兼容性创建一个临时的ClusterHealth实例
            health = ClusterHealth()
            status = "🟢 健康" if health.available else "🔴 不健康"
            score = f"{health.score:.1f}" if health.score >= 0 else "N/A"

            logger.info(f"集群 [{name}]: {status}")
            logger.info(f"  地址: {head_address}")
            logger.info(f"  首选项: {'是' if prefer else '否'}")
            logger.info(f"  权重: {weight}")

            # 从runtime_env中提取home_dir信息
            home_dir = "未设置"
            if runtime_env:
                env_vars = runtime_env.get('env_vars', {})
                home_dir = env_vars.get('home_dir', '未设置')
            logger.info(f"  Home目录: {home_dir}")

            # logger.info(f"  评分: {score}")
            logger.info(f"  标签: {', '.join(tags)}")

            if health.resources:
                cpu_free = health.resources.get('cpu_free', 0)
                cpu_total = health.resources.get('cpu_total', 0)
                gpu_free = health.resources.get('gpu_free', 0)
                gpu_total = health.resources.get('gpu_total', 0)
                node_count = health.resources.get('node_count', 0)
                cpu_utilization = health.resources.get('cpu_utilization', 0)
                mem_utilization = health.resources.get('mem_utilization', 0)
                # Calculate memory usage from available and total values
                total_memory_bytes = health.resources.get('total', {}).get('memory', 0)
                available_memory_bytes = health.resources.get('available', {}).get('memory', 0)
                total_memory_mb = total_memory_bytes / (1024*1024) if total_memory_bytes > 0 else 0
                used_memory_mb = total_memory_mb - (available_memory_bytes / (1024*1024)) if total_memory_bytes > 0 else 0

                logger.info(f"  CPU: {cpu_free:.2f}/{cpu_total:.2f} ({cpu_utilization:.2%} 已使用)")
                # 转换为GiB显示
                used_memory_gib = used_memory_mb / 1024.0
                total_memory_gib = total_memory_mb / 1024.0
                logger.info(f"  内存: {used_memory_gib:.2f}/{total_memory_gib:.2f}GiB ({mem_utilization:.2%} 已使用)")
                logger.info(f"  GPU: {gpu_free}/{gpu_total}")
                logger.info(f"  节点数: {node_count}")

            logger.info("-" * 30)


    def _check_cluster_health(self, cluster, resource_snapshot: Optional[ResourceSnapshot] = None) -> ClusterHealth:
        """检查单个集群健康状态"""
        health = ClusterHealth()

        # 如果提供了ResourceSnapshot，则使用它
        if resource_snapshot:
            # 使用传入的ResourceSnapshot数据
            cpu_total = resource_snapshot.cluster_cpu_total_cores
            cpu_used = resource_snapshot.cluster_cpu_used_cores
            cpu_free = cpu_total - cpu_used
            cpu_utilization = resource_snapshot.cluster_cpu_usage_percent / 100.0 if cpu_total > 0 else 0

            # 内存信息
            mem_total = resource_snapshot.cluster_mem_total_mb
            mem_used = resource_snapshot.cluster_mem_used_mb
            mem_free = mem_total - mem_used
            mem_utilization = resource_snapshot.cluster_mem_usage_percent / 100.0 if mem_total > 0 else 0

            node_count = resource_snapshot.node_count
        else:
            # 如果没有ResourceSnapshot，使用默认值
            node_count = 0
            # 设置默认值
            cpu_total = 0
            cpu_used = 0
            cpu_free = 0
            cpu_utilization = 0
            mem_total = 0
            mem_used = 0
            mem_free = 0
            mem_utilization = 0

        gpu_free = 0  # ResourceSnapshot不包含GPU信息
        gpu_total = 0  # ResourceSnapshot不包含GPU信息

        # 获取集群配置信息
        # 遍历cluster_metadata列表找到对应集群的配置信息
        cluster_config = None
        for config in self.cluster_metadata:
            if config.name == cluster.name:
                cluster_config = config

        # 如果没有找到对应的配置，使用默认值
        weight = cluster_config.weight if cluster_config else 1.0
        prefer = cluster_config.prefer if cluster_config else False

        if cpu_free <= 0:
            score = -1
        else:
            # 基础评分 = 可用CPU * 集群权重
            base_score = cpu_free * weight

            # 内存资源加成 = 可用内存(GiB) * 0.1 * 集群权重
            memory_available_gib = mem_free / 1024.0  # Convert MB to GiB
            memory_bonus = memory_available_gib * 0.1 * weight

            # GPU 资源加成（由于ResourceSnapshot不提供GPU信息，暂时设为0）
            gpu_bonus = gpu_free * 5  # GPU资源更宝贵

            # 偏好集群加成
            preference_bonus = 1.2 if prefer else 1.0

            # 负载均衡因子：资源利用率越低得分越高
            load_balance_factor = 1.0 - cpu_utilization  # 负载越低因子越高

            # 最终评分
            score = (base_score + memory_bonus + gpu_bonus) * preference_bonus * load_balance_factor

        # 收集资源详情 - 使用ResourceSnapshot数据
        resources = {
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
            "gpu_free": gpu_free,
            "gpu_total": gpu_total,
            "cpu_utilization": cpu_utilization,
            "mem_utilization": mem_utilization,
            "node_count": node_count
        }

        health.update(score, resources, True)

        # 记录详细的资源信息
        logger.info(f"集群 [{cluster.name}] 资源状态: "
                    f"CPU={cpu_free:.2f}/{cpu_total:.2f} ({cpu_utilization:.2%} 已使用), "
                    f"内存={mem_free/1024.0:.2f}/{mem_total/1024.0:.2f}GiB ({mem_utilization:.2%} 已使用), "
                    f"GPU={gpu_free}/{gpu_total}, "
                    f"节点数={node_count}, "
                    f"评分={score:.1f}")


        return health