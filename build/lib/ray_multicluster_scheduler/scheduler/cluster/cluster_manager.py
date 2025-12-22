"""
Cluster manager that handles cluster configurations and connections.
"""

import ray
import time
import logging
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ClusterConfig:
    """Configuration for a Ray cluster."""
    name: str
    head_address: str
    dashboard: str
    prefer: bool = False
    weight: float = 1.0
    runtime_env: Optional[Dict[str, Any]] = None  # 新增：运行时环境配置，包含conda和home_dir等信息
    tags: List[str] = field(default_factory=list)


@dataclass
class ClusterHealth:
    """Health status information for a cluster."""
    score: float = 0.0
    resources: Dict[str, Any] = field(default_factory=dict)
    available: bool = True
    last_checked: datetime = field(default_factory=datetime.now)
    error_message: str = ""

    def update(self, score: float, resources: Dict[str, Any], available: bool, error_message: str = ""):
        """Update cluster health status."""
        self.score = score
        self.resources = resources
        self.available = available
        self.last_checked = datetime.now()
        self.error_message = error_message


class ClusterManager:
    """Manages Ray cluster configurations and connections."""

    def __init__(self):
        """Initialize the cluster manager."""
        self.clusters: Dict[str, ClusterConfig] = {}
        self.health_status: Dict[str, ClusterHealth] = {}
        self.active_connections: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self.metrics = {
            "total_checks": 0,
            "successful_checks": 0,
            "failed_checks": 0,
            "last_refresh": None
        }

    def add_cluster(self, config: ClusterConfig):
        """Add a cluster configuration."""
        self.clusters[config.name] = config
        self.health_status[config.name] = ClusterHealth()
        logger.info(f"Added cluster configuration: {config.name}")

    def remove_cluster(self, name: str):
        """Remove a cluster configuration."""
        if name in self.clusters:
            del self.clusters[name]
            del self.health_status[name]
            logger.info(f"Removed cluster configuration: {name}")

    def get_cluster_config(self, name: str) -> Optional[ClusterConfig]:
        """Get cluster configuration by name."""
        return self.clusters.get(name)

    def get_all_cluster_configs(self) -> Dict[str, ClusterConfig]:
        """Get all cluster configurations."""
        return self.clusters.copy()

    def get_cluster_home_dir(self, cluster_name: str) -> Optional[str]:
        """Get the home directory for a specific cluster."""
        config = self.get_cluster_config(cluster_name)
        if config and config.runtime_env:
            env_vars = config.runtime_env.get('env_vars', {})
            return env_vars.get('home_dir')
        return None

    def _log_cluster_configurations(self):
        """Log cluster configurations for debugging."""
        logger.info("📋 集群信息和资源使用情况:")
        logger.info("=" * 50)

        if not self.clusters:
            logger.info("🚫 当前没有配置集群")
            return

        for name, config in self.clusters.items():
            health = self.health_status.get(name, ClusterHealth())
            status = "🟢 健康" if health.available else "🔴 不健康"
            score = f"{health.score:.1f}" if health.score >= 0 else "N/A"

            logger.info(f"集群 [{name}]: {status}")
            logger.info(f"  地址: {config.head_address}")
            logger.info(f"  首选项: {'是' if config.prefer else '否'}")
            logger.info(f"  权重: {config.weight}")

            # 从runtime_env中提取home_dir信息
            home_dir = "未设置"
            if config.runtime_env:
                env_vars = config.runtime_env.get('env_vars', {})
                home_dir = env_vars.get('home_dir', '未设置')
            logger.info(f"  Home目录: {home_dir}")

            logger.info(f"  评分: {score}")
            logger.info(f"  标签: {', '.join(config.tags)}")

            if health.resources:
                cpu_free = health.resources.get('cpu_free', 0)
                cpu_total = health.resources.get('cpu_total', 0)
                gpu_free = health.resources.get('gpu_free', 0)
                gpu_total = health.resources.get('gpu_total', 0)
                node_count = health.resources.get('node_count', 0)

                logger.info(f"  CPU: {cpu_free}/{cpu_total}")
                logger.info(f"  GPU: {gpu_free}/{gpu_total}")
                logger.info(f"  节点数: {node_count}")

            logger.info("-" * 30)

    def refresh_all_clusters(self):
        """Refresh health status for all clusters."""
        with self._lock:
            logger.info("开始刷新集群状态...")

            for name, config in self.clusters.items():
                try:
                    health = self._check_cluster_health(config)
                    self.health_status[name] = health

                    if health.available:
                        self.metrics["successful_checks"] += 1
                    else:
                        self.metrics["failed_checks"] += 1

                except Exception as e:
                    logger.error(f"检查集群 {name} 健康状态时出错: {e}")
                    import traceback
                    traceback.print_exc()
                    self.metrics["failed_checks"] += 1
                    # Update health status with error
                    error_health = ClusterHealth()
                    error_health.update(-1, {}, False, str(e))
                    self.health_status[name] = error_health

            self.metrics["total_checks"] += 1
            self.metrics["last_refresh"] = datetime.now()
            logger.info(f"集群状态刷新完成，总计检查: {self.metrics['total_checks']}")

    def _check_cluster_health(self, config: ClusterConfig) -> ClusterHealth:
        """检查单个集群健康状态"""
        health = ClusterHealth()
        ray_address = f"ray://{config.head_address}"

        try:
            # 尝试连接到集群，添加超时机制
            import socket
            host, port = config.head_address.split(':')
            port = int(port)

            # 先检查端口是否可达
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 5秒超时
            result = sock.connect_ex((host, port))
            sock.close()

            if result != 0:
                health.update(-1, {}, False, f"无法连接到集群地址 {config.head_address}")
                return health

            # 尝试连接到集群
            ray.init(
                address=ray_address,
                ignore_reinit_error=True,
                logging_level=logging.WARNING,
                _system_config={"num_heartbeats_timeout": 10}
            )

            # 等待连接稳定
            time.sleep(0.5)

            if not ray.is_initialized():
                health.update(-1, {}, False, "Ray未正确初始化")
                return health

            # 获取资源信息
            avail_resources = ray.available_resources()
            total_resources = ray.cluster_resources()

            # 处理MAC集群的特殊CPU资源
            # 对于MAC集群，我们需要同时考虑CPU和MacCPU资源
            cpu_free, cpu_total = self._calculate_cpu_resources(avail_resources, total_resources, config)

            gpu_free = avail_resources.get("GPU", 0)
            gpu_total = total_resources.get("GPU", 0)

            if cpu_free <= 0:
                score = -1
            else:
                # 基础评分 = 可用CPU * 集群权重
                base_score = cpu_free * config.weight
                # GPU 资源加成
                gpu_bonus = gpu_free * 5  # GPU资源更宝贵

                # 偏好集群加成
                preference_bonus = 1.2 if config.prefer else 1.0

                # 负载均衡因子：资源利用率越低得分越高
                cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
                load_balance_factor = 1.0 - cpu_utilization  # 负载越低因子越高

                # 最终评分
                score = (base_score + gpu_bonus) * preference_bonus * load_balance_factor

            # 收集资源详情
            node_count = len(ray.nodes())
            resources = {
                "available": avail_resources,
                "total": total_resources,
                "cpu_free": cpu_free,
                "cpu_total": cpu_total,
                "gpu_free": gpu_free,
                "gpu_total": gpu_total,
                "cpu_utilization": cpu_utilization,
                "node_count": node_count
            }

            health.update(score, resources, True)

            # 记录详细的资源信息
            logger.info(f"集群 [{config.name}] 资源状态: "
                       f"CPU={cpu_free}/{cpu_total} ({cpu_utilization:.1%} 已使用), "
                       f"GPU={gpu_free}/{gpu_total}, "
                       f"节点数={node_count}, "
                       f"评分={score:.1f}")

            # 断开连接以避免占用资源
            ray.shutdown()

        except Exception as e:
            logger.warning(f"集群 [{config.name}] 连接失败: {e}")
            import traceback
            traceback.print_exc()
            health.update(-1, {}, False, str(e))

        return health

    def _calculate_cpu_resources(self, avail_resources: Dict[str, Any],
                                total_resources: Dict[str, Any],
                                config: ClusterConfig) -> tuple:
        """
        计算CPU资源，特别处理MAC集群的特殊资源类型

        Args:
            avail_resources: 可用资源字典
            total_resources: 总资源字典
            config: 集群配置

        Returns:
            tuple: (cpu_free, cpu_total)
        """
        # 默认使用标准CPU资源
        cpu_free = avail_resources.get("CPU", 0)
        cpu_total = total_resources.get("CPU", 0)

        # 对于MAC集群，检查是否有MacCPU资源
        if "mac" in config.name.lower() or any("mac" in tag.lower() for tag in config.tags):
            mac_cpu_free = avail_resources.get("MacCPU", 0)
            mac_cpu_total = total_resources.get("MacCPU", 0)

            # 如果MacCPU资源更大，则使用MacCPU资源
            if mac_cpu_total > cpu_total:
                cpu_free = mac_cpu_free
                cpu_total = mac_cpu_total
                logger.debug(f"使用MAC特殊CPU资源: 可用={cpu_free}, 总计={cpu_total}")

        return cpu_free, cpu_total

    def select_best_cluster(self, required_resources: Dict[str, float] = None) -> Optional[str]:
        """Select the best cluster based on health scores and resource requirements."""
        if required_resources is None:
            required_resources = {}

        # Filter healthy clusters
        healthy_clusters = {
            name: self.health_status[name]
            for name in self.clusters
            if self.health_status[name].available
        }

        # Filter clusters with sufficient resources
        sufficient_clusters = {}
        for name, health in healthy_clusters.items():
            resources = health.resources
            available_cpu = resources.get("cpu_free", 0)
            available_gpu = resources.get("gpu_free", 0)
            required_cpu = required_resources.get("CPU", 0)
            required_gpu = required_resources.get("GPU", 0)

            if available_cpu >= required_cpu and available_gpu >= required_gpu:
                sufficient_clusters[name] = health

        # Check if we have any sufficient clusters
        if not sufficient_clusters:
            logger.warning("没有找到满足资源需求的集群")
            return None

        # Select the best cluster based on score
        best_cluster = max(sufficient_clusters.items(), key=lambda x: x[1].score)[0]
        best_score = sufficient_clusters[best_cluster].score

        # Log selection details
        if len(sufficient_clusters) > 1:
            # Log details for load balancing
            for name, health in sufficient_clusters.items():
                resources = health.resources
                cpu_free = resources.get("cpu_free", 0)
                cpu_total = resources.get("cpu_total", 0)
                gpu_free = resources.get("gpu_free", 0)
                gpu_total = resources.get("gpu_total", 0)
                cpu_utilization = resources.get("cpu_utilization", 0)

                logger.info(f"集群 [{name}] 评分={health.score:.1f}, "
                           f"CPU负载={cpu_utilization:.1%} ({cpu_free}/{cpu_total}), "
                           f"GPU={gpu_free}/{gpu_total}")

        logger.info(f"选择最佳集群 [{best_cluster}] 进行负载均衡: "
                   f"评分={best_score:.1f}")

        return best_cluster

    def get_best_cluster(self, requirements: Optional[Dict] = None) -> Optional[str]:
        """Get the best cluster based on current health and requirements.

        This is a wrapper method for select_best_cluster to maintain API compatibility.

        Args:
            requirements: Dictionary containing resource requirements and tags

        Returns:
            Name of the best cluster or None if no suitable cluster found
        """
        if requirements is None:
            requirements = {}

        # Extract resource requirements
        required_resources = requirements.get("resources", {})

        return self.select_best_cluster(required_resources)

    def get_or_create_connection(self, cluster_name: str):
        """获取或创建集群连接"""
        with self._lock:
            if cluster_name in self.active_connections:
                try:
                    # 验证连接是否仍然有效
                    ray.init(
                        address=f"ray://{self.clusters[cluster_name].head_address}",
                        ignore_reinit_error=True
                    )
                    if ray.is_initialized():
                        conn_info = self.active_connections[cluster_name]
                        logger.info(f"🔁 使用现有连接到集群 [{cluster_name}] ({conn_info['address']})")
                        return self.active_connections[cluster_name]
                except:
                    # 连接无效，移除
                    del self.active_connections[cluster_name]
                    import traceback
                    traceback.print_exc()

            # 创建新连接
            try:
                ray_address = f"ray://{self.clusters[cluster_name].head_address}"
                logger.info(f"🔄 正在创建到集群 [{cluster_name}] 的新连接: {ray_address}")

                ray.init(
                    address=ray_address,
                    ignore_reinit_error=True,
                    logging_level=logging.INFO
                )

                if not ray.is_initialized():
                    raise ConnectionError("Ray 连接失败")

                self.active_connections[cluster_name] = {
                    "address": ray_address,
                    "created_at": datetime.now(),
                    "last_used": datetime.now()
                }

                # 获取并显示集群资源信息
                try:
                    avail_resources = ray.available_resources()
                    total_resources = ray.cluster_resources()
                    cpu_free = avail_resources.get("CPU", 0)
                    cpu_total = total_resources.get("CPU", 0)
                    gpu_free = avail_resources.get("GPU", 0)
                    gpu_total = total_resources.get("GPU", 0)
                    node_count = len(ray.nodes())

                    logger.info(f"✅ 连接到集群 [{cluster_name}] 成功")
                    logger.info(f"   CPU: {cpu_free}/{cpu_total}")
                    logger.info(f"   GPU: {gpu_free}/{gpu_total}")
                    logger.info(f"   节点数: {node_count}")
                except Exception as e:
                    logger.warning(f"获取集群 [{cluster_name}] 资源信息时出错: {e}")

                return self.active_connections[cluster_name]

            except Exception as e:
                logger.error(f"❌ 连接到集群 [{cluster_name}] 失败: {e}")
                import traceback
                traceback.print_exc()
                return None