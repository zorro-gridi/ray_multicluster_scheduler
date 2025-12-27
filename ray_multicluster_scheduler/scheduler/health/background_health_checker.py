"""
Background Health Checker Actor

这是一个 detached actor，作为后台常驻服务持续运行，定期收集集群资源快照并推送到 ClusterResourceMonitor。

特点：
1. lifetime="detached" - 不随用户程序退出
2. 每 20 秒自动收集一次所有集群的快照
3. 将快照推送到全局的 ClusterResourceMonitor
4. 用户调用 health_check 时直接从 ClusterResourceMonitor 读取，无需等待
"""

import time
import ray
import threading
from typing import Dict, List, Any
from ray_multicluster_scheduler.common.model import ClusterMetadata, ResourceSnapshot
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker

logger = get_logger(__name__)


@ray.remote(name="background_health_checker", lifetime="detached", max_concurrency=1)
class BackgroundHealthChecker:
    """
    后台健康检查 Actor

    作为常驻服务运行，定期收集集群资源快照并推送到 ClusterResourceMonitor。
    """

    def __init__(
        self,
        cluster_metadata_list: List[ClusterMetadata],
        client_pool_config: Dict[str, Any],
        detached_monitor_ref,
        refresh_interval: int = 20
    ):
        """
        初始化后台健康检查器

        Args:
            cluster_metadata_list: 集群元数据列表
            client_pool_config: 客户端连接池配置
            detached_monitor_ref: ClusterResourceMonitor actor 引用
            refresh_interval: 刷新间隔（秒），默认 20 秒
        """
        self.cluster_metadata_list = cluster_metadata_list
        self.client_pool_config = client_pool_config
        self.detached_monitor = detached_monitor_ref
        self.refresh_interval = refresh_interval
        self.running = True
        self.iteration_count = 0
        self._monitoring_thread = None  # 后台监控线程

        logger.info("="*60)
        logger.info("BackgroundHealthChecker 初始化完成")
        logger.info(f"监控 {len(cluster_metadata_list)} 个集群")
        logger.info(f"刷新间隔: {refresh_interval} 秒")
        logger.info("="*60)

    def start_monitoring(self):
        """
        启动后台监控循环（在后台线程中运行）

        这个方法会立即返回，监控循环在后台线程中持续运行。
        """
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            logger.warning("⚠️  监控线程已经在运行")
            return

        logger.info("🚀 启动后台监控线程...")
        self._monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self._monitoring_thread.start()
        logger.info("✅ 后台监控线程已启动")

    def _monitoring_loop(self):
        """
        监控循环（在后台线程中运行）
        """
        logger.info("🚀 后台监控循环已启动")

        # 首次立即收集一次
        try:
            logger.info("🔄 执行首次快照收集...")
            self._collect_and_push_snapshots()
            logger.info("✅ 首次快照收集完成")
        except Exception as e:
            logger.error(f"❌ 首次快照收集失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

        # 进入定期刷新循环
        while self.running:
            try:
                # 等待刷新间隔
                time.sleep(self.refresh_interval)

                # 收集并推送快照
                self.iteration_count += 1
                logger.info(f"🔄 后台刷新 #{self.iteration_count}: 开始收集快照...")
                self._collect_and_push_snapshots()
                logger.info(f"✅ 后台刷新 #{self.iteration_count}: 完成")

            except Exception as e:
                logger.error(f"❌ 后台刷新 #{self.iteration_count} 失败: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # 发生错误后等待一段时间再重试
                time.sleep(10)

        logger.info("⚠️  后台监控循环已停止")

    def _collect_and_push_snapshots(self):
        """
        收集所有集群的快照并推送到 ClusterResourceMonitor
        """
        # 重新创建 RayClientPool（因为 actor 中不能直接传递对象）
        from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
        from ray_multicluster_scheduler.control_plane.config import ConfigManager

        # 创建临时的 ConfigManager 和 ClientPool
        config_manager = ConfigManager()
        config_manager.clusters = self.client_pool_config
        client_pool = RayClientPool(config_manager)

        # 记录要监控的集群
        cluster_names = [m.name for m in self.cluster_metadata_list]
        logger.info(f"   📋 准备收集 {len(cluster_names)} 个集群的快照: {cluster_names}")

        # 创建 HealthChecker 并收集快照
        health_checker = HealthChecker(
            self.cluster_metadata_list,
            client_pool,
            detached_monitor=None  # 不使用 detached_monitor，避免循环依赖
        )

        # 执行健康检查（use_detached_monitor=False 强制本地收集）
        snapshots = health_checker.check_health(use_detached_monitor=False)

        # 详细记录收集结果
        collected_clusters = list(snapshots.keys()) if snapshots else []
        logger.info(f"   📊 实际收集到 {len(collected_clusters)} 个集群的快照: {collected_clusters}")

        # 检查是否有集群未被收集
        missing_clusters = set(cluster_names) - set(collected_clusters)
        if missing_clusters:
            logger.warning(f"   ⚠️  以下集群未收集到快照: {list(missing_clusters)}")

        if snapshots:
            # 推送到 ClusterResourceMonitor
            try:
                # 注意：在 actor 方法内部调用其他 actor 方法时，不要使用 ray.get()
                # 直接调用 .remote() 即可，避免嵌套 ObjectRef
                self.detached_monitor.update_snapshots.remote(snapshots)
                logger.info(f"   ✅ 推送 {len(snapshots)} 个快照到 ClusterResourceMonitor")
                for cluster_name, snapshot in snapshots.items():
                    logger.info(f"      - {cluster_name}: CPU={snapshot.cluster_cpu_usage_percent:.1f}%, "
                              f"MEM={snapshot.cluster_mem_usage_percent:.1f}%, "
                              f"节点数={snapshot.node_count}")
            except Exception as e:
                logger.error(f"   ❌ 推送快照到 ClusterResourceMonitor 失败: {e}")
                import traceback
                logger.error(f"   异常堆栈:\n{traceback.format_exc()}")
                raise
        else:
            logger.warning("   ⚠️  未收集到任何快照")

    def stop_monitoring(self):
        """
        停止后台监控
        """
        logger.info("🛑 停止后台监控...")
        self.running = False

    def ping(self) -> str:
        """
        健康检查

        Returns:
            状态信息
        """
        return f"BackgroundHealthChecker alive. Iterations: {self.iteration_count}, Running: {self.running}"

    def get_status(self) -> Dict[str, Any]:
        """
        获取状态信息

        Returns:
            包含状态信息的字典
        """
        return {
            "running": self.running,
            "iteration_count": self.iteration_count,
            "refresh_interval": self.refresh_interval,
            "clusters": [m.name for m in self.cluster_metadata_list]
        }
