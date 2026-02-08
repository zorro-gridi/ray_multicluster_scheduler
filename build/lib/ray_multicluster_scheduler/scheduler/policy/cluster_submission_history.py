"""集群任务提交历史记录管理器，用于跟踪每个集群最近一次任务提交的时间"""

import time
import threading
from typing import Dict, Optional
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.config import settings, ExecutionMode

logger = get_logger(__name__)


class ClusterSubmissionHistory:
    """集群任务提交历史记录管理器 - 线程安全版本"""

    def __init__(self):
        # 存储每个集群最近一次任务提交的时间戳
        self._last_submission_times: Dict[str, float] = {}
        # 默认等待时间：40秒
        self.SUBMISSION_WAIT_TIME = 40.0
        # 添加锁保护并发访问
        self._lock = threading.RLock()

    def record_submission(self, cluster_name: str):
        """记录集群任务提交时间"""
        with self._lock:
            current_time = time.time()
            self._last_submission_times[cluster_name] = current_time
            logger.debug(f"记录集群 {cluster_name} 任务提交时间: {current_time}")

    def get_last_submission_time(self, cluster_name: str) -> Optional[float]:
        """获取集群最近一次任务提交的时间"""
        with self._lock:
            return self._last_submission_times.get(cluster_name)

    def is_cluster_available(self, cluster_name: str) -> bool:
        """检查集群是否可用（上次任务提交时间是否超过等待时间）"""
        with self._lock:
            last_submission_time = self._last_submission_times.get(cluster_name)

            if last_submission_time is None:
                # 如果集群从未提交过任务，则认为可用
                return True

            current_time = time.time()
            time_since_last_submission = current_time - last_submission_time

            # 如果距离上次提交时间超过等待时间，则集群可用
            is_available = time_since_last_submission >= self.SUBMISSION_WAIT_TIME

            logger.debug(f"集群 {cluster_name} 上次提交时间: {last_submission_time}, "
                        f"距离当前: {time_since_last_submission:.2f}s, "
                        f"是否可用: {is_available}")

            return is_available

    def is_cluster_available_and_record(self, cluster_name: str, is_top_level_task: bool = True) -> bool:
        """
        原子操作：检查集群是否可用，如果可用则立即记录提交时间

        这个方法解决了竞态条件问题：
        1. 检查和记录在同一个锁内完成
        2. 防止多个任务同时通过检查
        3. 确保40秒规则的严格执行

        Args:
            cluster_name: 集群名称
            is_top_level_task: 是否为顶级任务，顶级任务受40秒限制，子任务不受限制

        Returns:
            bool: True表示集群可用且已记录提交时间，False表示集群不可用
        """
        # 如果是子任务，绕过40秒限制
        if not is_top_level_task:
            with self._lock:
                current_time = time.time()
                # 使用特殊键记录子任务，不影响顶级任务的限制
                sub_task_key = f"{cluster_name}_subtask"
                self._last_submission_times[sub_task_key] = current_time
                logger.debug(f"子任务，记录集群 {cluster_name} 提交时间: {current_time}")
                return True

        # 如果是顶级任务，应用40秒限制
        with self._lock:
            # 串行模式：检查是否有任务正在执行
            if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
                from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
                if TaskQueue.is_cluster_busy(cluster_name):
                    logger.debug(f"集群 {cluster_name} 在串行模式下繁忙，有任务正在执行")
                    return False

            last_submission_time = self._last_submission_times.get(cluster_name)

            if last_submission_time is None:
                # 集群从未提交过，可用
                current_time = time.time()
                self._last_submission_times[cluster_name] = current_time
                logger.debug(f"集群 {cluster_name} 首次顶级任务提交，记录时间: {current_time}")
                return True

            current_time = time.time()
            time_since_last_submission = current_time - last_submission_time

            if time_since_last_submission >= self.SUBMISSION_WAIT_TIME:
                # 集群可用，记录新的提交时间
                self._last_submission_times[cluster_name] = current_time
                logger.debug(f"集群 {cluster_name} 顶级任务可用，记录新的提交时间: {current_time}")
                return True
            else:
                # 集群不可用
                remaining_time = self.SUBMISSION_WAIT_TIME - time_since_last_submission
                logger.debug(f"集群 {cluster_name} 顶级任务不可用，还需等待 {remaining_time:.2f}s")
                return False

    def get_available_clusters(self, cluster_names: list) -> list:
        """获取可用集群列表（排除40秒内已提交任务的集群）"""
        with self._lock:
            available_clusters = []
            for cluster_name in cluster_names:
                if self.is_cluster_available(cluster_name):
                    available_clusters.append(cluster_name)

            logger.debug(f"可用集群列表: {available_clusters}")
            return available_clusters

    def get_remaining_wait_time(self, cluster_name: str) -> float:
        """获取集群还需要等待的时间（如果小于0表示可以提交）"""
        with self._lock:
            last_submission_time = self._last_submission_times.get(cluster_name)

            if last_submission_time is None:
                # 如果集群从未提交过任务，则不需要等待
                return 0.0

            current_time = time.time()
            time_since_last_submission = current_time - last_submission_time
            remaining_wait_time = self.SUBMISSION_WAIT_TIME - time_since_last_submission

            return max(0.0, remaining_wait_time)