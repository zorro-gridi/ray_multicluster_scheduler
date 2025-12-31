"""集群任务提交历史记录管理器，用于跟踪每个集群最近一次任务提交的时间"""

import time
from typing import Dict, Optional
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class ClusterSubmissionHistory:
    """集群任务提交历史记录管理器"""

    def __init__(self):
        # 存储每个集群最近一次任务提交的时间戳
        self._last_submission_times: Dict[str, float] = {}
        # 默认等待时间：40秒
        self.SUBMISSION_WAIT_TIME = 40.0

    def record_submission(self, cluster_name: str):
        """记录集群任务提交时间"""
        current_time = time.time()
        self._last_submission_times[cluster_name] = current_time
        logger.debug(f"记录集群 {cluster_name} 任务提交时间: {current_time}")

    def get_last_submission_time(self, cluster_name: str) -> Optional[float]:
        """获取集群最近一次任务提交的时间"""
        return self._last_submission_times.get(cluster_name)

    def is_cluster_available(self, cluster_name: str) -> bool:
        """检查集群是否可用（上次任务提交时间是否超过等待时间）"""
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

    def get_available_clusters(self, cluster_names: list) -> list:
        """获取可用集群列表（排除40秒内已提交任务的集群）"""
        available_clusters = []
        for cluster_name in cluster_names:
            if self.is_cluster_available(cluster_name):
                available_clusters.append(cluster_name)

        logger.debug(f"可用集群列表: {available_clusters}")
        return available_clusters

    def get_remaining_wait_time(self, cluster_name: str) -> float:
        """获取集群还需要等待的时间（如果小于0表示可以提交）"""
        last_submission_time = self._last_submission_times.get(cluster_name)

        if last_submission_time is None:
            # 如果集群从未提交过任务，则不需要等待
            return 0.0

        current_time = time.time()
        time_since_last_submission = current_time - last_submission_time
        remaining_wait_time = self.SUBMISSION_WAIT_TIME - time_since_last_submission

        return max(0.0, remaining_wait_time)