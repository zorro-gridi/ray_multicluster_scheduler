"""
增强版评分策略
基于集群管理器的真实评分机制实现负载均衡
"""

from typing import Dict, List
from ray_multicluster_scheduler.common.model import TaskDescription, SchedulingDecision, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class EnhancedScoreBasedPolicy:
    """增强版基于资源可用性的调度策略"""

    def __init__(self):
        pass

    def evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot],
                 cluster_metadata: Dict[str, ClusterMetadata] = None) -> SchedulingDecision:
        """使用增强评分机制评估集群"""

        if not cluster_metadata:
            cluster_metadata = {}

        # 计算每个集群的评分
        scores = {}

        for cluster_name, snapshot in cluster_snapshots.items():
            # 获取集群配置信息
            config = cluster_metadata.get(cluster_name)

            # 使用新的资源指标
            cpu_total_cores = snapshot.cluster_cpu_total_cores
            cpu_used_cores = snapshot.cluster_cpu_used_cores
            cpu_free = round(cpu_total_cores - cpu_used_cores, 1)
            cpu_total = cpu_total_cores

            # 内存资源指标
            mem_total_mb = snapshot.cluster_mem_total_mb
            mem_used_mb = snapshot.cluster_mem_used_mb
            mem_free_mb = mem_total_mb - mem_used_mb

            # 对于MAC集群，我们没有特殊的MacCPU资源指标，使用默认值
            if config and ("mac" in cluster_name.lower() or any("mac" in tag.lower() for tag in config.tags)):
                # 目前没有MacCPU指标，使用默认CPU指标
                pass

            # GPU资源暂时不可用，使用默认值
            gpu_free = 0
            gpu_total = 0

            # 如果没有集群配置信息，使用默认值
            weight = config.weight if config else 1.0
            prefer = config.prefer if config else False

            # 计算评分
            if cpu_free <= 0:
                score = -1
            else:
                # 基础评分 = 可用CPU * 集群权重
                base_score = cpu_free * weight

                # 内存资源加成 = 可用内存(GB) * 0.1 * 集群权重
                memory_available_gb = mem_free_mb / 1024.0  # Convert MB to GB
                memory_bonus = memory_available_gb * 0.1 * weight

                # GPU 资源加成
                gpu_bonus = gpu_free * 5  # GPU资源更宝贵

                # 偏好集群加成
                preference_bonus = 1.2 if prefer else 1.0

                # 负载均衡因子：资源利用率越低得分越高
                cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
                load_balance_factor = 1.0 - cpu_utilization  # 负载越低因子越高

                # 最终评分
                score = (base_score + memory_bonus + gpu_bonus) * preference_bonus * load_balance_factor

            scores[cluster_name] = {
                'score': score,
                'cpu_free': cpu_free,
                'cpu_total': cpu_total,
                'gpu_free': gpu_free,
                'gpu_total': gpu_total
            }

        # 过滤掉评分小于0的集群
        valid_scores = {name: info for name, info in scores.items() if info['score'] >= 0}

        # 检查是否有可用集群
        if not valid_scores:
            logger.warning("没有可用的集群进行评分评估")
            return SchedulingDecision(
                task_id=task_desc.task_id,
                cluster_name="",
                reason="没有可用的集群进行评分评估"
            )

        # 选择评分最高的集群
        best_cluster = max(valid_scores.items(), key=lambda x: x[1]['score'])
        best_cluster_name = best_cluster[0]
        best_cluster_info = best_cluster[1]

        logger.debug(f"增强版评分策略评估结果: {valid_scores}")

        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name=best_cluster_name,
            reason=f"通过增强版评分策略选择集群 {best_cluster_name}，"
                   f"评分为 {best_cluster_info['score']:.2f}，"
                   f"可用资源: CPU={best_cluster_info['cpu_free']}, GPU={best_cluster_info['gpu_free']}"
        )