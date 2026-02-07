"""
测试 rules 验证模块

此模块用于验证自定义 rules 是否被正确加载和应用。
"""

from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


def calculate_cluster_metrics(
    cluster_name: str,
    cpu_usage: float,
    memory_usage: float,
    task_count: int,
) -> dict:
    """
    计算集群的综合指标得分。

    Args:
        cluster_name: 集群名称
        cpu_usage: CPU 使用率 (0.0 - 1.0)
        memory_usage: 内存使用率 (0.0 - 1.0)
        task_count: 当前任务数量

    Returns:
        包含综合指标得分的字典，包括：
        - score: 综合得分 (0.0 - 1.0)
        - status: 集群状态 (healthy/warning/critical)
        - recommendations: 优化建议列表

    Raises:
        ValueError: 如果使用率参数超出有效范围

    Examples:
        >>> calculate_cluster_metrics("cluster-1", 0.5, 0.6, 10)
        {'score': 0.45, 'status': 'healthy', 'recommendations': []}
    """
    # 参数验证
    if not 0.0 <= cpu_usage <= 1.0:
        raise ValueError(f"CPU 使用率必须在 0.0-1.0 之间，当前值: {cpu_usage}")
    if not 0.0 <= memory_usage <= 1.0:
        raise ValueError(f"内存使用率必须在 0.0-1.0 之间，当前值: {memory_usage}")

    logger.info(
        f"ClusterMetrics - 计算集群指标: {cluster_name}, "
        f"CPU: {cpu_usage:.2f}, Memory: {memory_usage:.2f}, Tasks: {task_count}"
    )

    # 计算综合得分
    resource_score = (cpu_usage + memory_usage) / 2
    task_factor = min(task_count / 100, 1.0)
    final_score = (resource_score * 0.7) + (task_factor * 0.3)

    # 确定状态
    if final_score < 0.5:
        status = "healthy"
        recommendations = []
    elif final_score < 0.8:
        status = "warning"
        recommendations = ["考虑减少新任务分配"]
    else:
        status = "critical"
        recommendations = ["暂停新任务分配", "考虑迁移部分任务"]

    logger.info(f"ClusterMetrics - 计算完成: score={final_score:.2f}, status={status}")

    return {
        "score": final_score,
        "status": status,
        "recommendations": recommendations,
    }
