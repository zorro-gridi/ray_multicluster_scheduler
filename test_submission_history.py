#!/usr/bin/env python
"""测试集群任务提交历史记录功能"""

import time
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath('.'))

from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import ClusterSubmissionHistory


def test_cluster_submission_history():
    """测试集群提交历史记录功能"""
    print("开始测试集群任务提交历史记录功能...")

    # 创建集群提交历史记录实例
    history = ClusterSubmissionHistory()

    # 测试1: 检查未记录的集群是否可用
    print("\n测试1: 检查未记录的集群是否可用")
    cluster_name = "test_cluster"
    is_available = history.is_cluster_available(cluster_name)
    print(f"集群 {cluster_name} 是否可用: {is_available}")
    assert is_available == True, "未记录的集群应该可用"
    print("✓ 测试1通过")

    # 测试2: 记录提交时间后检查集群是否不可用（在40秒内）
    print("\n测试2: 记录提交时间后检查集群是否不可用（在40秒内）")
    history.record_submission(cluster_name)
    is_available = history.is_cluster_available(cluster_name)
    print(f"集群 {cluster_name} 是否可用: {is_available}")
    assert is_available == False, "刚提交任务的集群应该不可用"
    print("✓ 测试2通过")

    # 测试3: 检查剩余等待时间
    print("\n测试3: 检查剩余等待时间")
    remaining_time = history.get_remaining_wait_time(cluster_name)
    print(f"集群 {cluster_name} 剩余等待时间: {remaining_time:.2f}秒")
    assert remaining_time > 0, "刚提交任务的集群应该有正的等待时间"
    print("✓ 测试3通过")

    # 测试4: 等待一段时间后检查集群是否可用
    print("\n测试4: 等待0.1秒后检查集群是否可用（实际不会等待40秒，只是验证逻辑）")
    # 注意：我们不会实际等待40秒，只是验证剩余等待时间会减少
    time.sleep(0.1)  # 等待0.1秒
    remaining_time_after_wait = history.get_remaining_wait_time(cluster_name)
    print(f"等待0.1秒后，集群 {cluster_name} 剩余等待时间: {remaining_time_after_wait:.2f}秒")
    assert remaining_time_after_wait < remaining_time, "等待后剩余时间应该减少"
    print("✓ 测试4通过")

    # 测试5: 检查可用集群列表
    print("\n测试5: 检查可用集群列表")
    clusters = ["cluster1", "cluster2", "cluster3"]

    # 记录cluster2的提交时间
    history.record_submission("cluster2")

    available_clusters = history.get_available_clusters(clusters)
    print(f"可用集群列表: {available_clusters}")
    expected_clusters = ["cluster1", "cluster3"]  # cluster2应该被排除
    assert set(available_clusters) == set(expected_clusters), f"预期 {expected_clusters}, 实际 {available_clusters}"
    print("✓ 测试5通过")

    print("\n所有测试通过！集群任务提交历史记录功能正常工作。")


if __name__ == "__main__":
    test_cluster_submission_history()