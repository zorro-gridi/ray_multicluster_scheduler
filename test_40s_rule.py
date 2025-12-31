#!/usr/bin/env python
"""测试负载均衡策略规则：上一个任务提交调度的时间距离当前不足40秒的集群不接受新任务的提交"""

import time
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath('.'))

from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import ClusterSubmissionHistory


def test_40s_rule():
    """测试40秒规则"""
    print("开始测试负载均衡策略的40秒规则...")

    # 创建集群提交历史记录实例
    history = ClusterSubmissionHistory()

    # 测试场景1: 集群在40秒内提交了任务，不应该接受新任务
    print("\n测试场景1: 集群在40秒内提交了任务，不应该接受新任务")
    cluster_name = "test_cluster"

    # 记录一次任务提交
    history.record_submission(cluster_name)
    print(f"记录了集群 {cluster_name} 的任务提交")

    # 检查集群是否可用（应该不可用）
    is_available = history.is_cluster_available(cluster_name)
    print(f"集群 {cluster_name} 在任务提交后是否可用: {is_available}")
    assert is_available == False, "40秒内已提交任务的集群应该不可用"
    print("✓ 测试场景1通过")

    # 测试场景2: 检查剩余等待时间
    print("\n测试场景2: 检查剩余等待时间")
    remaining_time = history.get_remaining_wait_time(cluster_name)
    print(f"集群 {cluster_name} 剩余等待时间: {remaining_time:.2f}秒")
    # 由于刚记录了提交时间，剩余时间应该接近40秒
    assert 39.0 <= remaining_time <= 40.0, f"剩余时间应该接近40秒，实际: {remaining_time}"
    print("✓ 测试场景2通过")

    # 测试场景3: 集群在40秒后应该可以接受新任务
    print("\n测试场景3: 模拟等待时间后集群应该可以接受新任务")
    # 注意：我们不实际等待40秒，而是通过时间操作验证逻辑
    # 记录当前时间戳并手动设置为40秒前的提交时间
    import time
    current_time = time.time()

    # 手动设置提交时间为40秒前
    history._last_submission_times[cluster_name] = current_time - 40.0

    # 检查集群是否可用（应该可用，因为超过了40秒）
    is_available_after_timeout = history.is_cluster_available(cluster_name)
    print(f"集群 {cluster_name} 在40秒后是否可用: {is_available_after_timeout}")
    assert is_available_after_timeout == True, "超过40秒的集群应该可用"
    print("✓ 测试场景3通过")

    # 测试场景4: 多个集群的过滤
    print("\n测试场景4: 多个集群的过滤")
    clusters = ["cluster_a", "cluster_b", "cluster_c"]

    # 记录cluster_b的提交时间（40秒前，所以应该可用）
    history._last_submission_times["cluster_b"] = current_time - 40.0
    # 记录cluster_c的提交时间（刚刚，所以应该不可用）
    history.record_submission("cluster_c")

    available_clusters = history.get_available_clusters(clusters)
    print(f"所有集群: {clusters}")
    print(f"可用集群: {available_clusters}")

    # 应该只有cluster_a和cluster_b可用（cluster_c刚刚提交过）
    expected_clusters = ["cluster_a", "cluster_b"]
    assert set(available_clusters) == set(expected_clusters), f"预期 {expected_clusters}, 实际 {available_clusters}"
    print("✓ 测试场景4通过")

    print("\n所有40秒规则测试通过！负载均衡策略的40秒间隔功能正常工作。")


def test_timing_behavior():
    """测试时间行为"""
    print("\n测试时间行为...")

    history = ClusterSubmissionHistory()
    cluster_name = "timing_test_cluster"

    # 记录提交时间
    history.record_submission(cluster_name)

    # 等待一小段时间
    time.sleep(0.01)  # 等待10毫秒

    # 检查剩余时间是否减少了
    remaining_time = history.get_remaining_wait_time(cluster_name)
    print(f"剩余时间: {remaining_time:.3f}秒 (应该略小于40秒)")

    # 应该略小于40秒
    assert remaining_time < 40.0, f"剩余时间应该小于40秒，实际: {remaining_time}"
    assert remaining_time > 39.9, f"剩余时间应该大于39.9秒（因为我们只等了0.01秒），实际: {remaining_time}"

    print("✓ 时间行为测试通过")


if __name__ == "__main__":
    test_40s_rule()
    test_timing_behavior()
    print("\n🎉 所有测试通过！40秒规则实现正确！")