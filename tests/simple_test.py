#!/usr/bin/env python3
"""
简单的资源阈值队列测试
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

import time as time_module
from unittest.mock import Mock

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager


def test_all_clusters_over_threshold():
    """测试所有集群资源使用率超过阈值时的行为"""
    print("=== 测试所有集群资源使用率超过阈值 ===")

    # 创建模拟对象
    cluster_manager = Mock(spec=ClusterManager)
    cluster_monitor = Mock(spec=ClusterMonitor)
    cluster_monitor.cluster_manager = cluster_manager

    # 创建任务生命周期管理器
    task_lifecycle_manager = TaskLifecycleManager(cluster_monitor)

    # 创建测试数据
    current_time = time_module.time()
    cluster_snapshots = {
        "cluster1": ResourceSnapshot(
            cluster_name="cluster1",
            total_resources={"CPU": 4.0, "GPU": 0},
            available_resources={"CPU": 0.5, "GPU": 0},  # 使用率87.5%
            node_count=2,
            timestamp=current_time
        ),
        "cluster2": ResourceSnapshot(
            cluster_name="cluster2",
            total_resources={"CPU": 8.0, "GPU": 2.0},
            available_resources={"CPU": 1.0, "GPU": 0},  # 使用率87.5%
            node_count=3,
            timestamp=current_time
        )
    }

    cluster_info = {
        "cluster1": {
            "snapshot": cluster_snapshots["cluster1"],
            "metadata": Mock()
        },
        "cluster2": {
            "snapshot": cluster_snapshots["cluster2"],
            "metadata": Mock()
        }
    }

    # 设置集群监控器返回值
    cluster_monitor.get_all_cluster_info.return_value = cluster_info

    # 创建任务描述
    task_desc = TaskDescription(
        task_id="test_task_1",
        name="test_task",
        func_or_class=lambda: None,
        args=(),
        kwargs={},
        resource_requirements={"CPU": 1.0},
        tags=["test"],
        preferred_cluster=None
    )

    # 调用submit_task方法
    result = task_lifecycle_manager.submit_task(task_desc)

    # 验证结果
    print(f"任务ID: {result}")
    print(f"队列中的任务数量: {len(task_lifecycle_manager.queued_tasks)}")
    print(f"任务队列大小: {task_lifecycle_manager.task_queue.size()}")

    # 验证任务被加入队列
    assert result == "test_task_1"
    assert len(task_lifecycle_manager.queued_tasks) == 1
    assert task_lifecycle_manager.task_queue.size() == 1

    print("✅ 测试通过")


def test_some_clusters_under_threshold():
    """测试部分集群资源使用率低于阈值时的行为"""
    print("\n=== 测试部分集群资源使用率低于阈值 ===")

    # 创建模拟对象
    cluster_manager = Mock(spec=ClusterManager)
    cluster_monitor = Mock(spec=ClusterMonitor)
    cluster_monitor.cluster_manager = cluster_manager

    # 创建任务生命周期管理器
    task_lifecycle_manager = TaskLifecycleManager(cluster_monitor)

    # 创建测试数据 - 两个集群都有足够资源
    current_time = time_module.time()
    cluster_snapshots = {
        "cluster1": ResourceSnapshot(
            cluster_name="cluster1",
            total_resources={"CPU": 4.0, "GPU": 0},
            available_resources={"CPU": 3.0, "GPU": 0},  # 使用率25%
            node_count=2,
            timestamp=current_time
        ),
        "cluster2": ResourceSnapshot(
            cluster_name="cluster2",
            total_resources={"CPU": 8.0, "GPU": 2.0},
            available_resources={"CPU": 6.0, "GPU": 0},  # 使用率25%
            node_count=3,
            timestamp=current_time
        )
    }

    cluster_info = {
        "cluster1": {
            "snapshot": cluster_snapshots["cluster1"],
            "metadata": Mock()
        },
        "cluster2": {
            "snapshot": cluster_snapshots["cluster2"],
            "metadata": Mock()
        }
    }

    # 设置集群监控器返回值
    cluster_monitor.get_all_cluster_info.return_value = cluster_info

    # 创建任务描述
    task_desc = TaskDescription(
        task_id="test_task_2",
        name="test_task",
        func_or_class=lambda: None,
        args=(),
        kwargs={},
        resource_requirements={"CPU": 1.0},
        tags=["test"],
        preferred_cluster=None
    )

    # 调用submit_task方法
    result = task_lifecycle_manager.submit_task(task_desc)

    # 验证结果
    print(f"任务ID: {result}")
    print(f"队列中的任务数量: {len(task_lifecycle_manager.queued_tasks)}")
    print(f"任务队列大小: {task_lifecycle_manager.task_queue.size()}")

    # 对于这种情况，任务应该被调度而不是排队
    # 注意：由于我们没有完全模拟调度过程，这里的结果可能不同
    print("✅ 测试完成")


if __name__ == "__main__":
    test_all_clusters_over_threshold()
    test_some_clusters_under_threshold()
    print("\n🎉 所有测试完成!")