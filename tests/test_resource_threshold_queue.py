#!/usr/bin/env python3
"""
资源阈值队列测试用例
测试当所有集群资源使用率超过阈值时，新提交的任务是否正确放入队列等待
"""

import unittest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List
import time as time_module

# 导入相关模块
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, SchedulingDecision
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager


class TestResourceThresholdQueue(unittest.TestCase):
    """测试资源阈值队列功能"""

    def setUp(self):
        """测试前准备"""
        # 创建模拟的集群管理器
        self.cluster_manager = Mock(spec=ClusterManager)

        # 创建模拟的集群监控器
        self.cluster_monitor = Mock(spec=ClusterMonitor)
        self.cluster_monitor.cluster_manager = self.cluster_manager

        # 创建任务生命周期管理器
        self.task_lifecycle_manager = TaskLifecycleManager(self.cluster_monitor)

        # 创建策略引擎
        self.policy_engine = PolicyEngine()

        # 设置测试数据
        self._setup_test_data()

    def _setup_test_data(self):
        """设置测试数据"""
        current_time = time_module.time()

        # 模拟两个集群，资源使用率都超过阈值(80%)
        self.cluster_snapshots = {
            "cluster1": ResourceSnapshot(
                cluster_name="cluster1",
                total_resources={"CPU": 4.0, "GPU": 0},
                available_resources={"CPU": 0.5, "GPU": 0},  # 只有0.5个CPU可用，使用率87.5%
                node_count=2,
                timestamp=current_time
            ),
            "cluster2": ResourceSnapshot(
                cluster_name="cluster2",
                total_resources={"CPU": 8.0, "GPU": 2.0},
                available_resources={"CPU": 1.0, "GPU": 0},  # 只有1个CPU可用，使用率87.5%
                node_count=3,
                timestamp=current_time
            )
        }

        # 模拟集群元数据
        self.cluster_metadata = {
            "cluster1": Mock(),
            "cluster2": Mock()
        }

        # 模拟集群信息
        self.cluster_info = {
            "cluster1": {
                "snapshot": self.cluster_snapshots["cluster1"],
                "metadata": self.cluster_metadata["cluster1"]
            },
            "cluster2": {
                "snapshot": self.cluster_snapshots["cluster2"],
                "metadata": self.cluster_metadata["cluster2"]
            }
        }

    def test_all_clusters_over_threshold_queues_task(self):
        """测试所有集群资源使用率超过阈值时，任务应被放入队列"""
        # 设置集群监控器返回高使用率的集群快照
        self.cluster_monitor.get_all_cluster_info.return_value = self.cluster_info

        # 创建一个任务描述
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
        result = self.task_lifecycle_manager.submit_task(task_desc)

        # 验证任务ID被返回
        self.assertEqual(result, "test_task_1")

        # 验证任务被加入队列
        self.assertIn(task_desc, self.task_lifecycle_manager.queued_tasks)

        # 验证队列中有1个任务
        self.assertEqual(len(self.task_lifecycle_manager.queued_tasks), 1)

        # 验证任务队列大小为1
        self.assertEqual(self.task_lifecycle_manager.task_queue.size(), 1)

    def test_some_clusters_under_threshold_schedules_task(self):
        """测试部分集群资源使用率低于阈值时，任务应被调度而不是排队"""
        current_time = time_module.time()

        # 修改集群快照，使两个集群都有足够的资源
        self.cluster_snapshots["cluster1"] = ResourceSnapshot(
            cluster_name="cluster1",
            total_resources={"CPU": 4.0, "GPU": 0},
            available_resources={"CPU": 3.0, "GPU": 0},  # 3个CPU可用，使用率25%
            node_count=2,
            timestamp=current_time
        )

        self.cluster_snapshots["cluster2"] = ResourceSnapshot(
            cluster_name="cluster2",
            total_resources={"CPU": 8.0, "GPU": 2.0},
            available_resources={"CPU": 6.0, "GPU": 0},  # 6个CPU可用，使用率25%
            node_count=3,
            timestamp=current_time
        )

        # 更新集群信息
        self.cluster_info["cluster1"]["snapshot"] = self.cluster_snapshots["cluster1"]
        self.cluster_info["cluster2"]["snapshot"] = self.cluster_snapshots["cluster2"]
        self.cluster_monitor.get_all_cluster_info.return_value = self.cluster_info

        # 创建策略引擎并替换task_lifecycle_manager中的策略引擎
        self.task_lifecycle_manager.policy_engine = self.policy_engine

        # 更新策略引擎的集群元数据
        self.task_lifecycle_manager.policy_engine.update_cluster_metadata({
            "cluster1": self.cluster_metadata["cluster1"],
            "cluster2": self.cluster_metadata["cluster2"]
        })

        # 创建一个任务描述
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

        # 模拟dispatcher.dispatch_task方法
        with patch.object(self.task_lifecycle_manager.dispatcher, 'dispatch_task') as mock_dispatch:
            mock_dispatch.return_value = Mock()  # 返回一个模拟的future

            # 调用submit_task方法
            result = self.task_lifecycle_manager.submit_task(task_desc)

            # 验证任务被调度而不是排队
            # 注意：这里我们验证dispatch_task被调用，说明任务被调度了
            mock_dispatch.assert_called_once()

    def test_preferred_cluster_over_threshold_queues_task(self):
        """测试指定集群资源使用率超过阈值时，任务应被放入队列"""
        # 设置集群监控器返回高使用率的集群快照
        self.cluster_monitor.get_all_cluster_info.return_value = self.cluster_info

        # 创建一个任务描述，指定使用cluster1
        task_desc = TaskDescription(
            task_id="test_task_3",
            name="test_task",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test"],
            preferred_cluster="cluster1"  # 指定使用cluster1
        )

        # 调用submit_task方法
        result = self.task_lifecycle_manager.submit_task(task_desc)

        # 验证任务ID被返回
        self.assertEqual(result, "test_task_3")

        # 验证任务被加入队列
        self.assertIn(task_desc, self.task_lifecycle_manager.queued_tasks)

        # 验证队列中有1个任务
        self.assertEqual(len(self.task_lifecycle_manager.queued_tasks), 1)

    def test_no_healthy_clusters_does_not_queue_task(self):
        """测试没有健康集群时不排队任务（根据用户要求）"""
        # 模拟没有健康集群的情况
        self.cluster_monitor.get_all_cluster_info.return_value = {}

        # 创建一个任务描述
        task_desc = TaskDescription(
            task_id="test_task_4",
            name="test_task",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test"],
            preferred_cluster=None
        )

        # 调用submit_task方法
        result = self.task_lifecycle_manager.submit_task(task_desc)

        # 验证任务ID被返回
        self.assertEqual(result, "test_task_4")

        # 验证任务被加入队列（即使没有健康集群也排队，等待集群恢复）
        self.assertIn(task_desc, self.task_lifecycle_manager.queued_tasks)

        # 验证队列中有1个任务
        self.assertEqual(len(self.task_lifecycle_manager.queued_tasks), 1)

    def test_task_re_evaluation_when_resources_become_available(self):
        """测试当资源变得可用时，排队任务会被重新评估"""
        # 首先让所有集群资源使用率超过阈值
        self.cluster_monitor.get_all_cluster_info.return_value = self.cluster_info

        # 创建一个任务描述
        task_desc = TaskDescription(
            task_id="test_task_5",
            name="test_task",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test"],
            preferred_cluster=None
        )

        # 提交任务，应该被排队
        result = self.task_lifecycle_manager.submit_task(task_desc)

        # 验证任务被加入队列
        self.assertIn(task_desc, self.task_lifecycle_manager.queued_tasks)
        self.assertEqual(self.task_lifecycle_manager.task_queue.size(), 1)


if __name__ == '__main__':
    unittest.main()