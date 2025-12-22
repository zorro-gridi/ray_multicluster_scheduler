#!/usr/bin/env python3
"""
submit_task 接口单元测试
诊断和解决 "Could not get client for cluster mac" 异常问题
"""

import sys
import os
import time
import unittest
from unittest.mock import Mock, patch, MagicMock
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    UnifiedScheduler,
    initialize_scheduler_environment,
    submit_task
)
from ray_multicluster_scheduler.app.client_api.submit_task import (
    initialize_scheduler as init_task_scheduler,
    _task_lifecycle_manager
)
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager, ClusterConfig
from ray_multicluster_scheduler.common.model import ResourceSnapshot, ClusterMetadata


class TestSubmitTaskInterface(unittest.TestCase):
    """submit_task 接口单元测试"""

    def setUp(self):
        """测试前准备"""
        # 清理全局状态
        from ray_multicluster_scheduler.app.client_api.unified_scheduler import _unified_scheduler
        if _unified_scheduler:
            _unified_scheduler._initialized = False
            _unified_scheduler.task_lifecycle_manager = None
            _unified_scheduler._config_file_path = None

        # 清理submit_task模块的全局状态
        from ray_multicluster_scheduler.app.client_api.submit_task import _task_lifecycle_manager, _initialization_attempted
        import ray_multicluster_scheduler.app.client_api.submit_task as submit_task_module
        submit_task_module._task_lifecycle_manager = None
        submit_task_module._initialization_attempted = False
        submit_task_module._task_results = {}

    def test_submit_task_with_mocked_components(self):
        """测试submit_task接口与模拟组件"""
        print("=" * 60)
        print("测试submit_task接口与模拟组件")
        print("=" * 60)

        # 创建模拟的集群管理器
        cluster_manager = Mock(spec=ClusterManager)

        # 创建模拟的集群监控器
        cluster_monitor = Mock(spec=ClusterMonitor)
        cluster_monitor.cluster_manager = cluster_manager

        # 创建模拟的TaskLifecycleManager
        task_lifecycle_manager = Mock(spec=TaskLifecycleManager)
        task_lifecycle_manager.submit_task_and_get_future.return_value = "mock_result"

        # 模拟集群配置
        cluster_configs = {
            "mac": ClusterConfig(
                name="mac",
                head_address="192.168.5.2:32546",
                dashboard="http://192.168.5.2:8265",
                prefer=True,
                weight=1.2,
                runtime_env={
                    "conda": "k8s",
                    "env_vars": {
                        "home_dir": "/Users/zorro"
                    }
                },
                tags=["macos", "arm64"]
            )
        }
        cluster_manager.clusters = cluster_configs

        # 模拟集群信息
        cluster_info = {
            "mac": {
                "metadata": cluster_configs["mac"],
                "snapshot": ResourceSnapshot(
                    cluster_name="mac",
                    available_resources={"CPU": 8.0, "GPU": 0},
                    total_resources={"CPU": 8.0, "GPU": 0},
                    node_count=1,
                    timestamp=time.time()
                )
            }
        }
        cluster_monitor.get_all_cluster_info.return_value = cluster_info
        cluster_monitor.refresh_resource_snapshots.return_value = None

        # 替换实际的TaskLifecycleManager
        with patch('ray_multicluster_scheduler.app.client_api.unified_scheduler.TaskLifecycleManager') as mock_task_lifecycle_manager_cls:
            mock_task_lifecycle_manager_cls.return_value = task_lifecycle_manager

            # 初始化调度器环境
            unified_scheduler = UnifiedScheduler()
            returned_manager = unified_scheduler.initialize_environment()

            # 验证TaskLifecycleManager被正确创建
            mock_task_lifecycle_manager_cls.assert_called_once()
            self.assertEqual(returned_manager, task_lifecycle_manager)

            # 初始化submit_task模块
            init_task_scheduler(task_lifecycle_manager)

            # 定义测试函数
            def test_function(x, y):
                return x + y

            # 提交任务
            task_id, result = submit_task(
                func=test_function,
                args=(1, 2),
                kwargs={},
                resource_requirements={"CPU": 1.0},
                tags=["test"],
                name="test_task",
                preferred_cluster="mac"
            )

            # 验证结果
            self.assertIsNotNone(task_id)
            self.assertEqual(result, "mock_result")

            # 验证submit_task_and_get_future被调用
            task_lifecycle_manager.submit_task_and_get_future.assert_called_once()

            print("✅ submit_task接口测试通过")

    def test_submit_task_without_initialization(self):
        """测试未初始化时submit_task的行为"""
        print("\n" + "=" * 60)
        print("测试未初始化时submit_task的行为")
        print("=" * 60)

        # 定义测试函数
        def test_function(x, y):
            return x + y

        # 尝试提交任务而不初始化调度器
        with self.assertRaises(Exception) as context:
            submit_task(
                func=test_function,
                args=(1, 2),
                kwargs={},
                resource_requirements={"CPU": 1.0},
                tags=["test"],
                name="test_task",
                preferred_cluster="mac"
            )

        # 验证异常信息
        self.assertIn("Scheduler not initialized", str(context.exception))
        print("✅ 未初始化时正确抛出异常")

    def test_submit_task_with_connection_failure(self):
        """测试集群连接失败时submit_task的行为"""
        print("\n" + "=" * 60)
        print("测试集群连接失败时submit_task的行为")
        print("=" * 60)

        # 创建模拟的集群管理器
        cluster_manager = Mock(spec=ClusterManager)

        # 创建模拟的集群监控器
        cluster_monitor = Mock(spec=ClusterMonitor)
        cluster_monitor.cluster_manager = cluster_manager

        # 创建模拟的TaskLifecycleManager
        task_lifecycle_manager = Mock(spec=TaskLifecycleManager)
        # 模拟submit_task_and_get_future抛出TaskSubmissionError异常
        from ray_multicluster_scheduler.common.exception import TaskSubmissionError
        task_lifecycle_manager.submit_task_and_get_future.side_effect = TaskSubmissionError("Could not get client for cluster mac")

        # 模拟集群配置
        cluster_configs = {
            "mac": ClusterConfig(
                name="mac",
                head_address="192.168.5.2:32546",
                dashboard="http://192.168.5.2:8265",
                prefer=True,
                weight=1.2,
                runtime_env={
                    "conda": "k8s",
                    "env_vars": {
                        "home_dir": "/Users/zorro"
                    }
                },
                tags=["macos", "arm64"]
            )
        }
        cluster_manager.clusters = cluster_configs

        # 模拟集群信息
        cluster_info = {
            "mac": {
                "metadata": cluster_configs["mac"],
                "snapshot": ResourceSnapshot(
                    cluster_name="mac",
                    available_resources={"CPU": 8.0, "GPU": 0},
                    total_resources={"CPU": 8.0, "GPU": 0},
                    node_count=1,
                    timestamp=time.time()
                )
            }
        }
        cluster_monitor.get_all_cluster_info.return_value = cluster_info
        cluster_monitor.refresh_resource_snapshots.return_value = None

        # 替换实际的TaskLifecycleManager
        with patch('ray_multicluster_scheduler.app.client_api.unified_scheduler.TaskLifecycleManager') as mock_task_lifecycle_manager_cls:
            mock_task_lifecycle_manager_cls.return_value = task_lifecycle_manager

            # 初始化调度器环境
            unified_scheduler = UnifiedScheduler()
            returned_manager = unified_scheduler.initialize_environment()

            # 初始化submit_task模块
            init_task_scheduler(task_lifecycle_manager)

            # 定义测试函数
            def test_function(x, y):
                return x + y

            # 提交任务，应该抛出异常
            with self.assertRaises(Exception) as context:
                submit_task(
                    func=test_function,
                    args=(1, 2),
                    kwargs={},
                    resource_requirements={"CPU": 1.0},
                    tags=["test"],
                    name="test_task",
                    preferred_cluster="mac"
                )

            # 验证异常信息
            self.assertIn("Could not get client for cluster mac", str(context.exception))
            print("✅ 集群连接失败时正确抛出异常")

    def test_lazy_initialization(self):
        """测试惰性初始化功能"""
        print("\n" + "=" * 60)
        print("测试惰性初始化功能")
        print("=" * 60)

        # 创建模拟的集群管理器
        cluster_manager = Mock(spec=ClusterManager)

        # 创建模拟的集群监控器
        cluster_monitor = Mock(spec=ClusterMonitor)
        cluster_monitor.cluster_manager = cluster_manager

        # 创建模拟的TaskLifecycleManager
        task_lifecycle_manager = Mock(spec=TaskLifecycleManager)
        task_lifecycle_manager.submit_task_and_get_future.return_value = "lazy_init_result"

        # 模拟集群配置
        cluster_configs = {
            "mac": ClusterConfig(
                name="mac",
                head_address="192.168.5.2:32546",
                dashboard="http://192.168.5.2:8265",
                prefer=True,
                weight=1.2,
                runtime_env={
                    "conda": "k8s",
                    "env_vars": {
                        "home_dir": "/Users/zorro"
                    }
                },
                tags=["macos", "arm64"]
            )
        }
        cluster_manager.clusters = cluster_configs

        # 模拟集群信息
        cluster_info = {
            "mac": {
                "metadata": cluster_configs["mac"],
                "snapshot": ResourceSnapshot(
                    cluster_name="mac",
                    available_resources={"CPU": 8.0, "GPU": 0},
                    total_resources={"CPU": 8.0, "GPU": 0},
                    node_count=1,
                    timestamp=time.time()
                )
            }
        }
        cluster_monitor.get_all_cluster_info.return_value = cluster_info
        cluster_monitor.refresh_resource_snapshots.return_value = None

        # 定义测试函数
        def test_function(x, y):
            return x + y

        # 在没有显式初始化的情况下直接调用submit_task
        with patch('ray_multicluster_scheduler.app.client_api.unified_scheduler.ClusterMonitor') as mock_cluster_monitor_cls, \
             patch('ray_multicluster_scheduler.app.client_api.unified_scheduler.TaskLifecycleManager') as mock_task_lifecycle_manager_cls:

            mock_cluster_monitor_cls.return_value = cluster_monitor
            mock_task_lifecycle_manager_cls.return_value = task_lifecycle_manager

            # 提交任务，应该触发惰性初始化
            task_id, result = submit_task(
                func=test_function,
                args=(1, 2),
                kwargs={},
                resource_requirements={"CPU": 1.0},
                tags=["test"],
                name="lazy_init_test_task",
                preferred_cluster="mac"
            )

            # 验证结果
            self.assertIsNotNone(task_id)
            self.assertEqual(result, "lazy_init_result")

            # 验证ClusterMonitor和TaskLifecycleManager被创建
            mock_cluster_monitor_cls.assert_called_once()
            mock_task_lifecycle_manager_cls.assert_called_once()

            print("✅ 惰性初始化功能测试通过")

    def test_cluster_client_connection_process(self):
        """测试集群客户端连接过程"""
        print("\n" + "=" * 60)
        print("测试集群客户端连接过程")
        print("=" * 60)

        # 创建模拟的集群管理器
        cluster_manager = Mock(spec=ClusterManager)

        # 创建模拟的集群监控器
        cluster_monitor = Mock(spec=ClusterMonitor)
        cluster_monitor.cluster_manager = cluster_manager

        # 创建真实的TaskLifecycleManager实例用于测试
        task_lifecycle_manager = TaskLifecycleManager(cluster_monitor)

        # 模拟集群配置
        cluster_configs = {
            "mac": ClusterConfig(
                name="mac",
                head_address="192.168.5.2:32546",
                dashboard="http://192.168.5.2:8265",
                prefer=True,
                weight=1.2,
                runtime_env={
                    "conda": "k8s",
                    "env_vars": {
                        "home_dir": "/Users/zorro"
                    }
                },
                tags=["macos", "arm64"]
            )
        }
        cluster_manager.clusters = cluster_configs

        # 模拟集群信息
        cluster_info = {
            "mac": {
                "metadata": cluster_configs["mac"],
                "snapshot": ResourceSnapshot(
                    cluster_name="mac",
                    available_resources={"CPU": 8.0, "GPU": 0},
                    total_resources={"CPU": 8.0, "GPU": 0},
                    node_count=1,
                    timestamp=time.time()
                )
            }
        }
        cluster_monitor.get_all_cluster_info.return_value = cluster_info
        cluster_monitor.refresh_resource_snapshots.return_value = None

        # 初始化submit_task模块
        init_task_scheduler(task_lifecycle_manager)

        # 验证调度器已初始化
        from ray_multicluster_scheduler.app.client_api.submit_task import _task_lifecycle_manager as actual_task_lifecycle_manager
        self.assertIsNotNone(actual_task_lifecycle_manager)

        print("✅ 集群客户端连接过程测试准备完成")


def diagnose_connection_issue():
    """诊断连接问题"""
    print("\n" + "=" * 60)
    print("诊断连接问题")
    print("=" * 60)

    print("\n可能的原因分析:")
    print("1. 集群地址不可达: 192.168.5.2:32546")
    print("2. 集群服务未启动或异常")
    print("3. 网络连接问题")
    print("4. 防火墙或安全组限制")
    print("5. 集群配置错误")
    print("6. 客户端连接池问题")

    print("\n解决方案建议:")
    print("1. 检查集群地址是否正确")
    print("2. 验证集群服务是否正常运行")
    print("3. 检查网络连通性")
    print("4. 检查防火墙设置")
    print("5. 验证集群配置文件")
    print("6. 重启调度器和集群服务")


if __name__ == "__main__":
    # 运行单元测试
    unittest.main(exit=False)

    # 诊断连接问题
    diagnose_connection_issue()

    print("\n" + "=" * 60)
    print("🎉 测试完成!")
    print("=" * 60)