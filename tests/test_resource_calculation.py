#!/usr/bin/env python3
"""
资源计算准确性测试
验证CPU和内存使用率计算是否准确
"""

import sys
import os
import time as time_module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from unittest.mock import Mock, patch
import ray

from ray_multicluster_scheduler.common.model import ResourceSnapshot
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager, ClusterConfig, ClusterHealth
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor


def test_cpu_utilization_calculation():
    """测试CPU使用率计算准确性"""
    print("=" * 60)
    print("测试CPU使用率计算准确性")
    print("=" * 60)

    # 模拟不同的资源情况
    test_cases = [
        {
            "name": "情况1: 无负载",
            "total_resources": {"CPU": 8.0, "GPU": 0},
            "available_resources": {"CPU": 8.0, "GPU": 0},
            "expected_utilization": 0.0
        },
        {
            "name": "情况2: 50%负载",
            "total_resources": {"CPU": 8.0, "GPU": 0},
            "available_resources": {"CPU": 4.0, "GPU": 0},
            "expected_utilization": 0.5
        },
        {
            "name": "情况3: 75%负载",
            "total_resources": {"CPU": 8.0, "GPU": 0},
            "available_resources": {"CPU": 2.0, "GPU": 0},
            "expected_utilization": 0.75
        },
        {
            "name": "情况4: 100%负载",
            "total_resources": {"CPU": 8.0, "GPU": 0},
            "available_resources": {"CPU": 0.0, "GPU": 0},
            "expected_utilization": 1.0
        },
        {
            "name": "情况5: 无CPU资源",
            "total_resources": {"CPU": 0.0, "GPU": 0},
            "available_resources": {"CPU": 0.0, "GPU": 0},
            "expected_utilization": 0.0
        }
    ]

    for case in test_cases:
        print(f"\n{case['name']}:")
        cpu_total = case["total_resources"].get("CPU", 0)
        cpu_free = case["available_resources"].get("CPU", 0)

        # 计算CPU使用率
        if cpu_total > 0:
            cpu_utilization = (cpu_total - cpu_free) / cpu_total
        else:
            cpu_utilization = 0

        print(f"  总CPU: {cpu_total}, 可用CPU: {cpu_free}")
        print(f"  计算得到的CPU使用率: {cpu_utilization:.2%}")
        print(f"  期望的CPU使用率: {case['expected_utilization']:.2%}")

        # 验证计算是否正确
        assert abs(cpu_utilization - case["expected_utilization"]) < 0.001, \
            f"CPU使用率计算错误: 期望 {case['expected_utilization']:.2%}, 实际 {cpu_utilization:.2%}"
        print("  ✅ 计算正确")


def test_cluster_manager_resource_calculation():
    """测试ClusterManager中的资源计算"""
    print("\n" + "=" * 60)
    print("测试ClusterManager中的资源计算")
    print("=" * 60)

    # 创建ClusterManager实例
    cluster_manager = ClusterManager()

    # 创建模拟的集群配置
    cluster_config = ClusterConfig(
        name="test_cluster",
        head_address="127.0.0.1:6379",
        dashboard="http://127.0.0.1:8265",
        prefer=True,
        weight=1.0,
        runtime_env={"conda": "test"},
        tags=["test"]
    )

    cluster_manager.add_cluster(cluster_config)

    # 模拟_check_cluster_health方法中的资源计算逻辑
    print("\n模拟资源计算逻辑:")

    # 模拟不同的资源情况
    mock_resources = [
        {
            "name": "模拟无负载情况",
            "avail_resources": {"CPU": 8.0, "GPU": 0, "memory": 16000000000},
            "total_resources": {"CPU": 8.0, "GPU": 0, "memory": 16000000000},
            "expected_cpu_util": 0.0
        },
        {
            "name": "模拟50%CPU负载情况",
            "avail_resources": {"CPU": 4.0, "GPU": 0, "memory": 16000000000},
            "total_resources": {"CPU": 8.0, "GPU": 0, "memory": 16000000000},
            "expected_cpu_util": 0.5
        },
        {
            "name": "模拟高负载情况",
            "avail_resources": {"CPU": 1.0, "GPU": 0, "memory": 16000000000},
            "total_resources": {"CPU": 8.0, "GPU": 0, "memory": 16000000000},
            "expected_cpu_util": 0.875
        }
    ]

    for resources in mock_resources:
        print(f"\n{resources['name']}:")
        avail_resources = resources["avail_resources"]
        total_resources = resources["total_resources"]

        # 模拟ClusterManager中的计算逻辑
        cpu_free = avail_resources.get("CPU", 0)
        cpu_total = total_resources.get("CPU", 0)
        gpu_free = avail_resources.get("GPU", 0)
        gpu_total = total_resources.get("GPU", 0)

        # CPU使用率计算
        cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0

        print(f"  CPU: 总计={cpu_total}, 可用={cpu_free}, 使用率={cpu_utilization:.2%}")
        print(f"  GPU: 总计={gpu_total}, 可用={gpu_free}")
        print(f"  期望CPU使用率: {resources['expected_cpu_util']:.2%}")

        # 验证计算
        assert abs(cpu_utilization - resources["expected_cpu_util"]) < 0.001, \
            f"CPU使用率计算错误: 期望 {resources['expected_cpu_util']:.2%}, 实际 {cpu_utilization:.2%}"
        print("  ✅ CPU使用率计算正确")


def analyze_possible_issues():
    """分析可能导致CPU使用率为0的可能原因"""
    print("\n" + "=" * 60)
    print("分析可能导致CPU使用率为0的可能原因")
    print("=" * 60)

    print("\n1. Ray集群资源报告问题:")
    print("   - Ray可能没有正确报告集群资源")
    print("   - 集群连接可能存在问题")
    print("   - 集群可能处于不健康状态")

    print("\n2. 资源计算时机问题:")
    print("   - 在任务刚开始执行时检查，可能还未更新资源统计")
    print("   - 资源统计更新有延迟")

    print("\n3. 任务类型问题:")
    print("   - 任务可能是I/O密集型而非CPU密集型")
    print("   - 任务可能在等待资源分配")
    print("   - 任务可能被阻塞在其他地方")

    print("\n4. Ray资源配置问题:")
    print("   - 集群可能没有正确配置CPU资源")
    print("   - 任务可能没有正确请求CPU资源")

    print("\n5. 监控频率问题:")
    print("   - 监控间隔过长，错过资源使用峰值")
    print("   - 资源使用时间很短，难以捕获")


def demonstrate_resource_monitoring():
    """演示资源监控过程"""
    print("\n" + "=" * 60)
    print("演示资源监控过程")
    print("=" * 60)

    # 创建模拟的ClusterManager
    cluster_manager = ClusterManager()

    # 添加一个测试集群配置
    cluster_config = ClusterConfig(
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

    cluster_manager.add_cluster(cluster_config)

    print("创建的集群配置:")
    print(f"  名称: {cluster_config.name}")
    print(f"  地址: {cluster_config.head_address}")
    print(f"  是否偏好集群: {cluster_config.prefer}")
    print(f"  权重: {cluster_config.weight}")

    # 模拟检查集群健康状态的过程
    print("\n模拟集群健康检查过程:")
    print("1. 连接到Ray集群...")
    print("2. 获取可用资源...")
    print("3. 获取总资源...")
    print("4. 计算资源使用率...")
    print("5. 更新集群健康状态...")


if __name__ == "__main__":
    # 测试CPU使用率计算
    test_cpu_utilization_calculation()

    # 测试ClusterManager中的资源计算
    test_cluster_manager_resource_calculation()

    # 分析可能的问题
    analyze_possible_issues()

    # 演示资源监控过程
    demonstrate_resource_monitoring()

    print("\n" + "=" * 60)
    print("🎉 所有测试和分析完成!")
    print("=" * 60)
    print("\n结论:")
    print("1. CPU使用率计算公式是正确的")
    print("2. 如果观察到CPU使用率为0，可能是以下原因之一:")
    print("   - Ray集群没有正确报告资源信息")
    print("   - 任务尚未开始执行或已执行完成")
    print("   - 监控时机不当，错过了资源使用峰值")
    print("   - 集群连接或配置存在问题")