#!/usr/bin/env python3
"""
测试MAC集群CPU资源处理
验证修复后的CPU资源计算是否正确处理MAC集群的特殊资源
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager, ClusterConfig


def test_mac_cpu_resource_calculation():
    """测试MAC集群CPU资源计算"""
    print("=" * 60)
    print("测试MAC集群CPU资源计算")
    print("=" * 60)

    # 创建ClusterManager实例
    cluster_manager = ClusterManager()

    # 创建MAC集群配置
    mac_config = ClusterConfig(
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

    cluster_manager.add_cluster(mac_config)

    # 模拟不同的资源情况
    test_cases = [
        {
            "name": "标准资源情况",
            "avail_resources": {"CPU": 8.0, "MacCPU": 6.0},
            "total_resources": {"CPU": 8.0, "MacCPU": 6.0},
            "expected_cpu_free": 8.0,
            "expected_cpu_total": 8.0
        },
        {
            "name": "MAC特殊资源更大情况",
            "avail_resources": {"CPU": 5.0, "MacCPU": 6.0},
            "total_resources": {"CPU": 5.0, "MacCPU": 6.0},
            "expected_cpu_free": 6.0,
            "expected_cpu_total": 6.0
        },
        {
            "name": "只有标准CPU资源",
            "avail_resources": {"CPU": 8.0},
            "total_resources": {"CPU": 8.0},
            "expected_cpu_free": 8.0,
            "expected_cpu_total": 8.0
        },
        {
            "name": "只有MacCPU资源",
            "avail_resources": {"MacCPU": 6.0},
            "total_resources": {"MacCPU": 6.0},
            "expected_cpu_free": 6.0,
            "expected_cpu_total": 6.0
        }
    ]

    for case in test_cases:
        print(f"\n测试案例: {case['name']}")
        print(f"  可用资源: {case['avail_resources']}")
        print(f"  总资源: {case['total_resources']}")

        # 调用_calculate_cpu_resources方法
        cpu_free, cpu_total = cluster_manager._calculate_cpu_resources(
            case['avail_resources'],
            case['total_resources'],
            mac_config
        )

        print(f"  计算结果: 可用CPU={cpu_free}, 总CPU={cpu_total}")
        print(f"  期望结果: 可用CPU={case['expected_cpu_free']}, 总CPU={case['expected_cpu_total']}")

        # 验证结果
        assert cpu_free == case['expected_cpu_free'], f"可用CPU计算错误: 期望 {case['expected_cpu_free']}, 实际 {cpu_free}"
        assert cpu_total == case['expected_cpu_total'], f"总CPU计算错误: 期望 {case['expected_cpu_total']}, 实际 {cpu_total}"

        print("  ✅ 测试通过")


def test_non_mac_cluster_resources():
    """测试非MAC集群资源计算"""
    print("\n" + "=" * 60)
    print("测试非MAC集群资源计算")
    print("=" * 60)

    # 创建ClusterManager实例
    cluster_manager = ClusterManager()

    # 创建非MAC集群配置
    centos_config = ClusterConfig(
        name="centos",
        head_address="192.168.5.7:32546",
        dashboard="http://192.168.5.7:31591",
        prefer=False,
        weight=1.0,
        runtime_env={
            "conda": "ts",
            "env_vars": {
                "home_dir": "/home/zorro"
            }
        },
        tags=["linux", "x86_64"]
    )

    cluster_manager.add_cluster(centos_config)

    # 测试非MAC集群的资源计算
    test_cases = [
        {
            "name": "标准Linux集群",
            "avail_resources": {"CPU": 5.0},
            "total_resources": {"CPU": 5.0},
            "expected_cpu_free": 5.0,
            "expected_cpu_total": 5.0
        },
        {
            "name": "Linux集群有额外资源但不使用",
            "avail_resources": {"CPU": 5.0, "MacCPU": 6.0},
            "total_resources": {"CPU": 5.0, "MacCPU": 6.0},
            "expected_cpu_free": 5.0,
            "expected_cpu_total": 5.0
        }
    ]

    for case in test_cases:
        print(f"\n测试案例: {case['name']}")
        print(f"  可用资源: {case['avail_resources']}")
        print(f"  总资源: {case['total_resources']}")

        # 调用_calculate_cpu_resources方法
        cpu_free, cpu_total = cluster_manager._calculate_cpu_resources(
            case['avail_resources'],
            case['total_resources'],
            centos_config
        )

        print(f"  计算结果: 可用CPU={cpu_free}, 总CPU={cpu_total}")
        print(f"  期望结果: 可用CPU={case['expected_cpu_free']}, 总CPU={case['expected_cpu_total']}")

        # 验证结果
        assert cpu_free == case['expected_cpu_free'], f"可用CPU计算错误: 期望 {case['expected_cpu_free']}, 实际 {cpu_free}"
        assert cpu_total == case['expected_cpu_total'], f"总CPU计算错误: 期望 {case['expected_cpu_total']}, 实际 {cpu_total}"

        print("  ✅ 测试通过")


def demonstrate_fix_effect():
    """演示修复效果"""
    print("\n" + "=" * 60)
    print("演示修复效果")
    print("=" * 60)

    print("\n修复前的问题:")
    print("1. MAC集群有CPU=8.0和MacCPU=6.0两种资源")
    print("2. 调度系统只使用CPU资源进行计算")
    print("3. 当CPU资源显示为0但MacCPU资源不为0时，计算结果不准确")
    print("4. Dashboard显示100%使用率，但调度系统计算为0%")

    print("\n修复后的解决方案:")
    print("1. 增加了_calculate_cpu_resources方法专门处理特殊资源")
    print("2. 对于MAC集群，智能选择合适的CPU资源类型")
    print("3. 当MacCPU资源比标准CPU资源更大时，使用MacCPU资源")
    print("4. 非MAC集群不受影响，继续使用标准CPU资源")

    print("\n预期效果:")
    print("✅ MAC集群的资源计算将更加准确")
    print("✅ Dashboard和调度系统的资源使用率将保持一致")
    print("✅ 资源阈值检查将基于准确的资源使用率进行")


if __name__ == "__main__":
    # 测试MAC集群CPU资源计算
    test_mac_cpu_resource_calculation()

    # 测试非MAC集群资源计算
    test_non_mac_cluster_resources()

    # 演示修复效果
    demonstrate_fix_effect()

    print("\n" + "=" * 60)
    print("🎉 所有测试通过，修复完成!")
    print("=" * 60)