#!/usr/bin/env python3
"""
真实集群连接负载均衡策略验证测试
专注于验证策略决策而不执行实际任务
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    UnifiedScheduler,
    initialize_scheduler_environment
)
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.common.model import TaskDescription


def test_load_balancing_strategy_with_real_clusters():
    """测试使用真实集群连接的负载均衡策略"""
    print("=" * 80)
    print("🔍 真实集群连接负载均衡策略验证测试")
    print("=" * 80)

    try:
        # 1. 初始化调度环境
        print("🔧 初始化调度环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度环境初始化完成")

        # 2. 获取集群监视器
        cluster_monitor = task_lifecycle_manager.cluster_monitor

        # 3. 刷新集群状态
        print("🔄 刷新集群状态...")
        cluster_monitor.refresh_resource_snapshots(force=True)
        print("✅ 集群状态刷新完成")

        # 4. 获取集群信息
        print("📋 获取集群信息...")
        cluster_info = cluster_monitor.get_all_cluster_info()

        print(f"\n📊 集群状态信息:")
        cluster_snapshots = {}
        cluster_metadata = {}

        for cluster_name, info in cluster_info.items():
            if info and 'snapshot' in info and info['snapshot']:
                snapshot = info['snapshot']
                metadata = info['metadata']
                cluster_snapshots[cluster_name] = snapshot
                cluster_metadata[cluster_name] = metadata

                cpu_available = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                gpu_available = snapshot.available_resources.get("GPU", 0)
                gpu_total = snapshot.total_resources.get("GPU", 0)

                print(f"  • {cluster_name}: CPU={cpu_available}/{cpu_total}, GPU={gpu_available}/{gpu_total}")
            else:
                print(f"  • {cluster_name}: 无法获取资源信息")

        # 5. 创建策略引擎
        print("🔧 创建策略引擎...")
        policy_engine = PolicyEngine(cluster_monitor)
        policy_engine.update_cluster_metadata(cluster_metadata)
        print("✅ 策略引擎创建完成")

        # 6. 模拟任务调度决策
        print(f"\n🚀 模拟任务调度决策...")
        cluster_distribution = defaultdict(int)

        # 提交20个任务，每个任务需要2个CPU，不指定集群
        for i in range(20):
            task_desc = TaskDescription(
                task_id=f"lb_test_task_{i}",
                name=f"负载均衡测试任务{i}",
                func_or_class=lambda: None,
                args=(),
                kwargs={},
                resource_requirements={"CPU": 2.0},
                tags=["test", "load_balance"],
                preferred_cluster=None  # 不指定集群，使用负载均衡
            )

            # 让策略引擎做调度决策
            decision = policy_engine.schedule(task_desc, cluster_snapshots)

            if decision and decision.cluster_name:
                cluster_distribution[decision.cluster_name] += 1
                print(f"    任务 {i}: 调度到 {decision.cluster_name} - {decision.reason}")
            else:
                print(f"    任务 {i}: 无法调度")

        # 7. 生成测试报告
        print(f"\n📊 调度决策统计:")
        total_scheduled = sum(cluster_distribution.values())
        for cluster_name, count in cluster_distribution.items():
            percentage = (count / total_scheduled * 100) if total_scheduled > 0 else 0
            print(f"  • {cluster_name}: {count}个任务 ({percentage:.1f}%)")

        # 8. 分析负载均衡效果
        print(f"\n📋 负载均衡效果分析:")
        if len(cluster_distribution) > 1:
            counts = list(cluster_distribution.values())
            max_count = max(counts)
            min_count = min(counts)
            balance_ratio = min_count / max_count if max_count > 0 else 0

            print(f"  ✅ 实现了跨集群负载均衡")
            print(f"     • 不同集群都有任务被调度")
            print(f"     • 负载均衡比率: {balance_ratio:.2f} (越接近1越均衡)")
        else:
            print(f"  ⚠️  任务主要在单个集群调度")
            print(f"     • 未充分利用多集群资源")

        return cluster_distribution

    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    # 运行真实集群连接负载均衡策略测试
    cluster_dist = test_load_balancing_strategy_with_real_clusters()

    if cluster_dist:
        print("\n" + "=" * 80)
        print("🏁 测试总结")
        print("=" * 80)

        if len(cluster_dist) > 1:
            print(f"✅ 负载均衡策略验证成功")
            print(f"   • 任务被分散到多个集群")
            print(f"   • 实现了跨集群负载均衡")
        else:
            print(f"⚠️  负载均衡策略有待改进")
            print(f"   • 任务主要在单个集群调度")
            print(f"   • 未充分利用多集群资源")

        print(f"\n📈 最终统计:")
        total_scheduled = sum(cluster_dist.values())
        for cluster, count in cluster_dist.items():
            percentage = (count / total_scheduled * 100) if total_scheduled > 0 else 0
            print(f"   • {cluster}: {count}个任务 ({percentage:.1f}%)")
    else:
        print("\n" + "=" * 80)
        print("❌ 测试失败")
        print("=" * 80)
        print(f"   • 请检查集群连接状态")
        print(f"   • 确认集群配置正确")


if __name__ == "__main__":
    main()