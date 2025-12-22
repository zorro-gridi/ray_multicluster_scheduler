#!/usr/bin/env python3
"""
真实场景下的负载均衡测试
模拟任务调度过程中资源变化的情况
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine


def realistic_load_balancing_test():
    """真实场景下的负载均衡测试"""
    print("=" * 80)
    print("🔍 真实场景下的负载均衡测试")
    print("=" * 80)

    # 创建策略引擎
    policy_engine = PolicyEngine()

    # 模拟集群配置 - centos(16核) 和 mac(8核)
    cluster_configs = {
        "centos": ClusterMetadata(
            name="centos",
            head_address="192.168.5.7:32546",
            dashboard="http://192.168.5.7:31591",
            prefer=False,
            weight=1.0,
            runtime_env={
                "conda": "ts",
                "env_vars": {"home_dir": "/home/zorro"}
            },
            tags=["linux", "x86_64"]
        ),
        "mac": ClusterMetadata(
            name="mac",
            head_address="192.168.5.2:32546",
            dashboard="http://192.168.5.2:8265",
            prefer=True,
            weight=1.2,
            runtime_env={
                "conda": "k8s",
                "env_vars": {"home_dir": "/Users/zorro"}
            },
            tags=["macos", "arm64"]
        )
    }

    # 更新策略引擎的集群元数据
    policy_engine.update_cluster_metadata(cluster_configs)

    # 初始化集群资源快照
    cluster_resources = {
        "centos": {
            "total": {"CPU": 16.0, "GPU": 0},
            "available": {"CPU": 16.0, "GPU": 0}
        },
        "mac": {
            "total": {"CPU": 8.0, "GPU": 0, "MacCPU": 8.0},
            "available": {"CPU": 8.0, "GPU": 0, "MacCPU": 8.0}
        }
    }

    # 统计变量
    cluster_distribution = defaultdict(int)
    queued_tasks = 0
    scheduled_tasks = 0

    # 提交30个任务，每个任务需要2个CPU，不指定集群（使用负载均衡）
    print(f"\n🚀 提交30个任务（每个任务需要2个CPU，不指定集群）:")
    print(f"   • centos集群: 16个CPU")
    print(f"   • mac集群: 8个CPU")
    print(f"   • 总可用CPU: 24个")
    print(f"   • 总需求CPU: 60个 (30个任务 × 2个CPU)")

    for i in range(30):
        # 创建当前的资源快照
        current_time = time.time()
        cluster_snapshots = {}

        for cluster_name, resources in cluster_resources.items():
            cluster_snapshots[cluster_name] = ResourceSnapshot(
                cluster_name=cluster_name,
                total_resources=resources["total"],
                available_resources=resources["available"],
                node_count=1,  # 简化处理
                timestamp=current_time
            )

        task_desc = TaskDescription(
            task_id=f"realistic_lb_task_{i}",
            name=f"realistic_lb_task_{i}",
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
            scheduled_tasks += 1
            print(f"    任务 {i}: 调度到 {decision.cluster_name} - {decision.reason}")

            # 更新集群资源（模拟任务占用资源）
            selected_cluster = decision.cluster_name
            if selected_cluster in cluster_resources:
                # 减少可用资源
                cpu_type = "MacCPU" if selected_cluster == "mac" else "CPU"
                current_available = cluster_resources[selected_cluster]["available"].get(cpu_type, 0)
                new_available = max(0, current_available - 2.0)
                cluster_resources[selected_cluster]["available"][cpu_type] = new_available

                # 同时更新标准CPU资源，保持一致性
                if cpu_type == "MacCPU":
                    cluster_resources[selected_cluster]["available"]["CPU"] = new_available
        else:
            queued_tasks += 1
            print(f"    任务 {i}: 进入队列等待")

    # 生成测试报告
    print(f"\n📊 测试结果统计:")
    generate_realistic_lb_test_report(cluster_distribution, queued_tasks, scheduled_tasks, cluster_resources)

    return cluster_distribution, queued_tasks, scheduled_tasks


def generate_realistic_lb_test_report(cluster_distribution, queued_tasks, scheduled_tasks, cluster_resources):
    """生成真实场景负载均衡测试报告"""
    print(f"\n📋 集群分布统计:")

    for cluster_name, count in cluster_distribution.items():
        resources = cluster_resources.get(cluster_name, {})
        available = resources.get("available", {})
        total = resources.get("total", {})

        cpu_available = available.get("CPU", 0)
        cpu_total = total.get("CPU", 0)

        # 对于MAC集群，检查MacCPU资源
        if cluster_name == "mac":
            mac_cpu_available = available.get("MacCPU", 0)
            mac_cpu_total = total.get("MacCPU", 0)
            if mac_cpu_total > cpu_total:
                cpu_available = mac_cpu_available
                cpu_total = mac_cpu_total

        print(f"  • {cluster_name}: {count}个任务 (总CPU: {cpu_total}, 可用CPU: {cpu_available:.1f})")

    print(f"\n📋 调度统计:")
    print(f"  • 成功调度任务: {scheduled_tasks}个")
    print(f"  • 进入队列任务: {queued_tasks}个")
    print(f"  • 总任务数: {scheduled_tasks + queued_tasks}个")

    # 分析调度行为
    print(f"\n📋 调度行为分析:")
    if len(cluster_distribution) > 1:
        print(f"  ✅ 系统能够将任务分散到多个集群进行调度")
        print(f"     • 不同集群都有任务被调度")
        print(f"     • 实现了跨集群负载均衡")

        # 计算负载均衡程度
        counts = list(cluster_distribution.values())
        max_count = max(counts)
        min_count = min(counts)
        balance_ratio = min_count / max_count if max_count > 0 else 0

        print(f"     • 负载均衡比率: {balance_ratio:.2f} (越接近1越均衡)")
    else:
        print(f"  ⚠️  系统可能只在一个集群上进行调度")
        print(f"     • 只有一个集群有任务被调度")
        print(f"     • 可能未充分利用所有可用集群")

    # 分析资源利用情况
    print(f"\n📋 资源利用分析:")
    total_initial_capacity = 16.0 + 8.0  # centos(16) + mac(8)
    total_required = (scheduled_tasks + queued_tasks) * 2  # 每个任务需要2个CPU
    total_consumed = scheduled_tasks * 2
    utilization_rate = total_consumed / total_initial_capacity if total_initial_capacity > 0 else 0

    print(f"  • 初始总集群容量: {total_initial_capacity}个CPU")
    print(f"  • 总任务需求: {total_required}个CPU")
    print(f"  • 实际消耗资源: {total_consumed}个CPU")
    print(f"  • 资源利用率: {utilization_rate:.1%}")
    print(f"  • 排队任务数: {queued_tasks}个")


def main():
    # 运行真实场景下的负载均衡测试
    cluster_dist, queued, scheduled = realistic_load_balancing_test()

    print("\n" + "=" * 80)
    print("🏁 测试总结")
    print("=" * 80)

    if len(cluster_dist) > 1:
        print(f"✅ 改进后的系统能够实现跨集群负载均衡")
        print(f"   • 任务被分散到多个集群执行")
        print(f"   • 充分利用了所有可用集群的资源")

        # 计算负载均衡程度
        counts = list(cluster_dist.values())
        max_count = max(counts)
        min_count = min(counts)
        balance_ratio = min_count / max_count if max_count > 0 else 0

        print(f"   • 负载均衡比率: {balance_ratio:.2f}")
    else:
        print(f"⚠️  系统仍存在调度局限性")
        print(f"   • 任务主要集中在单个集群执行")
        print(f"   • 需要进一步优化")

    print(f"\n📈 最终调度统计:")
    print(f"   • 成功调度任务: {scheduled}个")
    print(f"   • 进入队列任务: {queued}个")
    for cluster, count in cluster_dist.items():
        print(f"   • {cluster}集群: {count}个任务")


if __name__ == "__main__":
    main()