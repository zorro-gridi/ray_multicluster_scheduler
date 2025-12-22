#!/usr/bin/env python3
"""
改进版负载均衡策略测试用例
验证优化后的跨集群负载均衡能力
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine


def test_improved_load_balancing():
    """测试改进后的负载均衡策略"""
    print("=" * 80)
    print("🔍 改进版负载均衡策略测试")
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

    # 模拟集群资源快照 - centos有16个CPU，mac有8个CPU
    current_time = time.time()
    cluster_snapshots = {
        "centos": ResourceSnapshot(
            cluster_name="centos",
            total_resources={"CPU": 16.0, "GPU": 0},
            available_resources={"CPU": 16.0, "GPU": 0},
            node_count=2,
            timestamp=current_time
        ),
        "mac": ResourceSnapshot(
            cluster_name="mac",
            total_resources={"CPU": 8.0, "GPU": 0, "MacCPU": 8.0},
            available_resources={"CPU": 8.0, "GPU": 0, "MacCPU": 8.0},
            node_count=1,
            timestamp=current_time
        )
    }

    # 统计变量
    cluster_distribution = defaultdict(int)
    queued_tasks = 0

    # 提交30个任务，每个任务需要2个CPU，不指定集群（使用负载均衡）
    # 总共需要60个CPU，但两个集群总共只有24个CPU
    print(f"\n🚀 提交30个任务（每个任务需要2个CPU，不指定集群）:")
    print(f"   • centos集群: 16个CPU")
    print(f"   • mac集群: 8个CPU")
    print(f"   • 总可用CPU: 24个")
    print(f"   • 总需求CPU: 60个 (30个任务 × 2个CPU)")
    print(f"   • 应该有12个任务立即调度，18个任务排队")

    for i in range(30):
        task_desc = TaskDescription(
            task_id=f"improved_lb_task_{i}",
            name=f"improved_lb_task_{i}",
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
            queued_tasks += 1
            print(f"    任务 {i}: 进入队列等待")

    # 生成测试报告
    print(f"\n📊 测试结果统计:")
    generate_improved_lb_test_report(cluster_distribution, queued_tasks, cluster_snapshots, cluster_configs)

    return cluster_distribution, queued_tasks


def generate_improved_lb_test_report(cluster_distribution, queued_tasks, cluster_snapshots, cluster_configs):
    """生成改进版负载均衡测试报告"""
    print(f"\n📋 集群分布统计:")
    total_scheduled = sum(cluster_distribution.values())

    for cluster_name, count in cluster_distribution.items():
        # 获取集群信息
        snapshot = cluster_snapshots.get(cluster_name)
        config = cluster_configs.get(cluster_name)
        if snapshot and config:
            cpu_total = snapshot.total_resources.get("CPU", 0)
            cpu_available = snapshot.available_resources.get("CPU", 0)

            # 对于MAC集群，检查MacCPU资源
            if "mac" in cluster_name.lower():
                mac_cpu_total = snapshot.total_resources.get("MacCPU", 0)
                mac_cpu_available = snapshot.available_resources.get("MacCPU", 0)
                if mac_cpu_total > cpu_total:
                    cpu_total = mac_cpu_total
                    cpu_available = mac_cpu_available

            print(f"  • {cluster_name}: {count}个任务 (总CPU: {cpu_total}, 可用CPU: {cpu_available}, 权重: {config.weight})")
        else:
            print(f"  • {cluster_name}: {count}个任务")

    print(f"\n📋 队列统计:")
    print(f"  • 立即调度任务: {total_scheduled}个")
    print(f"  • 进入队列任务: {queued_tasks}个")
    print(f"  • 总任务数: {total_scheduled + queued_tasks}个")

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
    total_capacity = 0
    for cluster_name, snapshot in cluster_snapshots.items():
        config = cluster_configs.get(cluster_name)
        cpu_total = snapshot.total_resources.get("CPU", 0)
        # 对于MAC集群，检查MacCPU资源
        if config and "mac" in cluster_name.lower():
            mac_cpu_total = snapshot.total_resources.get("MacCPU", 0)
            if mac_cpu_total > cpu_total:
                cpu_total = mac_cpu_total
        total_capacity += cpu_total

    total_required = (total_scheduled + queued_tasks) * 2  # 每个任务需要2个CPU
    utilization_rate = (total_scheduled * 2) / total_capacity if total_capacity > 0 else 0

    print(f"  • 总集群容量: {total_capacity}个CPU")
    print(f"  • 总任务需求: {total_required}个CPU")
    print(f"  • 实际调度任务: {total_scheduled}个 (消耗{total_scheduled * 2}个CPU)")
    print(f"  • 资源利用率: {utilization_rate:.1%}")
    print(f"  • 排队任务数: {queued_tasks}个")


def compare_before_after():
    """比较改进前后的效果"""
    print("\n" + "=" * 80)
    print("🔄 改进前后效果对比")
    print("=" * 80)

    print(f"\n📋 改进前的问题:")
    print(f"  1. 评分策略过于简化:")
    print(f"     • 使用固定值进行归一化导致评分失真")
    print(f"     • 未考虑集群权重、偏好和真实负载")
    print(f"     • 未正确处理MAC集群的特殊CPU资源")

    print(f"\n  2. 负载均衡效果差:")
    print(f"     • 所有任务都被调度到同一个集群")
    print(f"     • 未充分利用多集群资源")
    print(f"     • 无法实现真正的负载均衡")

    print(f"\n📋 改进后的优势:")
    print(f"  1. 增强版评分策略:")
    print(f"     • 基于实际资源配置进行评分")
    print(f"     • 考虑集群权重、偏好和负载因子")
    print(f"     • 正确处理MAC集群的特殊CPU资源")

    print(f"\n  2. 更好的负载均衡:")
    print(f"     • 任务能够分散到多个集群")
    print(f"     • 充分利用所有可用集群资源")
    print(f"     • 实现更合理的任务分配")


def main():
    # 运行改进版负载均衡测试
    cluster_dist, queued = test_improved_load_balancing()

    # 比较改进前后效果
    compare_before_after()

    print("\n" + "=" * 80)
    print("🏁 测试总结")
    print("=" * 80)

    total_scheduled = sum(cluster_dist.values())
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
        print(f"⚠️  改进后的系统仍存在调度局限性")
        print(f"   • 任务主要集中在单个集群执行")
        print(f"   • 需要进一步优化")

    print(f"\n📈 调度统计:")
    print(f"   • 立即调度任务: {total_scheduled}个")
    print(f"   • 进入队列任务: {queued}个")
    for cluster, count in cluster_dist.items():
        print(f"   • {cluster}集群: {count}个任务")


if __name__ == "__main__":
    main()