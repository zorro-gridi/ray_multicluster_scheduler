#!/usr/bin/env python3
"""
修复版多集群并发调度测试用例
使用更准确的评分机制来验证跨集群负载均衡
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager


def enhanced_multi_cluster_concurrency_test():
    """增强版多集群并发调度测试"""
    print("=" * 80)
    print("🔍 增强版多集群并发调度测试")
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

    # 手动计算每个集群的评分以验证调度逻辑
    print(f"\n📊 集群评分计算:")
    manual_scores = calculate_cluster_scores(cluster_configs, cluster_snapshots)
    for cluster_name, score_details in manual_scores.items():
        print(f"  • {cluster_name}: {score_details['final_score']:.2f}")
        print(f"    - 基础评分: {score_details['base_score']:.2f} (可用CPU × 权重)")
        print(f"    - GPU加成: {score_details['gpu_bonus']:.2f}")
        print(f"    - 偏好加成: {score_details['preference_bonus']:.2f}")
        print(f"    - 负载因子: {score_details['load_factor']:.2f}")

    for i in range(30):
        task_desc = TaskDescription(
            task_id=f"multi_cluster_task_{i}",
            name=f"multi_cluster_task_{i}",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 2.0},
            tags=["test", "multi_cluster"],
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
    generate_enhanced_test_report(cluster_distribution, queued_tasks, cluster_snapshots, manual_scores)

    return cluster_distribution, queued_tasks


def calculate_cluster_scores(cluster_configs, cluster_snapshots):
    """手动计算集群评分以验证调度逻辑"""
    scores = {}

    for cluster_name, snapshot in cluster_snapshots.items():
        config = cluster_configs[cluster_name]

        # 处理MAC集群的特殊CPU资源
        cpu_free = snapshot.available_resources.get("CPU", 0)
        cpu_total = snapshot.total_resources.get("CPU", 0)

        if "mac" in cluster_name.lower():
            mac_cpu_free = snapshot.available_resources.get("MacCPU", 0)
            mac_cpu_total = snapshot.total_resources.get("MacCPU", 0)
            if mac_cpu_total > cpu_total:
                cpu_free = mac_cpu_free
                cpu_total = mac_cpu_total

        gpu_free = snapshot.available_resources.get("GPU", 0)
        gpu_total = snapshot.total_resources.get("GPU", 0)

        # 计算评分
        base_score = cpu_free * config.weight
        gpu_bonus = gpu_free * 5  # GPU资源更宝贵
        preference_bonus = 1.2 if config.prefer else 1.0
        cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
        load_factor = 1.0 - cpu_utilization  # 负载越低因子越高

        final_score = (base_score + gpu_bonus) * preference_bonus * load_factor

        scores[cluster_name] = {
            'final_score': final_score,
            'base_score': base_score,
            'gpu_bonus': gpu_bonus,
            'preference_bonus': preference_bonus,
            'load_factor': load_factor,
            'cpu_free': cpu_free,
            'cpu_total': cpu_total
        }

    return scores


def generate_enhanced_test_report(cluster_distribution, queued_tasks, cluster_snapshots, manual_scores):
    """生成增强版测试报告"""
    print(f"\n📋 集群分布统计:")
    total_scheduled = sum(cluster_distribution.values())

    for cluster_name, count in cluster_distribution.items():
        # 获取集群信息
        snapshot = cluster_snapshots.get(cluster_name)
        score_info = manual_scores.get(cluster_name, {})
        if snapshot and score_info:
            cpu_total = score_info.get('cpu_total', 0)
            cpu_free = score_info.get('cpu_free', 0)

            print(f"  • {cluster_name}: {count}个任务 (总CPU: {cpu_total}, 可用CPU: {cpu_free}, 评分: {score_info.get('final_score', 0):.2f})")
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
    else:
        print(f"  ⚠️  系统可能只在一个集群上进行调度")
        print(f"     • 只有一个集群有任务被调度")
        print(f"     • 可能未充分利用所有可用集群")

    # 分析资源利用情况
    print(f"\n📋 资源利用分析:")
    total_capacity = sum([info.get('cpu_total', 0) for info in manual_scores.values()])
    total_required = (total_scheduled + queued_tasks) * 2  # 每个任务需要2个CPU
    utilization_rate = (total_scheduled * 2) / total_capacity if total_capacity > 0 else 0

    print(f"  • 总集群容量: {total_capacity}个CPU")
    print(f"  • 总任务需求: {total_required}个CPU")
    print(f"  • 实际调度任务: {total_scheduled}个 (消耗{total_scheduled * 2}个CPU)")
    print(f"  • 资源利用率: {utilization_rate:.1%}")
    print(f"  • 排队任务数: {queued_tasks}个")


def analyze_current_implementation_issues():
    """分析当前实现的问题"""
    print("\n" + "=" * 80)
    print("⚠️  当前实现问题分析")
    print("=" * 80)

    print(f"\n📋 评分策略存在的问题:")
    print(f"  1. 评分策略过于简化:")
    print(f"     • 使用固定值进行归一化(32 CPU, 128GB内存)")
    print(f"     • 未考虑集群的实际权重和偏好设置")
    print(f"     • 未正确处理MAC集群的特殊CPU资源")

    print(f"\n  2. 负载均衡算法缺陷:")
    print(f"     • 评分策略未使用集群管理器的复杂评分机制")
    print(f"     • 未考虑资源利用率对评分的影响")
    print(f"     • 未实现真正的负载均衡，只是简单的资源比较")

    print(f"\n  3. 调度决策流程问题:")
    print(f"     • 策略引擎未充分利用集群管理器的评分能力")
    print(f"     • 任务调度缺乏动态调整机制")
    print(f"     • 未实现跨集群的任务迁移功能")


def proposed_improvements():
    """提出改进建议"""
    print("\n" + "=" * 80)
    print("💡 改进建议")
    print("=" * 80)

    print(f"\n📋 评分策略改进建议:")
    print(f"  1. 动态归一化:")
    print(f"     • 根据集群实际资源配置进行归一化")
    print(f"     • 考虑集群的历史负载情况")

    print(f"\n  2. 增强评分机制:")
    print(f"     • 正确处理MAC集群的特殊CPU资源")
    print(f"     • 引入更复杂的评分因素（如网络延迟、任务类型等）")
    print(f"     • 实现基于机器学习的智能评分")

    print(f"\n  3. 负载均衡优化:")
    print(f"     • 实现动态负载均衡算法")
    print(f"     • 引入任务迁移机制")
    print(f"     • 考虑集群间的资源互补性")


def main():
    # 运行增强版多集群并发调度测试
    cluster_dist, queued = enhanced_multi_cluster_concurrency_test()

    # 分析当前实现问题
    analyze_current_implementation_issues()

    # 提出改进建议
    proposed_improvements()

    print("\n" + "=" * 80)
    print("🏁 测试总结")
    print("=" * 80)

    total_scheduled = sum(cluster_dist.values())
    if len(cluster_dist) > 1:
        print(f"✅ 测试结果表明系统能够实现跨集群负载均衡")
        print(f"   • 任务被分散到多个集群执行")
        print(f"   • 充分利用了所有可用集群的资源")
    else:
        print(f"⚠️  测试结果表明系统存在调度局限性")
        print(f"   • 任务主要集中在单个集群执行")
        print(f"   • 未充分利用所有可用集群")
        print(f"   • 评分策略需要改进以实现真正的负载均衡")

    print(f"\n📈 调度统计:")
    print(f"   • 立即调度任务: {total_scheduled}个")
    print(f"   • 进入队列任务: {queued}个")


if __name__ == "__main__":
    main()