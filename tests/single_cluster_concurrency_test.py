#!/usr/bin/env python3
"""
单集群并发调度问题测试用例
验证当提交的并发任务数量大于任何单一集群的最大可用并发量时，
系统是否真的只有一个集群在进行并发调度
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine


def single_cluster_concurrency_test():
    """单集群并发调度问题测试"""
    print("=" * 80)
    print("🔍 单集群并发调度问题测试")
    print("=" * 80)

    # 模拟集群配置 - mac为首选集群
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

    # 创建策略引擎并更新集群元数据
    policy_engine = PolicyEngine()
    policy_engine.update_cluster_metadata(cluster_configs)

    # 测试场景: 提交超过任一集群容量的任务，观察调度分布
    print(f"\n📋 测试场景: 提交超过任一集群容量的任务")
    print(f"   centos集群容量: 16 CPU")
    print(f"   mac集群容量: 8 CPU")
    print(f"   提交20个任务（超过mac集群容量，但小于centos集群容量）")

    # 模拟充足的资源情况
    current_time = time.time()
    cluster_snapshots = {
        "centos": ResourceSnapshot(
            cluster_name="centos",
            total_resources={"CPU": 16.0, "GPU": 0},
            available_resources={"CPU": 16.0, "GPU": 0},
            node_count=5,
            timestamp=current_time
        ),
        "mac": ResourceSnapshot(
            cluster_name="mac",
            total_resources={"CPU": 8.0, "GPU": 0},
            available_resources={"CPU": 8.0, "GPU": 0},
            node_count=1,
            timestamp=current_time
        )
    }

    # 统计变量
    cluster_distribution = defaultdict(int)
    queued_tasks = 0

    # 提交20个任务，不指定集群（使用负载均衡）
    print(f"\n🚀 提交20个任务（不指定集群）:")
    for i in range(20):
        task_desc = TaskDescription(
            task_id=f"load_balance_task_{i}",
            name=f"load_balance_task_{i}",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 1.0},
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
    generate_single_cluster_test_report(cluster_distribution, queued_tasks)

    return cluster_distribution, queued_tasks


def test_preferred_cluster_behavior():
    """测试指定集群的行为"""
    print(f"\n" + "=" * 80)
    print("🔍 指定集群行为测试")
    print("=" * 80)

    # 模拟集群配置
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

    # 创建策略引擎并更新集群元数据
    policy_engine = PolicyEngine()
    policy_engine.update_cluster_metadata(cluster_configs)

    # 模拟资源充足的情况
    current_time = time.time()
    cluster_snapshots = {
        "centos": ResourceSnapshot(
            cluster_name="centos",
            total_resources={"CPU": 16.0, "GPU": 0},
            available_resources={"CPU": 16.0, "GPU": 0},
            node_count=5,
            timestamp=current_time
        ),
        "mac": ResourceSnapshot(
            cluster_name="mac",
            total_resources={"CPU": 8.0, "GPU": 0},
            available_resources={"CPU": 8.0, "GPU": 0},
            node_count=1,
            timestamp=current_time
        )
    }

    # 统计变量
    cluster_distribution = defaultdict(int)
    queued_tasks = 0

    # 提交超过mac集群容量的任务，但指定到mac集群
    print(f"\n🚀 提交12个任务到mac集群（超过其8个CPU容量）:")
    for i in range(12):
        task_desc = TaskDescription(
            task_id=f"mac_preferred_task_{i}",
            name=f"mac_preferred_task_{i}",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test", "mac_preferred"],
            preferred_cluster="mac"  # 指定到mac集群
        )

        # 让策略引擎做调度决策
        decision = policy_engine.schedule(task_desc, cluster_snapshots)

        if decision and decision.cluster_name:
            cluster_distribution[decision.cluster_name] += 1
            print(f"    任务 {i}: 调度到 {decision.cluster_name}")
        else:
            queued_tasks += 1
            print(f"    任务 {i}: 进入队列等待")

    # 生成测试报告
    print(f"\n📊 指定集群测试结果统计:")
    generate_preferred_cluster_test_report(cluster_distribution, queued_tasks)

    return cluster_distribution, queued_tasks


def generate_single_cluster_test_report(cluster_distribution, queued_tasks):
    """生成单集群测试报告"""
    print("\n" + "=" * 60)
    print("📋 单集群并发调度测试报告")
    print("=" * 60)

    total_scheduled = sum(cluster_distribution.values())
    total_tasks = total_scheduled + queued_tasks

    print(f"  总提交任务数: {total_tasks}")
    print(f"  成功调度任务数: {total_scheduled}")
    print(f"  队列等待任务数: {queued_tasks}")

    print(f"\n  集群调度分布:")
    for cluster, count in cluster_distribution.items():
        percentage = (count / total_scheduled * 100) if total_scheduled > 0 else 0
        print(f"    {cluster}: {count} 个任务 ({percentage:.1f}%)")

    # 分析是否存在单集群调度问题
    if len(cluster_distribution) == 1:
        print(f"\n  ⚠️  发现单集群调度问题:")
        print(f"     所有任务都被调度到同一个集群: {list(cluster_distribution.keys())[0]}")
        print(f"     理论上应该根据负载均衡算法分布到多个集群")
    elif len(cluster_distribution) > 1:
        print(f"\n  ✅ 负载均衡正常:")
        print(f"     任务被分布到 {len(cluster_distribution)} 个集群")
        print(f"     符合负载均衡调度预期")
    else:
        print(f"\n  ⚠️  无任务被成功调度")


def generate_preferred_cluster_test_report(cluster_distribution, queued_tasks):
    """生成指定集群测试报告"""
    print("\n" + "=" * 60)
    print("📋 指定集群行为测试报告")
    print("=" * 60)

    total_scheduled = sum(cluster_distribution.values())
    total_tasks = total_scheduled + queued_tasks

    print(f"  总提交任务数: {total_tasks}")
    print(f"  成功调度任务数: {total_scheduled}")
    print(f"  队列等待任务数: {queued_tasks}")

    print(f"\n  集群调度分布:")
    for cluster, count in cluster_distribution.items():
        percentage = (count / total_scheduled * 100) if total_scheduled > 0 else 0
        print(f"    {cluster}: {count} 个任务 ({percentage:.1f}%)")

    # 分析指定集群行为
    if len(cluster_distribution) == 1 and "mac" in cluster_distribution:
        mac_scheduled = cluster_distribution["mac"]
        if mac_scheduled <= 8 and queued_tasks > 0:
            print(f"\n  ✅ 指定集群行为正常:")
            print(f"     指定集群(mac)最多调度8个任务")
            print(f"     超出容量的任务({queued_tasks}个)进入队列等待")
        elif mac_scheduled > 8:
            print(f"\n  ⚠️  指定集群行为异常:")
            print(f"     指定集群(mac)调度了超过其容量的任务")
        else:
            print(f"\n  ✅ 指定集群行为正常:")
            print(f"     所有任务都在指定集群执行")
    else:
        print(f"\n  ⚠️  指定集群行为异常:")
        print(f"     任务被调度到了非指定集群")


def analyze_scheduling_algorithm():
    """分析调度算法"""
    print("\n" + "=" * 80)
    print("🧠 调度算法分析")
    print("=" * 80)

    print(f"\n📋 负载均衡调度逻辑:")
    print(f"  1. 未指定集群的任务会经历以下决策过程:")
    print(f"     • 收集所有健康集群的资源快照")
    print(f"     • 计算每个集群的评分")
    print(f"     • 评分因素包括: 可用资源、集群权重、偏好设置、负载均衡因子")
    print(f"     • 选择评分最高的集群进行调度")

    print(f"\n  2. 评分计算公式:")
    print(f"     • 基础评分 = 可用CPU × 集群权重")
    print(f"     • GPU资源加成 = 可用GPU × 5（GPU更宝贵）")
    print(f"     • 偏好集群加成 = 1.2（如果是偏好集群）")
    print(f"     • 负载均衡因子 = 1.0 - CPU使用率")
    print(f"     • 最终评分 = (基础评分 + GPU加成) × 偏好加成 × 负载均衡因子")

    print(f"\n  3. 调度决策优先级:")
    print(f"     • 首选集群指定 > 负载均衡")
    print(f"     • 资源阈值检查 > 集群评分")
    print(f"     • 集群健康状态 > 所有其他因素")


def answer_user_observation():
    """回答用户观察到的问题"""
    print("\n" + "=" * 80)
    print("🎯 回答用户观察到的问题")
    print("=" * 80)

    print(f"\n问题: 用户通过实际任务测试发现，即使不指定优先调度集群时，")
    print(f"      当提交的并发任务数量大于任何单一集群的最大可用并发量时，")
    print(f"      也只有一个集群在进行并发调度。")

    print(f"\n分析可能的原因:")
    print(f"  1. 负载均衡算法偏向性:")
    print(f"     • mac集群设置了prefer=True，有额外的偏好加成(1.2倍)")
    print(f"     • mac集群权重为1.2，比centos的1.0更高")
    print(f"     • 即使mac集群容量较小，但综合评分可能更高")

    print(f"\n  2. 资源利用率影响:")
    print(f"     • 负载均衡因子 = 1.0 - CPU使用率")
    print(f"     • 如果一个集群刚开始调度任务，其负载均衡因子较高")
    print(f"     • 算法可能倾向于继续向同一集群调度以保持连续性")

    print(f"\n  3. 调度批次效应:")
    print(f"     • 在短时间内提交大量任务")
    print(f"     • 系统可能还未及时更新资源快照")
    print(f"     • 导致连续任务被调度到同一集群")

    print(f"\n验证建议:")
    print(f"  • 检查集群的prefer设置和权重配置")
    print(f"  • 验证负载均衡算法的实际评分计算")
    print(f"  • 观察长时间跨度内的任务分布情况")


if __name__ == "__main__":
    # 运行单集群并发调度测试
    lb_distribution, lb_queued = single_cluster_concurrency_test()

    # 运行指定集群行为测试
    pref_distribution, pref_queued = test_preferred_cluster_behavior()

    # 分析调度算法
    analyze_scheduling_algorithm()

    # 回答用户观察到的问题
    answer_user_observation()

    print("\n" + "=" * 80)
    print("🎉 单集群并发调度问题测试完成!")
    print("=" * 80)