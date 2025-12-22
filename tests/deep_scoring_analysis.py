#!/usr/bin/env python3
"""
深度评分分析测试
深入分析负载均衡算法的评分计算过程，找出任务调度偏向性原因
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata


def deep_scoring_analysis():
    """深度评分分析"""
    print("=" * 80)
    print("🔍 深度评分分析测试")
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

    # 手动计算每个集群的评分
    print(f"\n📊 集群评分详细计算:")

    for cluster_name, snapshot in cluster_snapshots.items():
        config = cluster_configs[cluster_name]

        # 获取资源信息
        cpu_available = snapshot.available_resources.get("CPU", 0)
        cpu_total = snapshot.total_resources.get("CPU", 0)
        gpu_available = snapshot.available_resources.get("GPU", 0)
        gpu_total = snapshot.total_resources.get("GPU", 0)

        # 计算资源使用率
        cpu_utilization = (cpu_total - cpu_available) / cpu_total if cpu_total > 0 else 0
        gpu_utilization = (gpu_total - gpu_available) / gpu_total if gpu_total > 0 else 0

        # 计算评分（模拟策略引擎的评分算法）
        # 基础评分 = 可用CPU × 集群权重
        base_score = cpu_available * config.weight

        # GPU 资源加成
        gpu_bonus = gpu_available * 5  # GPU资源更宝贵

        # 偏好集群加成
        preference_bonus = 1.2 if config.prefer else 1.0

        # 负载均衡因子：资源利用率越低得分越高
        load_balance_factor = 1.0 - cpu_utilization  # 负载越低因子越高

        # 最终评分
        final_score = (base_score + gpu_bonus) * preference_bonus * load_balance_factor

        print(f"\n  集群 [{cluster_name}]:")
        print(f"    CPU可用: {cpu_available}/{cpu_total}")
        print(f"    GPU可用: {gpu_available}/{gpu_total}")
        print(f"    CPU使用率: {cpu_utilization:.2%}")
        print(f"    GPU使用率: {gpu_utilization:.2%}")
        print(f"    集群权重: {config.weight}")
        print(f"    是否偏好集群: {'是' if config.prefer else '否'}")
        print(f"    基础评分: {base_score:.2f} (可用CPU × 权重)")
        print(f"    GPU加成: {gpu_bonus:.2f} (可用GPU × 5)")
        print(f"    偏好加成: {preference_bonus:.2f}")
        print(f"    负载因子: {load_balance_factor:.2f} (1.0 - CPU使用率)")
        print(f"    最终评分: {final_score:.2f}")

    # 分析评分差异
    print(f"\n🎯 评分差异分析:")

    centos_snapshot = cluster_snapshots["centos"]
    mac_snapshot = cluster_snapshots["mac"]
    centos_config = cluster_configs["centos"]
    mac_config = cluster_configs["mac"]

    # 计算centos评分
    centos_cpu_available = centos_snapshot.available_resources.get("CPU", 0)
    centos_base_score = centos_cpu_available * centos_config.weight
    centos_gpu_bonus = centos_snapshot.available_resources.get("GPU", 0) * 5
    centos_preference_bonus = 1.2 if centos_config.prefer else 1.0
    centos_cpu_utilization = (centos_snapshot.total_resources.get("CPU", 0) - centos_cpu_available) / centos_snapshot.total_resources.get("CPU", 0) if centos_snapshot.total_resources.get("CPU", 0) > 0 else 0
    centos_load_balance_factor = 1.0 - centos_cpu_utilization
    centos_final_score = (centos_base_score + centos_gpu_bonus) * centos_preference_bonus * centos_load_balance_factor

    # 计算mac评分
    mac_cpu_available = mac_snapshot.available_resources.get("CPU", 0)
    mac_base_score = mac_cpu_available * mac_config.weight
    mac_gpu_bonus = mac_snapshot.available_resources.get("GPU", 0) * 5
    mac_preference_bonus = 1.2 if mac_config.prefer else 1.0
    mac_cpu_utilization = (mac_snapshot.total_resources.get("CPU", 0) - mac_cpu_available) / mac_snapshot.total_resources.get("CPU", 0) if mac_snapshot.total_resources.get("CPU", 0) > 0 else 0
    mac_load_balance_factor = 1.0 - mac_cpu_utilization
    mac_final_score = (mac_base_score + mac_gpu_bonus) * mac_preference_bonus * mac_load_balance_factor

    print(f"  centos最终评分: {centos_final_score:.2f}")
    print(f"  mac最终评分: {mac_final_score:.2f}")
    print(f"  评分差值: {abs(centos_final_score - mac_final_score):.2f}")

    if centos_final_score > mac_final_score:
        print(f"  ✅ centos评分更高，因此任务被调度到centos集群")
        ratio = centos_final_score / mac_final_score if mac_final_score > 0 else float('inf')
        print(f"  评分比例: {ratio:.2f}:1 (centos:mac)")
    else:
        print(f"  ✅ mac评分更高，因此任务被调度到mac集群")
        ratio = mac_final_score / centos_final_score if centos_final_score > 0 else float('inf')
        print(f"  评分比例: {ratio:.2f}:1 (mac:centos)")

    # 分析根本原因
    print(f"\n🔍 根本原因分析:")
    print(f"  1. 资源容量差异:")
    print(f"     • centos: 16 CPU vs mac: 8 CPU")
    print(f"     • CPU容量差距: 2:1")

    print(f"\n  2. 权重和偏好影响:")
    print(f"     • centos权重: 1.0")
    print(f"     • mac权重: 1.2")
    print(f"     • mac偏好加成: 1.2")
    print(f"     • mac综合优势: 1.2 × 1.2 = 1.44")

    print(f"\n  3. 最终影响:")
    print(f"     • centos基础资源评分: 16 × 1.0 = 16")
    print(f"     • mac基础资源评分: 8 × 1.2 = 9.6")
    print(f"     • 加上偏好加成后mac评分: 9.6 × 1.2 = 11.52")
    print(f"     • centos仍比mac高: 16 > 11.52")

    print(f"\n💡 结论:")
    print(f"    即使mac是偏好集群且有权重优势，但centos的CPU容量优势(2:1)")
    print(f"    足以抵消mac的权重和偏好优势(1.44倍)")
    print(f"    因此在资源充足情况下，任务会优先调度到centos集群")


def simulate_incremental_scheduling():
    """模拟增量调度过程"""
    print(f"\n" + "=" * 80)
    print("🔄 增量调度过程模拟")
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

    # 初始状态：资源充足
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

    # 模拟逐个任务调度并更新资源状态
    print(f"\n🚀 模拟逐个任务调度过程:")
    task_distribution = defaultdict(int)

    for i in range(20):
        # 为每个任务计算当前评分
        scores = {}
        for cluster_name, snapshot in cluster_snapshots.items():
            config = cluster_configs[cluster_name]

            # 获取资源信息
            cpu_available = snapshot.available_resources.get("CPU", 0)
            cpu_total = snapshot.total_resources.get("CPU", 0)
            gpu_available = snapshot.available_resources.get("GPU", 0)

            # 计算资源使用率
            cpu_utilization = (cpu_total - cpu_available) / cpu_total if cpu_total > 0 else 0

            # 计算评分
            base_score = cpu_available * config.weight
            gpu_bonus = gpu_available * 5
            preference_bonus = 1.2 if config.prefer else 1.0
            load_balance_factor = 1.0 - cpu_utilization
            final_score = (base_score + gpu_bonus) * preference_bonus * load_balance_factor

            scores[cluster_name] = final_score

        # 选择评分最高的集群
        selected_cluster = max(scores.items(), key=lambda x: x[1])[0]
        task_distribution[selected_cluster] += 1

        # 更新该集群的资源状态
        if cluster_snapshots[selected_cluster].available_resources["CPU"] > 0:
            cluster_snapshots[selected_cluster].available_resources["CPU"] -= 1

        print(f"  任务 {i+1}: 调度到 {selected_cluster} (centos: {scores['centos']:.2f}, mac: {scores['mac']:.2f})")

        # 显示当前资源状态
        if (i + 1) % 5 == 0 or i == 19:
            print(f"    当前资源状态:")
            for name, snapshot in cluster_snapshots.items():
                cpu_avail = snapshot.available_resources["CPU"]
                cpu_total = snapshot.total_resources["CPU"]
                cpu_util = (cpu_total - cpu_avail) / cpu_total if cpu_total > 0 else 0
                print(f"      {name}: {cpu_avail}/{cpu_total} CPU ({cpu_util:.1%} 已使用)")

    # 输出最终分布
    print(f"\n📊 最终任务分布:")
    total_tasks = sum(task_distribution.values())
    for cluster, count in task_distribution.items():
        percentage = (count / total_tasks) * 100
        print(f"  {cluster}: {count} 个任务 ({percentage:.1f}%)")


def provide_optimization_suggestions():
    """提供优化建议"""
    print(f"\n" + "=" * 80)
    print("💡 优化建议")
    print("=" * 80)

    print(f"\n🔧 集群配置优化:")
    print(f"  1. 调整权重设置:")
    print(f"     • 适当提高mac集群的权重(如1.5-2.0)")
    print(f"     • 或降低centos集群的权重(如0.8)")

    print(f"\n  2. 调整偏好设置:")
    print(f"     • 如果希望更均匀的负载分布，可将mac的prefer设为False")
    print(f"     • 或者将centos也设为prefer=True")

    print(f"\n  3. 动态权重调整:")
    print(f"     • 根据实际资源使用情况动态调整权重")
    print(f"     • 在调度算法中引入更复杂的负载均衡策略")

    print(f"\n📋 调度策略优化:")
    print(f"  1. 改进负载均衡算法:")
    print(f"     • 引入任务分布均衡因子")
    print(f"     • 考虑历史调度记录")
    print(f"     • 实现更智能的跨集群负载分担")

    print(f"\n  2. 增强资源感知:")
    print(f"     • 更频繁地更新资源快照")
    print(f"     • 实现实时资源监控")
    print(f"     • 考虑任务执行时间预测")


if __name__ == "__main__":
    # 运行深度评分分析
    deep_scoring_analysis()

    # 模拟增量调度过程
    simulate_incremental_scheduling()

    # 提供优化建议
    provide_optimization_suggestions()

    print("\n" + "=" * 80)
    print("🎉 深度评分分析测试完成!")
    print("=" * 80)