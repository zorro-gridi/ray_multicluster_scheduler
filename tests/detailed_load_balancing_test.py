#!/usr/bin/env python3
"""
深入分析负载均衡策略测试
增加任务数量并详细分析评分过程
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata


def detailed_load_balancing_analysis():
    """详细分析负载均衡策略"""
    print("=" * 80)
    print("🔍 深入分析负载均衡策略测试")
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
    
    # 模拟集群资源快照
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
    
    # 显示初始集群信息
    print(f"📋 初始集群信息:")
    for cluster_name, snapshot in cluster_snapshots.items():
        cpu_available = snapshot.available_resources.get("CPU", 0)
        cpu_total = snapshot.total_resources.get("CPU", 0)
        print(f"  • {cluster_name}: CPU={cpu_available}/{cpu_total}")
    
    # 创建策略引擎
    print("🔧 创建策略引擎...")
    policy_engine = PolicyEngine()
    policy_engine.update_cluster_metadata(cluster_configs)
    print("✅ 策略引擎创建完成")
    
    # 手动计算初始评分
    print(f"\n📊 初始集群评分:")
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
        weight = config.weight
        prefer = config.prefer
        
        # 计算评分
        base_score = cpu_free * weight
        gpu_bonus = gpu_free * 5
        preference_bonus = 1.2 if prefer else 1.0
        cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
        load_factor = 1.0 - cpu_utilization
        final_score = (base_score + gpu_bonus) * preference_bonus * load_factor
        
        print(f"  • {cluster_name}:")
        print(f"    - 可用CPU: {cpu_free}")
        print(f"    - 权重: {weight}")
        print(f"    - 偏好: {prefer}")
        print(f"    - 基础评分: {base_score}")
        print(f"    - GPU加成: {gpu_bonus}")
        print(f"    - 偏好加成: {preference_bonus}")
        print(f"    - 负载因子: {load_factor}")
        print(f"    - 最终评分: {final_score}")
    
    # 模拟任务调度决策
    print(f"\n🚀 模拟任务调度决策...")
    cluster_distribution = defaultdict(int)
    
    # 提交50个任务，每个任务需要1个CPU，不指定集群
    # 总共需要50个CPU，但两个集群总共只有24个CPU
    print(f"   • 提交50个任务，每个任务需要1个CPU")
    print(f"   • 总需求: 50个CPU")
    print(f"   • 总容量: 24个CPU (centos: 16, mac: 8)")
    
    for i in range(50):
        task_desc = TaskDescription(
            task_id=f"detailed_lb_task_{i}",
            name=f"详细负载均衡测试任务{i}",
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
            if i < 30:  # 只显示前30个任务的详细信息
                print(f"    任务 {i}: 调度到 {decision.cluster_name} - {decision.reason}")
        else:
            if i < 30:  # 只显示前30个任务的详细信息
                print(f"    任务 {i}: 无法调度")
    
    # 生成测试报告
    print(f"\n📊 调度决策统计:")
    total_scheduled = sum(cluster_distribution.values())
    for cluster_name, count in cluster_distribution.items():
        percentage = (count / total_scheduled * 100) if total_scheduled > 0 else 0
        print(f"  • {cluster_name}: {count}个任务 ({percentage:.1f}%)")
    
    # 分析负载均衡效果
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


def test_with_dynamic_resource_updates():
    """测试动态资源更新情况下的负载均衡"""
    print("\n" + "=" * 80)
    print("🔄 动态资源更新测试")
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
    
    # 初始化集群资源
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
    
    # 创建策略引擎
    policy_engine = PolicyEngine()
    policy_engine.update_cluster_metadata(cluster_configs)
    
    # 统计变量
    cluster_distribution = defaultdict(int)
    
    # 提交30个任务，每个任务需要1个CPU，不指定集群
    print(f"🚀 提交30个任务（每个任务需要1个CPU）:")
    
    for i in range(30):
        # 创建当前的资源快照
        current_time = time.time()
        cluster_snapshots = {}
        
        for cluster_name, resources in cluster_resources.items():
            cluster_snapshots[cluster_name] = ResourceSnapshot(
                cluster_name=cluster_name,
                total_resources=resources["total"],
                available_resources=resources["available"],
                node_count=1,
                timestamp=current_time
            )
        
        task_desc = TaskDescription(
            task_id=f"dynamic_lb_task_{i}",
            name=f"动态负载均衡测试任务{i}",
            func_or_class=lambda: None,
            args=(),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test", "dynamic_load_balance"],
            preferred_cluster=None
        )
        
        # 让策略引擎做调度决策
        decision = policy_engine.schedule(task_desc, cluster_snapshots)
        
        if decision and decision.cluster_name:
            cluster_distribution[decision.cluster_name] += 1
            print(f"    任务 {i}: 调度到 {decision.cluster_name} - {decision.reason}")
            
            # 更新集群资源（模拟任务占用资源）
            selected_cluster = decision.cluster_name
            if selected_cluster in cluster_resources:
                # 减少可用资源
                cpu_type = "MacCPU" if selected_cluster == "mac" else "CPU"
                current_available = cluster_resources[selected_cluster]["available"].get(cpu_type, 0)
                new_available = max(0, current_available - 1.0)
                cluster_resources[selected_cluster]["available"][cpu_type] = new_available
                
                # 同时更新标准CPU资源，保持一致性
                if cpu_type == "MacCPU":
                    cluster_resources[selected_cluster]["available"]["CPU"] = new_available
        else:
            print(f"    任务 {i}: 无法调度")
    
    # 生成测试报告
    print(f"\n📊 动态资源更新测试结果:")
    total_scheduled = sum(cluster_distribution.values())
    for cluster_name, count in cluster_distribution.items():
        percentage = (count / total_scheduled * 100) if total_scheduled > 0 else 0
        print(f"  • {cluster_name}: {count}个任务 ({percentage:.1f}%)")
    
    # 分析负载均衡效果
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


def main():
    # 运行详细分析测试
    cluster_dist1 = detailed_load_balancing_analysis()
    
    # 运行动态资源更新测试
    cluster_dist2 = test_with_dynamic_resource_updates()
    
    print("\n" + "=" * 80)
    print("🏁 综合测试总结")
    print("=" * 80)
    
    # 综合分析两个测试的结果
    total_tests = 2
    balanced_tests = 0
    
    if len(cluster_dist1) > 1:
        balanced_tests += 1
    
    if len(cluster_dist2) > 1:
        balanced_tests += 1
    
    if balanced_tests > 0:
        print(f"✅ 负载均衡策略验证部分成功")
        print(f"   • {balanced_tests}/{total_tests} 个测试实现了跨集群负载均衡")
        print(f"   • 系统能够在一定程度上实现负载均衡")
    else:
        print(f"⚠️  负载均衡策略有待改进")
        print(f"   • 所有测试均未实现跨集群负载均衡")
        print(f"   • 需要进一步优化评分策略")
    
    print(f"\n📈 最终统计:")
    print(f"   • 详细分析测试: centos={cluster_dist1.get('centos', 0)}, mac={cluster_dist1.get('mac', 0)}")
    print(f"   • 动态更新测试: centos={cluster_dist2.get('centos', 0)}, mac={cluster_dist2.get('mac', 0)}")


if __name__ == "__main__":
    main()