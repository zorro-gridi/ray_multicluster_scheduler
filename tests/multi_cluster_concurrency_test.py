#!/usr/bin/env python3
"""
多集群并发调度测试用例
验证当提交的并发任务数量大于任何单一集群的最大可用并发量时，
系统是否会将任务分散到多个集群进行调度，而不是只在一个集群上调度
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager


def multi_cluster_concurrency_test():
    """多集群并发调度测试"""
    print("=" * 80)
    print("🔍 多集群并发调度测试")
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
    print(f"   • 应该有6个任务立即调度，24个任务排队")
    
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
    generate_multi_cluster_test_report(cluster_distribution, queued_tasks, cluster_snapshots)
    
    return cluster_distribution, queued_tasks


def generate_multi_cluster_test_report(cluster_distribution, queued_tasks, cluster_snapshots):
    """生成多集群测试报告"""
    print(f"\n📋 集群分布统计:")
    total_scheduled = sum(cluster_distribution.values())
    
    for cluster_name, count in cluster_distribution.items():
        # 获取集群信息
        snapshot = cluster_snapshots.get(cluster_name)
        if snapshot:
            cpu_total = snapshot.total_resources.get("CPU", 0)
            cpu_available = snapshot.available_resources.get("CPU", 0)
            
            # 对于MAC集群，检查MacCPU资源
            if "mac" in cluster_name.lower():
                mac_cpu_total = snapshot.total_resources.get("MacCPU", 0)
                mac_cpu_available = snapshot.available_resources.get("MacCPU", 0)
                if mac_cpu_total > cpu_total:
                    cpu_total = mac_cpu_total
                    cpu_available = mac_cpu_available
            
            print(f"  • {cluster_name}: {count}个任务 (总CPU: {cpu_total}, 可用CPU: {cpu_available})")
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
    total_capacity = 0
    for cluster_name, snapshot in cluster_snapshots.items():
        cpu_total = snapshot.total_resources.get("CPU", 0)
        # 对于MAC集群，检查MacCPU资源
        if "mac" in cluster_name.lower():
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


def analyze_scheduling_mechanism():
    """分析调度机制"""
    print("\n" + "=" * 80)
    print("🧠 调度机制分析")
    print("=" * 80)
    
    print(f"\n📋 负载均衡调度逻辑:")
    print(f"  1. 未指定集群的任务调度流程:")
    print(f"     • 收集所有健康集群的资源快照")
    print(f"     • 检查各集群资源使用率是否超过阈值(80%)")
    print(f"     • 对未超过阈值的集群进行评分")
    print(f"     • 选择评分最高的集群进行调度")
    
    print(f"\n  2. 评分计算方法:")
    print(f"     • 基础评分 = 可用CPU × 集群权重")
    print(f"     • GPU资源加成 = 可用GPU × 5（GPU更宝贵）")
    print(f"     • 偏好集群加成 = 1.2（如果是偏好集群）")
    print(f"     • 负载均衡因子 = 1.0 - CPU使用率")
    print(f"     • 最终评分 = (基础评分 + GPU加成) × 偏好加成 × 负载均衡因子")
    
    print(f"\n  3. 特殊处理:")
    print(f"     • MAC集群会优先使用MacCPU资源（如果比标准CPU资源大）")
    print(f"     • 资源使用率超过80%的集群会被排除在调度之外")
    print(f"     • 任务需求超过集群容量时会进入队列等待")


def main():
    # 运行多集群并发调度测试
    cluster_dist, queued = multi_cluster_concurrency_test()
    
    # 分析调度机制
    analyze_scheduling_mechanism()
    
    print("\n" + "=" * 80)
    print("🏁 测试总结")
    print("=" * 80)
    
    total_scheduled = sum(cluster_dist.values())
    if len(cluster_dist) > 1:
        print(f"✅ 测试结果表明系统能够实现跨集群负载均衡")
        print(f"   • 任务被分散到多个集群执行")
        print(f"   • 充分利用了所有可用集群的资源")
    else:
        print(f"⚠️  测试结果表明系统可能存在调度局限性")
        print(f"   • 任务主要集中在单个集群执行")
        print(f"   • 可能未充分利用所有可用集群")
    
    print(f"\n📈 调度统计:")
    print(f"   • 立即调度任务: {total_scheduled}个")
    print(f"   • 进入队列任务: {queued}个")


if __name__ == "__main__":
    main()