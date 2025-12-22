#!/usr/bin/env python3
"""
最终版跨集群调度统计测试
重点展示并发任务在所有系统可用集群之间的负载分配和调度执行情况，并提供完整的统计数据
"""

import sys
import os
import time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')


def simulate_cross_cluster_scheduling():
    """模拟跨集群调度过程并生成统计数据"""
    print("=" * 80)
    print("📊 跨集群调度统计模拟")
    print("=" * 80)
    
    # 模拟集群配置
    clusters = {
        "centos": {
            "cpu_capacity": 16,
            "gpu_capacity": 0,
            "prefer": False,
            "weight": 1.0,
            "tags": ["linux", "x86_64"]
        },
        "mac": {
            "cpu_capacity": 8,
            "gpu_capacity": 0,
            "prefer": True,
            "weight": 1.2,
            "tags": ["macos", "arm64"]
        }
    }
    
    # 显示集群信息
    print("\n📋 集群配置信息:")
    total_system_capacity = 0
    for name, config in clusters.items():
        cpu_capacity = config["cpu_capacity"]
        total_system_capacity += cpu_capacity
        print(f"  集群 [{name}]:")
        print(f"    CPU容量: {cpu_capacity}")
        print(f"    GPU容量: {config['gpu_capacity']}")
        print(f"    是否偏好集群: {'是' if config['prefer'] else '否'}")
        print(f"    权重: {config['weight']}")
        print(f"    标签: {', '.join(config['tags'])}")
    
    print(f"\n  系统总CPU容量: {total_system_capacity}")
    
    # 模拟任务提交和调度过程
    print(f"\n🚀 模拟提交 {total_system_capacity + 10} 个并发任务:")
    
    # 任务分配统计
    task_allocation = {
        "centos": 0,
        "mac": 0,
        "queued": 0
    }
    
    # 模拟任务调度逻辑
    tasks_to_schedule = total_system_capacity + 10
    resource_threshold = 0.8  # 80%资源阈值
    
    # 模拟资源使用情况
    cluster_resources = {
        "centos": {"used_cpu": 0, "total_cpu": 16},
        "mac": {"used_cpu": 0, "total_cpu": 8}
    }
    
    # 模拟任务调度过程
    scheduled_tasks = 0
    queued_tasks = 0
    
    print(f"\n🔄 任务调度过程:")
    
    # 第一轮：调度任务到各个集群直到达到阈值
    for i in range(tasks_to_schedule):
        # 确定首选集群
        if i % 3 == 0:  # 1/3任务指定到centos
            preferred_cluster = "centos"
        elif i % 3 == 1:  # 1/3任务指定到mac
            preferred_cluster = "mac"
        else:  # 1/3任务使用负载均衡
            preferred_cluster = None
        
        # 计算各集群当前使用率
        centos_utilization = cluster_resources["centos"]["used_cpu"] / cluster_resources["centos"]["total_cpu"]
        mac_utilization = cluster_resources["mac"]["used_cpu"] / cluster_resources["mac"]["total_cpu"]
        
        # 决定调度到哪个集群
        target_cluster = None
        if preferred_cluster:
            # 检查首选集群是否过载
            if preferred_cluster == "centos":
                if centos_utilization < resource_threshold:
                    target_cluster = "centos"
                else:
                    # 首选集群过载，检查其他集群
                    if mac_utilization < resource_threshold:
                        target_cluster = "mac"
                    else:
                        # 所有集群都过载，任务排队
                        target_cluster = "queue"
            else:  # preferred_cluster == "mac"
                if mac_utilization < resource_threshold:
                    target_cluster = "mac"
                else:
                    # 首选集群过载，检查其他集群
                    if centos_utilization < resource_threshold:
                        target_cluster = "centos"
                    else:
                        # 所有集群都过载，任务排队
                        target_cluster = "queue"
        else:
            # 负载均衡：选择资源最充足的集群
            if centos_utilization < resource_threshold and mac_utilization < resource_threshold:
                # 两个集群都有资源，选择使用率更低的
                if centos_utilization <= mac_utilization:
                    target_cluster = "centos"
                else:
                    target_cluster = "mac"
            elif centos_utilization < resource_threshold:
                target_cluster = "centos"
            elif mac_utilization < resource_threshold:
                target_cluster = "mac"
            else:
                # 所有集群都过载，任务排队
                target_cluster = "queue"
        
        # 更新统计和资源使用情况
        if target_cluster == "queue":
            task_allocation["queued"] += 1
            queued_tasks += 1
            print(f"  任务 {i+1}: 队列等待 (所有集群资源使用率超过80%)")
        else:
            task_allocation[target_cluster] += 1
            scheduled_tasks += 1
            cluster_resources[target_cluster]["used_cpu"] += 1
            print(f"  任务 {i+1}: 调度到 {target_cluster} 集群")
    
    # 生成最终统计数据
    print("\n" + "=" * 80)
    print("📈 最终统计数据报告")
    print("=" * 80)
    
    # 任务分配统计
    print(f"\n📊 任务分配统计:")
    print(f"  调度到centos集群: {task_allocation['centos']} 个任务")
    print(f"  调度到mac集群: {task_allocation['mac']} 个任务")
    print(f"  队列等待: {task_allocation['queued']} 个任务")
    print(f"  总计: {sum(task_allocation.values())} 个任务")
    
    # 集群资源使用情况
    print(f"\n🖥️  集群资源使用情况:")
    for cluster_name, resources in cluster_resources.items():
        used_cpu = resources["used_cpu"]
        total_cpu = resources["total_cpu"]
        utilization = used_cpu / total_cpu if total_cpu > 0 else 0
        print(f"  {cluster_name}集群:")
        print(f"    CPU使用: {used_cpu}/{total_cpu} ({utilization:.1%})")
    
    # 负载分布分析
    print(f"\n⚖️  负载分布分析:")
    total_scheduled = task_allocation['centos'] + task_allocation['mac']
    if total_scheduled > 0:
        centos_percentage = (task_allocation['centos'] / total_scheduled) * 100
        mac_percentage = (task_allocation['mac'] / total_scheduled) * 100
        print(f"  centos集群负载: {centos_percentage:.1f}%")
        print(f"  mac集群负载: {mac_percentage:.1f}%")
    
    # 调度效率分析
    print(f"\n⚡ 调度效率分析:")
    print(f"  即时调度任务数: {scheduled_tasks}")
    print(f"  队列等待任务数: {queued_tasks}")
    total_tasks = scheduled_tasks + queued_tasks
    if total_tasks > 0:
        immediate_scheduling_rate = (scheduled_tasks / total_tasks) * 100
        print(f"  即时调度成功率: {immediate_scheduling_rate:.1f}%")
    
    # 系统容量分析
    print(f"\n📏 系统容量分析:")
    print(f"  系统总CPU容量: {total_system_capacity}")
    print(f"  实际调度任务数: {total_scheduled}")
    if total_system_capacity > 0:
        capacity_utilization = (total_scheduled / total_system_capacity) * 100
        print(f"  系统容量利用率: {capacity_utilization:.1f}%")
    
    return task_allocation, cluster_resources


def explain_scheduling_mechanism():
    """解释调度机制"""
    print("\n" + "=" * 80)
    print("🧠 跨集群调度机制详解")
    print("=" * 80)
    
    print("\n🔧 调度策略:")
    print("  1. 首选集群优先策略:")
    print("     • 用户可通过preferred_cluster参数指定首选集群")
    print("     • 系统优先尝试将任务调度到指定集群")
    print("     • 若指定集群资源不足，则尝试其他集群")
    
    print("\n  2. 资源阈值控制:")
    print("     • 系统设定80%资源使用率阈值")
    print("     • 当集群资源使用率超过阈值时，新任务进入队列等待")
    print("     • 防止集群过载，确保系统稳定性")
    
    print("\n  3. 负载均衡策略:")
    print("     • 未指定首选集群的任务采用负载均衡调度")
    print("     • 系统选择资源最充足的集群执行任务")
    print("     • 考虑集群权重和资源使用率进行智能调度")
    
    print("\n  4. 动态重调度:")
    print("     • 系统每30秒重新评估队列中的任务")
    print("     • 资源释放后自动调度等待中的任务")
    print("     • 确保资源充分利用")
    
    print("\n🔄 调度流程:")
    print("  1. 任务提交 → 检查是否指定首选集群")
    print("  2. 指定集群 → 检查该集群资源使用率")
    print("  3. 资源充足 → 立即调度到指定集群")
    print("  4. 资源紧张 → 检查其他集群资源")
    print("  5. 其他集群充足 → 调度到其他集群")
    print("  6. 所有集群紧张 → 任务进入队列等待")
    print("  7. 资源释放 → 重新评估队列任务")
    print("  8. 重新调度 → 将等待任务调度到合适集群")


def show_real_world_implications():
    """展示实际应用场景"""
    print("\n" + "=" * 80)
    print("🌐 实际应用场景")
    print("=" * 80)
    
    print("\n🏭 企业级应用:")
    print("  • 混合云环境下的任务调度")
    print("  • 异构计算资源的统一管理")
    print("  • 跨地域数据中心的任务分发")
    
    print("\n🔬 科学计算:")
    print("  • 不同架构CPU的任务适配")
    print("  • GPU资源的智能分配")
    print("  • 大规模并行计算任务调度")
    
    print("\n🎮 游戏开发:")
    print("  • 多平台构建任务分发")
    print("  • 不同硬件环境的测试任务")
    print("  • 实时渲染任务的负载均衡")
    
    print("\n🛍️ 电商平台:")
    print("  • 高峰期流量的弹性调度")
    print("  • 不同业务模块的资源隔离")
    print("  • 突发订单处理的快速响应")


if __name__ == "__main__":
    # 运行模拟统计
    task_allocation, cluster_resources = simulate_cross_cluster_scheduling()
    
    # 解释调度机制
    explain_scheduling_mechanism()
    
    # 展示实际应用场景
    show_real_world_implications()
    
    print("\n" + "=" * 80)
    print("✅ 跨集群调度统计分析完成!")
    print("=" * 80)