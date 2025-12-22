#!/usr/bin/env python3
"""
真实集群连接负载均衡测试用例
使用实际集群连接验证改进后的负载均衡策略
"""

import sys
import os
import time
import ray
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    UnifiedScheduler, 
    initialize_scheduler_environment, 
    submit_task
)
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.common.model import TaskDescription


# 用于收集统计数据的全局变量
task_statistics = {
    'total_submitted': 0,
    'total_completed': 0,
    'cluster_distribution': defaultdict(int),
    'task_results': [],
    'errors': []
}


def test_task(task_id, task_name, duration=1):
    """测试任务函数"""
    import time
    import ray
    
    start_time = time.time()
    print(f"[{ray.util.get_node_ip_address()}] 任务 {task_id} ({task_name}) 开始执行")
    time.sleep(duration)
    result = f"[{ray.util.get_node_ip_address()}] 任务 {task_id} ({task_name}) 执行完成"
    print(result)
    
    # 更新统计数据
    with threading.Lock():
        task_statistics['total_completed'] += 1
        task_statistics['cluster_distribution'][ray.util.get_node_ip_address()] += 1
        task_statistics['task_results'].append({
            'task_id': task_id,
            'task_name': task_name,
            'result': result,
            'execution_time': time.time() - start_time
        })
    
    return result


def test_real_cluster_load_balancing():
    """使用真实集群连接测试负载均衡"""
    print("=" * 80)
    print("🔍 真实集群连接负载均衡测试")
    print("=" * 80)
    
    try:
        # 1. 初始化调度环境
        print("🔧 初始化调度环境...")
        initialize_scheduler_environment()
        print("✅ 调度环境初始化完成")
        
        # 2. 创建统一调度器
        print("🔧 创建统一调度器...")
        scheduler = UnifiedScheduler()
        print("✅ 统一调度器创建完成")
        
        # 3. 初始化调度环境
        print("🔧 初始化调度环境...")
        task_lifecycle_manager = scheduler.initialize_environment()
        print("✅ 调度环境初始化完成")
        
        # 4. 等待集群连接建立
        print("⏳ 等待集群连接建立...")
        time.sleep(5)  # 给集群一些时间建立连接
        
        # 5. 显示集群状态
        print("📋 集群状态:")
        cluster_monitor = task_lifecycle_manager.cluster_monitor
        cluster_info = cluster_monitor.get_all_cluster_info()
        
        for cluster_name, info in cluster_info.items():
            if info and 'snapshot' in info and info['snapshot']:
                snapshot = info['snapshot']
                cpu_available = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                gpu_available = snapshot.available_resources.get("GPU", 0)
                gpu_total = snapshot.total_resources.get("GPU", 0)
                
                print(f"  • {cluster_name}: CPU={cpu_available}/{cpu_total}, GPU={gpu_available}/{gpu_total}")
            else:
                print(f"  • {cluster_name}: 无法获取资源信息")
        
        # 5. 提交测试任务
        print(f"\n🚀 提交测试任务...")
        futures = []
        
        # 提交10个任务，不指定集群（使用负载均衡）
        for i in range(10):
            task_id = f"real_lb_task_{i}"
            try:
                task_id, future = submit_task(
                    func=test_task,
                    args=(task_id, f"负载均衡任务{i}", 1),
                    resource_requirements={"CPU": 1.0},
                    name=f"负载均衡任务{i}"
                )
                futures.append((task_id, future))
                task_statistics['total_submitted'] += 1
                print(f"    ✓ 提交任务 {task_id}")
            except Exception as e:
                print(f"    ✗ 提交任务 {task_id} 失败: {e}")
                task_statistics['errors'].append(f"任务 {task_id} 提交失败: {e}")
        
        # 6. 等待任务完成
        print(f"\n⏳ 等待任务完成...")
        completed_tasks = 0
        for task_id, future in futures:
            try:
                result = ray.get(future, timeout=30)  # 30秒超时
                print(f"    ✓ 任务 {task_id} 完成: {result}")
                completed_tasks += 1
            except Exception as e:
                print(f"    ✗ 任务 {task_id} 失败: {e}")
                task_statistics['errors'].append(f"任务 {task_id} 执行失败: {e}")
        
        # 7. 生成测试报告
        print(f"\n📊 测试结果统计:")
        generate_real_cluster_test_report()
        
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        task_statistics['errors'].append(f"测试过程错误: {e}")
        return False


def generate_real_cluster_test_report():
    """生成真实集群测试报告"""
    print(f"\n📋 任务统计:")
    print(f"  • 总提交任务: {task_statistics['total_submitted']}个")
    print(f"  • 总完成任务: {task_statistics['total_completed']}个")
    print(f"  • 错误任务数: {len(task_statistics['errors'])}个")
    
    print(f"\n📋 集群分布:")
    total_distributed = sum(task_statistics['cluster_distribution'].values())
    for cluster, count in task_statistics['cluster_distribution'].items():
        percentage = (count / total_distributed * 100) if total_distributed > 0 else 0
        print(f"  • {cluster}: {count}个任务 ({percentage:.1f}%)")
    
    # 分析负载均衡效果
    print(f"\n📋 负载均衡分析:")
    if len(task_statistics['cluster_distribution']) > 1:
        counts = list(task_statistics['cluster_distribution'].values())
        max_count = max(counts)
        min_count = min(counts)
        balance_ratio = min_count / max_count if max_count > 0 else 0
        
        print(f"  ✅ 实现了跨集群负载均衡")
        print(f"     • 不同集群都有任务执行")
        print(f"     • 负载均衡比率: {balance_ratio:.2f} (越接近1越均衡)")
    else:
        print(f"  ⚠️  任务主要在单个集群执行")
        print(f"     • 未充分利用多集群资源")
    
    # 错误分析
    if task_statistics['errors']:
        print(f"\n📋 错误分析:")
        for error in task_statistics['errors'][:5]:  # 只显示前5个错误
            print(f"  • {error}")
        if len(task_statistics['errors']) > 5:
            print(f"  ... 还有 {len(task_statistics['errors']) - 5} 个错误")


def analyze_cluster_capabilities():
    """分析集群能力"""
    print("\n" + "=" * 80)
    print("🧠 集群能力分析")
    print("=" * 80)
    
    print(f"\n📋 集群配置:")
    print(f"  • centos集群:")
    print(f"    - 地址: 192.168.5.7:32546")
    print(f"    - 权重: 1.0")
    print(f"    - 偏好: 否")
    print(f"    - 标签: linux, x86_64")
    
    print(f"\n  • mac集群:")
    print(f"    - 地址: 192.168.5.2:32546")
    print(f"    - 权重: 1.2")
    print(f"    - 偏好: 是")
    print(f"    - 标签: macos, arm64")
    
    print(f"\n📋 负载均衡策略:")
    print(f"  1. 评分计算:")
    print(f"     • 基础评分 = 可用CPU × 集群权重")
    print(f"     • GPU资源加成 = 可用GPU × 5")
    print(f"     • 偏好集群加成 = 1.2")
    print(f"     • 负载均衡因子 = 1.0 - CPU使用率")
    print(f"     • 最终评分 = (基础评分 + GPU加成) × 偏好加成 × 负载均衡因子")
    
    print(f"\n  2. 调度决策:")
    print(f"     • 未指定集群的任务使用负载均衡策略")
    print(f"     • 根据评分选择最佳集群")
    print(f"     • 支持MAC集群特殊资源处理")


def main():
    # 分析集群能力
    analyze_cluster_capabilities()
    
    # 运行真实集群连接测试
    success = test_real_cluster_load_balancing()
    
    print("\n" + "=" * 80)
    print("🏁 测试总结")
    print("=" * 80)
    
    if success:
        if len(task_statistics['cluster_distribution']) > 1:
            print(f"✅ 负载均衡策略验证成功")
            print(f"   • 任务被分散到多个集群执行")
            print(f"   • 实现了跨集群负载均衡")
        else:
            print(f"⚠️  负载均衡策略有待改进")
            print(f"   • 任务主要在单个集群执行")
            print(f"   • 未充分利用多集群资源")
    else:
        print(f"❌ 测试执行失败")
        print(f"   • 请检查集群连接状态")
        print(f"   • 确认集群配置正确")
    
    print(f"\n📈 最终统计:")
    print(f"   • 总提交任务: {task_statistics['total_submitted']}个")
    print(f"   • 总完成任务: {task_statistics['total_completed']}个")
    for cluster, count in task_statistics['cluster_distribution'].items():
        print(f"   • {cluster}: {count}个任务")


if __name__ == "__main__":
    # 导入threading模块
    import threading
    main()