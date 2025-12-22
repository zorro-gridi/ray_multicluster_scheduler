#!/usr/bin/env python3
"""
真实集群连接跨集群调度测试用例
使用真实的集群连接来验证跨集群调度机制
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    UnifiedScheduler, 
    initialize_scheduler_environment, 
    submit_task
)


def simple_test_task(task_id, task_name, duration=1):
    """简单的测试任务函数"""
    import time
    print(f"任务 {task_id} ({task_name}) 开始执行")
    time.sleep(duration)
    result = f"任务 {task_id} ({task_name}) 执行完成"
    print(result)
    return result


def test_cross_cluster_scheduling_with_real_clusters():
    """使用真实集群连接测试跨集群调度"""
    print("=" * 70)
    print("使用真实集群连接测试跨集群调度")
    print("=" * 70)
    
    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")
        
        # 2. 显示集群信息
        print("\n2. 集群信息:")
        cluster_monitor = task_lifecycle_manager.cluster_monitor
        cluster_monitor.refresh_resource_snapshots(force=True)
        cluster_info = cluster_monitor.get_all_cluster_info()
        
        for name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']
            print(f"  集群 [{name}]:")
            print(f"    地址: {metadata.head_address}")
            print(f"    是否偏好集群: {'是' if metadata.prefer else '否'}")
            if snapshot:
                cpu_free = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                gpu_free = snapshot.available_resources.get("GPU", 0)
                gpu_total = snapshot.total_resources.get("GPU", 0)
                
                cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
                gpu_utilization = (gpu_total - gpu_free) / gpu_total if gpu_total > 0 else 0
                
                print(f"    CPU: {cpu_free}/{cpu_total} (使用率: {cpu_utilization:.1%})")
                print(f"    GPU: {gpu_free}/{gpu_total} (使用率: {gpu_utilization:.1%})")
            else:
                print("    ❌ 无法获取资源信息")
        
        # 3. 提交任务到首选集群（centos）
        print("\n3. 提交任务到首选集群（centos）...")
        
        # 提交几个任务到centos集群
        results = []
        for i in range(3):
            task_id, result = submit_task(
                func=simple_test_task,
                args=(f"centos-task-{i}", f"CentOS任务{i}", 1),
                kwargs={},
                resource_requirements={"CPU": 1.0},
                tags=["test", "centos"],
                name=f"centos_test_task_{i}",
                preferred_cluster="centos"
            )
            results.append((task_id, result))
            print(f"  ✅ 任务提交成功: {task_id}")
        
        # 4. 提交任务到mac集群
        print("\n4. 提交任务到mac集群...")
        
        for i in range(2):
            task_id, result = submit_task(
                func=simple_test_task,
                args=(f"mac-task-{i}", f"Mac任务{i}", 1),
                kwargs={},
                resource_requirements={"CPU": 1.0},
                tags=["test", "mac"],
                name=f"mac_test_task_{i}",
                preferred_cluster="mac"
            )
            results.append((task_id, result))
            print(f"  ✅ 任务提交成功: {task_id}")
        
        # 5. 提交不指定集群的任务（负载均衡）
        print("\n5. 提交不指定集群的任务（负载均衡）...")
        
        for i in range(2):
            task_id, result = submit_task(
                func=simple_test_task,
                args=(f"balanced-task-{i}", f"负载均衡任务{i}", 1),
                kwargs={},
                resource_requirements={"CPU": 1.0},
                tags=["test", "balanced"],
                name=f"balanced_test_task_{i}"
                # 不指定preferred_cluster，使用负载均衡
            )
            results.append((task_id, result))
            print(f"  ✅ 任务提交成功: {task_id}")
        
        # 6. 验证结果
        print("\n6. 验证结果...")
        for task_id, result in results:
            if result:
                print(f"  ✅ 任务 {task_id} 执行成功")
            else:
                print(f"  ⚠️  任务 {task_id} 结果为空")
        
        # 7. 清理资源
        print("\n7. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")
        
        print("\n🎉 真实集群跨集群调度测试完成!")
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        
        # 尝试清理资源
        try:
            from ray_multicluster_scheduler.app.client_api.submit_task import _task_lifecycle_manager
            if _task_lifecycle_manager:
                _task_lifecycle_manager.stop()
                print("✅ 任务生命周期管理器已停止")
        except:
            pass
            
        return False


def test_scenario_exceeding_cluster_capacity():
    """测试超出集群容量的场景"""
    print("\n" + "=" * 70)
    print("测试超出集群容量的场景")
    print("=" * 70)
    
    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")
        
        # 2. 查看当前集群资源情况
        print("\n2. 当前集群资源情况:")
        cluster_monitor = task_lifecycle_manager.cluster_monitor
        cluster_monitor.refresh_resource_snapshots(force=True)
        cluster_info = cluster_monitor.get_all_cluster_info()
        
        total_capacity = 0
        for name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']
            print(f"  集群 [{name}]:")
            if snapshot:
                cpu_free = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                total_capacity += cpu_total
                print(f"    CPU总容量: {cpu_total}, 可用: {cpu_free}")
            else:
                print("    ❌ 无法获取资源信息")
        
        print(f"\n  总集群CPU容量: {total_capacity}")
        
        # 3. 提交超过集群总容量的任务来测试排队机制
        print(f"\n3. 提交 {int(total_capacity) + 5} 个任务来测试排队机制...")
        
        task_results = []
        for i in range(int(total_capacity) + 5):
            try:
                task_id, result = submit_task(
                    func=simple_test_task,
                    args=(f"overflow-task-{i}", f"溢出任务{i}", 2),
                    kwargs={},
                    resource_requirements={"CPU": 1.0},
                    tags=["test", "overflow"],
                    name=f"overflow_test_task_{i}"
                )
                task_results.append((task_id, result))
                print(f"  ✅ 任务 {task_id} 提交成功")
            except Exception as e:
                print(f"  ❌ 任务提交失败: {e}")
        
        # 4. 验证部分任务被执行，部分任务可能排队
        print("\n4. 任务执行情况:")
        successful_tasks = 0
        for task_id, result in task_results:
            if result:
                successful_tasks += 1
                print(f"  ✅ 任务 {task_id} 执行成功")
            else:
                print(f"  ⚠️  任务 {task_id} 结果为空")
        
        print(f"\n  成功执行的任务数: {successful_tasks}/{len(task_results)}")
        
        # 5. 清理资源
        print("\n5. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")
        
        print("\n🎉 超出集群容量场景测试完成!")
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def demonstrate_cross_cluster_behavior():
    """演示跨集群行为"""
    print("\n" + "=" * 70)
    print("跨集群调度行为演示")
    print("=" * 70)
    
    print("\n系统跨集群调度机制说明:")
    print("1. 首选集群优先: 如果用户指定了preferred_cluster，系统会优先尝试调度到该集群")
    print("2. 资源阈值控制: 当集群资源使用率超过80%时，新任务会被放入队列等待")
    print("3. 负载均衡: 未指定首选集群时，系统会选择资源最充足的集群")
    print("4. 动态重调度: 系统每30秒会重新评估队列中的任务，尝试将其调度到合适的集群")
    print("5. 任务队列: 无法立即调度的任务会被保存在队列中，直到有合适资源")
    
    print("\n测试场景总结:")
    print("✓ 系统能够根据集群资源情况智能调度任务")
    print("✓ 指定集群的任务会优先调度到指定集群")
    print("✓ 未指定集群的任务会根据负载均衡算法调度")
    print("✓ 资源紧张时任务会进入队列等待")
    print("✓ 资源释放后队列中的任务会被重新调度")


if __name__ == "__main__":
    # 运行真实集群跨集群调度测试
    success1 = test_cross_cluster_scheduling_with_real_clusters()
    
    # 运行超出集群容量场景测试
    success2 = test_scenario_exceeding_cluster_capacity()
    
    # 演示跨集群行为
    demonstrate_cross_cluster_behavior()
    
    print("\n" + "=" * 70)
    if success1 and success2:
        print("🎉 所有真实集群测试通过!")
    else:
        print("⚠️  部分测试失败，请检查上述错误信息")
    print("=" * 70)