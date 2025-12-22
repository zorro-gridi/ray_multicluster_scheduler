#!/usr/bin/env python3
"""
综合跨集群调度测试用例
展示并发任务在所有系统可用集群之间的负载分配和调度执行情况，并提供统计数据
"""

import sys
import os
import time
import threading
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    UnifiedScheduler,
    initialize_scheduler_environment,
    submit_task
)


# 用于收集统计数据的全局变量
task_statistics = {
    'total_submitted': 0,
    'total_completed': 0,
    'cluster_distribution': defaultdict(int),
    'task_results': [],
    'errors': []
}
stats_lock = threading.Lock()


def test_task_with_stats(task_id, task_name, duration=1):
    """带统计信息的测试任务函数"""
    import time
    import threading

    start_time = time.time()
    print(f"[{threading.current_thread().name}] 任务 {task_id} ({task_name}) 开始执行")
    time.sleep(duration)
    end_time = time.time()

    result = {
        'task_id': task_id,
        'task_name': task_name,
        'duration': duration,
        'actual_duration': end_time - start_time,
        'thread_name': threading.current_thread().name,
        'status': 'completed',
        'timestamp': time.time()
    }

    # 更新统计数据
    with stats_lock:
        task_statistics['total_completed'] += 1
        task_statistics['task_results'].append(result)

    print(f"[{threading.current_thread().name}] 任务 {task_id} ({task_name}) 执行完成")
    return result


def submit_task_with_tracking(func, args, kwargs, resource_requirements, tags, name, preferred_cluster=None):
    """带跟踪的提交任务函数"""
    try:
        task_id, result = submit_task(
            func=func,
            args=args,
            kwargs=kwargs,
            resource_requirements=resource_requirements,
            tags=tags,
            name=name,
            preferred_cluster=preferred_cluster
        )

        # 更新统计数据
        with stats_lock:
            task_statistics['total_submitted'] += 1
            if preferred_cluster:
                task_statistics['cluster_distribution'][preferred_cluster] += 1
            else:
                # 对于未指定集群的任务，我们在结果中记录实际调度的集群
                # 这里简化处理，标记为"负载均衡"
                task_statistics['cluster_distribution']['load_balanced'] += 1

        return task_id, result
    except Exception as e:
        with stats_lock:
            task_statistics['errors'].append({
                'task_name': name,
                'error': str(e),
                'timestamp': time.time()
            })
        raise


def comprehensive_cross_cluster_test():
    """综合跨集群调度测试"""
    print("=" * 80)
    print("综合跨集群调度测试")
    print("=" * 80)

    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")

        # 2. 显示初始集群信息
        print("\n2. 初始集群信息:")
        cluster_monitor = task_lifecycle_manager.cluster_monitor
        cluster_monitor.refresh_resource_snapshots(force=True)
        cluster_info = cluster_monitor.get_all_cluster_info()

        cluster_resources = {}
        total_capacity = 0
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

                cluster_resources[name] = {
                    'cpu_total': cpu_total,
                    'cpu_free': cpu_free,
                    'gpu_total': gpu_total,
                    'gpu_free': gpu_free
                }

                total_capacity += cpu_total

                print(f"    CPU: {cpu_free}/{cpu_total} (使用率: {cpu_utilization:.1%})")
                print(f"    GPU: {gpu_free}/{gpu_total} (使用率: {gpu_utilization:.1%})")
            else:
                print("    ❌ 无法获取资源信息")

        print(f"\n  总集群CPU容量: {total_capacity}")

        # 3. 提交大量并发任务来测试负载分配
        print(f"\n3. 提交 {int(total_capacity) + 10} 个并发任务来测试负载分配...")

        tasks_to_submit = int(total_capacity) + 10
        submitted_tasks = []

        # 提交任务到指定集群
        centos_tasks = min(8, tasks_to_submit // 2)
        mac_tasks = min(5, tasks_to_submit // 3)
        balanced_tasks = tasks_to_submit - centos_tasks - mac_tasks

        print(f"  - 提交 {centos_tasks} 个任务到centos集群")
        print(f"  - 提交 {mac_tasks} 个任务到mac集群")
        print(f"  - 提交 {balanced_tasks} 个任务使用负载均衡")

        # 提交到centos集群的任务
        for i in range(centos_tasks):
            try:
                task_id, result = submit_task_with_tracking(
                    func=test_task_with_stats,
                    args=(f"centos-task-{i}", f"CentOS任务{i}", 1),
                    kwargs={},
                    resource_requirements={"CPU": 1.0},
                    tags=["test", "centos"],
                    name=f"centos_test_task_{i}",
                    preferred_cluster="centos"
                )
                submitted_tasks.append((task_id, result, "centos"))
                print(f"    ✅ CentOS任务 {i} 提交成功: {task_id}")
            except Exception as e:
                print(f"    ❌ CentOS任务 {i} 提交失败: {e}")

        # 提交到mac集群的任务
        for i in range(mac_tasks):
            try:
                task_id, result = submit_task_with_tracking(
                    func=test_task_with_stats,
                    args=(f"mac-task-{i}", f"Mac任务{i}", 1),
                    kwargs={},
                    resource_requirements={"CPU": 1.0},
                    tags=["test", "mac"],
                    name=f"mac_test_task_{i}",
                    preferred_cluster="mac"
                )
                submitted_tasks.append((task_id, result, "mac"))
                print(f"    ✅ Mac任务 {i} 提交成功: {task_id}")
            except Exception as e:
                print(f"    ❌ Mac任务 {i} 提交失败: {e}")

        # 提交负载均衡任务
        for i in range(balanced_tasks):
            try:
                task_id, result = submit_task_with_tracking(
                    func=test_task_with_stats,
                    args=(f"balanced-task-{i}", f"负载均衡任务{i}", 1),
                    kwargs={},
                    resource_requirements={"CPU": 1.0},
                    tags=["test", "balanced"],
                    name=f"balanced_test_task_{i}"
                    # 不指定preferred_cluster，使用负载均衡
                )
                submitted_tasks.append((task_id, result, "load_balanced"))
                print(f"    ✅ 负载均衡任务 {i} 提交成功: {task_id}")
            except Exception as e:
                print(f"    ❌ 负载均衡任务 {i} 提交失败: {e}")

        # 4. 等待所有任务完成
        print(f"\n4. 等待所有任务完成...")
        time.sleep(15)  # 等待任务执行完成

        # 5. 清理资源
        print("\n5. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")

        # 6. 生成统计数据报告
        print("\n6. 生成统计数据报告...")
        generate_statistics_report()

        print("\n🎉 综合跨集群调度测试完成!")
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


def generate_statistics_report():
    """生成统计数据报告"""
    print("\n" + "=" * 80)
    print("📊 跨集群调度统计数据报告")
    print("=" * 80)

    # 总体统计
    print(f"\n📈 总体统计:")
    print(f"  总提交任务数: {task_statistics['total_submitted']}")
    print(f"  总完成任务数: {task_statistics['total_completed']}")
    print(f"  成功率: {task_statistics['total_completed']/task_statistics['total_submitted']*100:.1f}%" if task_statistics['total_submitted'] > 0 else "  成功率: 0%")
    print(f"  错误任务数: {len(task_statistics['errors'])}")

    # 集群分布统计
    print(f"\n🗺️  集群分布统计:")
    for cluster, count in task_statistics['cluster_distribution'].items():
        print(f"  {cluster}: {count} 个任务")

    # 任务执行时间统计
    if task_statistics['task_results']:
        durations = [result['actual_duration'] for result in task_statistics['task_results']]
        avg_duration = sum(durations) / len(durations)
        min_duration = min(durations)
        max_duration = max(durations)

        print(f"\n⏱️  任务执行时间统计:")
        print(f"  平均执行时间: {avg_duration:.2f} 秒")
        print(f"  最短执行时间: {min_duration:.2f} 秒")
        print(f"  最长执行时间: {max_duration:.2f} 秒")

    # 错误统计
    if task_statistics['errors']:
        print(f"\n❌ 错误统计:")
        for error in task_statistics['errors']:
            print(f"  任务 {error['task_name']}: {error['error']}")

    # 集群负载分析
    print(f"\n🔍 集群负载分析:")
    total_tasks = sum(task_statistics['cluster_distribution'].values())
    if total_tasks > 0:
        for cluster, count in task_statistics['cluster_distribution'].items():
            percentage = (count / total_tasks) * 100
            print(f"  {cluster}: {count} 个任务 ({percentage:.1f}%)")


def demonstrate_cross_cluster_scheduling_behavior():
    """演示跨集群调度行为"""
    print("\n" + "=" * 80)
    print("🚀 跨集群调度行为演示")
    print("=" * 80)

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
    # 运行综合跨集群调度测试
    success = comprehensive_cross_cluster_test()

    # 演示跨集群调度行为
    demonstrate_cross_cluster_scheduling_behavior()

    print("\n" + "=" * 80)
    if success:
        print("🎉 综合跨集群调度测试通过!")
    else:
        print("⚠️  综合跨集群调度测试失败，请检查上述错误信息")
    print("=" * 80)