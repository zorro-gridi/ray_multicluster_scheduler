#!/usr/bin/env python3
"""
真实集群连接测试
测试submit_task接口与真实集群的连接
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
from ray_multicluster_scheduler.app.client_api.submit_task import (
    initialize_scheduler as init_task_scheduler
)


def test_real_cluster_connection():
    """测试真实集群连接"""
    print("=" * 60)
    print("测试真实集群连接")
    print("=" * 60)

    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")

        # 2. 提交简单任务到MAC集群
        print("\n2. 提交简单任务到MAC集群...")

        def simple_task(x, y):
            """简单的测试任务"""
            import time
            time.sleep(1)  # 模拟任务执行时间
            return x + y

        task_id, result = submit_task(
            func=simple_task,
            args=(10, 20),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test", "connection"],
            name="real_cluster_test_task",
            preferred_cluster="mac"
        )

        print(f"✅ 任务提交成功")
        print(f"   任务ID: {task_id}")
        print(f"   任务结果: {result}")

        # 3. 验证结果
        expected_result = 30
        if result == expected_result:
            print(f"✅ 任务结果正确: {result} == {expected_result}")
        else:
            print(f"❌ 任务结果错误: {result} != {expected_result}")

        # 4. 提交任务到CentOS集群
        print("\n3. 提交任务到CentOS集群...")

        task_id2, result2 = submit_task(
            func=simple_task,
            args=(5, 15),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test", "connection"],
            name="centos_test_task",
            preferred_cluster="centos"
        )

        print(f"✅ CentOS任务提交成功")
        print(f"   任务ID: {task_id2}")
        print(f"   任务结果: {result2}")

        # 5. 验证CentOS任务结果
        expected_result2 = 20
        if result2 == expected_result2:
            print(f"✅ CentOS任务结果正确: {result2} == {expected_result2}")
        else:
            print(f"❌ CentOS任务结果错误: {result2} != {expected_result2}")

        # 6. 测试不指定集群的任务提交（负载均衡）
        print("\n4. 测试不指定集群的任务提交（负载均衡）...")

        task_id3, result3 = submit_task(
            func=simple_task,
            args=(7, 3),
            kwargs={},
            resource_requirements={"CPU": 1.0},
            tags=["test", "load_balance"],
            name="load_balance_test_task"
            # 不指定preferred_cluster，使用负载均衡
        )

        print(f"✅ 负载均衡任务提交成功")
        print(f"   任务ID: {task_id3}")
        print(f"   任务结果: {result3}")

        # 7. 验证负载均衡任务结果
        expected_result3 = 10
        if result3 == expected_result3:
            print(f"✅ 负载均衡任务结果正确: {result3} == {expected_result3}")
        else:
            print(f"❌ 负载均衡任务结果错误: {result3} != {expected_result3}")

        # 8. 清理资源
        print("\n5. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")

        print("\n🎉 所有测试完成!")
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


def test_cluster_connectivity():
    """测试集群连通性"""
    print("\n" + "=" * 60)
    print("测试集群连通性")
    print("=" * 60)

    try:
        # 初始化调度器环境
        print("初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")

        # 获取集群监控器
        cluster_monitor = task_lifecycle_manager.cluster_monitor

        # 刷新集群资源快照
        print("刷新集群资源快照...")
        cluster_monitor.refresh_resource_snapshots(force=True)

        # 获取所有集群信息
        print("获取集群信息...")
        cluster_info = cluster_monitor.get_all_cluster_info()

        print(f"\n发现 {len(cluster_info)} 个集群:")
        for name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']

            print(f"\n集群 [{name}]:")
            print(f"  地址: {metadata.head_address}")
            print(f"  是否偏好集群: {'是' if metadata.prefer else '否'}")

            if snapshot and snapshot.available_resources:
                cpu_free = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                gpu_free = snapshot.available_resources.get("GPU", 0)
                gpu_total = snapshot.total_resources.get("GPU", 0)

                cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
                gpu_utilization = (gpu_total - gpu_free) / gpu_total if gpu_total > 0 else 0

                print(f"  CPU: {cpu_free}/{cpu_total} (使用率: {cpu_utilization:.1%})")
                print(f"  GPU: {gpu_free}/{gpu_total} (使用率: {gpu_utilization:.1%})")
            else:
                print("  ❌ 无法获取资源信息")

        # 清理资源
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("\n✅ 任务生命周期管理器已停止")

        return True

    except Exception as e:
        print(f"❌ 集群连通性测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def diagnose_connection_issues():
    """诊断连接问题"""
    print("\n" + "=" * 60)
    print("诊断连接问题")
    print("=" * 60)

    print("\n常见问题及解决方案:")
    print("1. 集群地址不可达:")
    print("   - 检查网络连通性: ping 192.168.5.2")
    print("   - 检查端口是否开放: telnet 192.168.5.2 32546")

    print("\n2. 集群服务未启动:")
    print("   - 检查Ray服务是否在集群上运行")
    print("   - 检查Head节点是否正常工作")

    print("\n3. 防火墙或安全组限制:")
    print("   - 检查防火墙规则")
    print("   - 检查云服务商的安全组设置")

    print("\n4. 配置文件问题:")
    print("   - 检查clusters.yaml配置是否正确")
    print("   - 确认head_address格式正确")

    print("\n5. Conda环境问题:")
    print("   - 确认集群上的conda环境存在")
    print("   - 检查conda环境名称是否正确")

    print("\n6. 权限问题:")
    print("   - 检查是否有足够的权限访问集群")
    print("   - 确认home_dir路径正确且可访问")


if __name__ == "__main__":
    # 测试真实集群连接
    success1 = test_real_cluster_connection()

    # 测试集群连通性
    success2 = test_cluster_connectivity()

    # 诊断连接问题
    diagnose_connection_issues()

    print("\n" + "=" * 60)
    if success1 and success2:
        print("🎉 所有测试通过!")
    else:
        print("⚠️  部分测试失败，请检查上述错误信息")
    print("=" * 60)