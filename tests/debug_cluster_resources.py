#!/usr/bin/env python3
"""
调试集群资源报告
连接到实际的Ray集群，检查资源报告是否准确
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

import ray
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager, ClusterConfig


def debug_ray_cluster_resources():
    """调试Ray集群资源报告"""
    print("=" * 60)
    print("调试Ray集群资源报告")
    print("=" * 60)

    # 集群配置
    cluster_configs = [
        {
            "name": "mac",
            "address": "ray://192.168.5.2:32546"
        },
        {
            "name": "centos",
            "address": "ray://192.168.5.7:32546"
        }
    ]

    for config in cluster_configs:
        print(f"\n检查集群: {config['name']} ({config['address']})")
        print("-" * 40)

        try:
            # 尝试连接到集群
            print("1. 连接到Ray集群...")
            ray.init(
                address=config['address'],
                ignore_reinit_error=True,
                logging_level="WARNING"
            )

            if not ray.is_initialized():
                print("   ❌ 连接失败")
                continue

            print("   ✅ 连接成功")

            # 等待连接稳定
            time.sleep(1)

            # 获取资源信息
            print("2. 获取资源信息...")
            avail_resources = ray.available_resources()
            total_resources = ray.cluster_resources()
            nodes = ray.nodes()

            print(f"   可用资源: {avail_resources}")
            print(f"   总资源: {total_resources}")
            print(f"   节点数: {len(nodes)}")

            # 提取CPU信息
            cpu_available = avail_resources.get("CPU", 0)
            cpu_total = total_resources.get("CPU", 0)

            print(f"   CPU: 可用={cpu_available}, 总计={cpu_total}")

            # 计算CPU使用率
            if cpu_total > 0:
                cpu_utilization = (cpu_total - cpu_available) / cpu_total
                print(f"   CPU使用率: {cpu_utilization:.2%}")
            else:
                print("   CPU使用率: 无法计算 (总CPU为0)")

            # 提取GPU信息
            gpu_available = avail_resources.get("GPU", 0)
            gpu_total = total_resources.get("GPU", 0)

            print(f"   GPU: 可用={gpu_available}, 总计={gpu_total}")

            if gpu_total > 0:
                gpu_utilization = (gpu_total - gpu_available) / gpu_total
                print(f"   GPU使用率: {gpu_utilization:.2%}")
            else:
                print("   GPU使用率: 无GPU资源")

            # 显示节点详细信息
            print("3. 节点详细信息:")
            for i, node in enumerate(nodes):
                print(f"   节点 {i+1}:")
                print(f"     节点ID: {node.get('NodeID', 'N/A')}")
                print(f"     资源: {node.get('Resources', {})}")
                print(f"     状态: {'存活' if node.get('Alive', False) else '离线'}")

            # 断开连接
            ray.shutdown()
            print("   🔌 断开连接")

        except Exception as e:
            print(f"   ❌ 连接或查询失败: {e}")
            import traceback
            traceback.print_exc()

            # 确保断开连接
            try:
                ray.shutdown()
            except:
                pass


def simulate_task_and_check_resources():
    """模拟任务执行并检查资源变化"""
    print("\n" + "=" * 60)
    print("模拟任务执行并检查资源变化")
    print("=" * 60)

    cluster_address = "ray://192.168.5.2:32546"
    print(f"连接到集群: {cluster_address}")

    try:
        # 连接到集群
        ray.init(
            address=cluster_address,
            ignore_reinit_error=True,
            logging_level="WARNING"
        )

        if not ray.is_initialized():
            print("❌ 连接失败")
            return

        print("✅ 连接成功")

        # 获取初始资源
        print("\n1. 获取初始资源状态:")
        initial_avail = ray.available_resources()
        initial_total = ray.cluster_resources()
        initial_cpu_avail = initial_avail.get("CPU", 0)
        initial_cpu_total = initial_total.get("CPU", 0)

        print(f"   初始可用CPU: {initial_cpu_avail}")
        print(f"   初始总CPU: {initial_cpu_total}")

        if initial_cpu_total > 0:
            initial_util = (initial_cpu_total - initial_cpu_avail) / initial_cpu_total
            print(f"   初始CPU使用率: {initial_util:.2%}")

        # 定义一个CPU密集型任务
        @ray.remote
        def cpu_intensive_task(duration):
            """CPU密集型任务"""
            import time
            start_time = time.time()
            while time.time() - start_time < duration:
                # 执行一些计算密集型操作
                sum(range(1000000))
            return f"任务完成，执行时间: {duration}秒"

        # 提交任务
        print("\n2. 提交CPU密集型任务...")
        task_duration = 10  # 10秒任务
        task_ref = cpu_intensive_task.remote(task_duration)
        print(f"   任务已提交，预计执行时间: {task_duration}秒")

        # 定期检查资源变化
        print("\n3. 定期检查资源变化:")
        for i in range(task_duration + 5):  # 检查比任务时间稍长一些
            time.sleep(1)

            current_avail = ray.available_resources()
            current_cpu_avail = current_avail.get("CPU", 0)

            print(f"   第{i+1}秒 - 可用CPU: {current_cpu_avail}")

            # 检查是否有变化
            if current_cpu_avail != initial_cpu_avail:
                print(f"   📊 资源发生变化! 可用CPU从 {initial_cpu_avail} 变为 {current_cpu_avail}")

                if initial_cpu_total > 0:
                    current_util = (initial_cpu_total - current_cpu_avail) / initial_cpu_total
                    print(f"   当前CPU使用率: {current_util:.2%}")

        # 获取任务结果
        print("\n4. 获取任务结果:")
        result = ray.get(task_ref)
        print(f"   任务结果: {result}")

        # 获取最终资源
        print("\n5. 获取最终资源状态:")
        final_avail = ray.available_resources()
        final_cpu_avail = final_avail.get("CPU", 0)

        print(f"   最终可用CPU: {final_cpu_avail}")

        if initial_cpu_total > 0:
            final_util = (initial_cpu_total - final_cpu_avail) / initial_cpu_total
            print(f"   最终CPU使用率: {final_util:.2%}")

        # 断开连接
        ray.shutdown()
        print("\n🔌 断开连接")

    except Exception as e:
        print(f"❌ 执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

        # 确保断开连接
        try:
            ray.shutdown()
        except:
            pass


if __name__ == "__main__":
    # 调试集群资源报告
    debug_ray_cluster_resources()

    # 模拟任务执行并检查资源变化
    # 注意：这个测试会实际执行任务，可能会花费一些时间
    # simulate_task_and_check_resources()

    print("\n" + "=" * 60)
    print("🎉 调试完成!")
    print("=" * 60)