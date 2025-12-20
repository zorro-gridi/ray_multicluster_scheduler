#!/usr/bin/env python3
"""
持续监控集群资源变化
通过持续监控来验证资源使用率计算的准确性
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

import ray


def continuous_resource_monitor():
    """持续监控集群资源变化"""
    print("=" * 60)
    print("持续监控集群资源变化")
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
        print("\n获取初始资源状态:")
        initial_avail = ray.available_resources()
        initial_total = ray.cluster_resources()
        initial_cpu_avail = initial_avail.get("CPU", 0)
        initial_cpu_total = initial_total.get("CPU", 0)

        print(f"初始可用CPU: {initial_cpu_avail}")
        print(f"初始总CPU: {initial_cpu_total}")

        if initial_cpu_total > 0:
            initial_util = (initial_cpu_total - initial_cpu_avail) / initial_cpu_total
            print(f"初始CPU使用率: {initial_util:.2%}")

        # 定义一个CPU密集型任务
        @ray.remote(num_cpus=1)
        def cpu_intensive_task(task_id, duration):
            """CPU密集型任务"""
            import time
            print(f"[任务{task_id}] 开始执行，预计耗时 {duration} 秒")
            start_time = time.time()
            counter = 0
            while time.time() - start_time < duration:
                # 执行一些计算密集型操作
                sum(range(100000))
                counter += 1
                # 每秒打印一次进度
                if counter % 100 == 0:
                    elapsed = time.time() - start_time
                    if elapsed >= 1 and int(elapsed) == int(time.time() - start_time - 1):
                        print(f"[任务{task_id}] 已执行 {int(elapsed)} 秒")
            print(f"[任务{task_id}] 执行完成")
            return f"任务{task_id}完成，执行时间: {duration}秒，循环次数: {counter}"

        # 提交多个任务以增加CPU负载
        print("\n提交多个CPU密集型任务...")
        task_refs = []
        num_tasks = 4  # 提交4个任务，使用4个CPU核心
        task_duration = 30  # 每个任务执行30秒

        for i in range(num_tasks):
            task_ref = cpu_intensive_task.remote(i+1, task_duration)
            task_refs.append(task_ref)
            print(f"任务 {i+1} 已提交")
            time.sleep(0.5)  # 稍微间隔提交任务

        print(f"\n总共提交了 {num_tasks} 个任务，预计执行时间 {task_duration} 秒")

        # 持续监控资源变化
        print("\n开始持续监控资源变化 (每2秒检查一次):")
        print("时间(s)\t可用CPU\t使用率\t任务状态")
        print("-" * 50)

        start_monitor_time = time.time()
        monitor_duration = task_duration + 10  # 监控比任务执行时间稍长一些

        task_completed = [False] * num_tasks

        while time.time() - start_monitor_time < monitor_duration:
            # 获取当前资源
            current_avail = ray.available_resources()
            current_cpu_avail = current_avail.get("CPU", 0)

            # 计算CPU使用率
            if initial_cpu_total > 0:
                current_util = (initial_cpu_total - current_cpu_avail) / initial_cpu_total
            else:
                current_util = 0

            # 检查任务状态
            completed_count = sum(task_completed)
            for i, task_ref in enumerate(task_refs):
                if not task_completed[i]:
                    try:
                        # 非阻塞检查任务是否完成
                        ready, _ = ray.wait([task_ref], timeout=0)
                        if ready:
                            task_completed[i] = True
                            completed_count += 1
                    except:
                        pass

            # 计算经过的时间
            elapsed_time = time.time() - start_monitor_time

            # 打印监控信息
            print(f"{elapsed_time:.1f}\t{current_cpu_avail:.1f}\t{current_util:.2%}\t{completed_count}/{num_tasks}")

            # 等待2秒
            time.sleep(2)

        # 获取任务结果
        print("\n获取任务结果:")
        results = ray.get(task_refs)
        for i, result in enumerate(results):
            print(f"任务 {i+1}: {result}")

        # 获取最终资源
        print("\n最终资源状态:")
        final_avail = ray.available_resources()
        final_cpu_avail = final_avail.get("CPU", 0)

        print(f"最终可用CPU: {final_cpu_avail}")

        if initial_cpu_total > 0:
            final_util = (initial_cpu_total - final_cpu_avail) / initial_cpu_total
            print(f"最终CPU使用率: {final_util:.2%}")

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


def check_cluster_node_resources():
    """检查集群节点级别的资源"""
    print("\n" + "=" * 60)
    print("检查集群节点级别的资源")
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

        # 获取节点信息
        nodes = ray.nodes()
        print(f"\n集群共有 {len(nodes)} 个节点:")

        total_cluster_cpu = 0
        available_cluster_cpu = 0

        for i, node in enumerate(nodes):
            print(f"\n节点 {i+1}:")
            print(f"  节点ID: {node.get('NodeID', 'N/A')}")
            print(f"  状态: {'存活' if node.get('Alive', False) else '离线'}")

            resources = node.get('Resources', {})
            cpu_total = resources.get('CPU', 0)
            total_cluster_cpu += cpu_total

            # 获取可用资源
            avail_resources = ray.available_resources()
            # 这里需要更精确地获取每个节点的可用资源

            print(f"  CPU资源: {cpu_total}")
            print(f"  其他资源: {resources}")

        print(f"\n集群总CPU: {total_cluster_cpu}")

        # 获取总的可用资源
        total_avail_resources = ray.available_resources()
        total_cpu_avail = total_avail_resources.get("CPU", 0)
        available_cluster_cpu = total_cpu_avail

        print(f"集群可用CPU: {available_cluster_cpu}")

        if total_cluster_cpu > 0:
            cluster_util = (total_cluster_cpu - available_cluster_cpu) / total_cluster_cpu
            print(f"集群CPU使用率: {cluster_util:.2%}")

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
    # 持续监控资源变化
    continuous_resource_monitor()

    # 检查集群节点资源
    check_cluster_node_resources()

    print("\n" + "=" * 60)
    print("🎉 监控完成!")
    print("=" * 60)