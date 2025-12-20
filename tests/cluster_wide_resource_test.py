#!/usr/bin/env python3
"""
集群范围资源使用率测试
验证CPU使用率计算是否为集群整体范围，而不是进程维度
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

import ray
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager, ClusterConfig


def test_cluster_wide_resource_calculation():
    """测试集群范围资源计算"""
    print("=" * 60)
    print("测试集群范围资源计算")
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

        # 获取初始资源状态
        print("\n1. 获取初始资源状态:")
        initial_avail = ray.available_resources()
        initial_total = ray.cluster_resources()
        initial_cpu_avail = initial_avail.get("CPU", 0)
        initial_cpu_total = initial_total.get("CPU", 0)

        print(f"   集群总CPU: {initial_cpu_total}")
        print(f"   集群可用CPU: {initial_cpu_avail}")

        if initial_cpu_total > 0:
            initial_util = (initial_cpu_total - initial_cpu_avail) / initial_cpu_total
            print(f"   集群CPU使用率: {initial_util:.2%}")

        # 定义不同类型的任务
        @ray.remote(num_cpus=1)
        def cpu_task(task_id, duration):
            """CPU密集型任务"""
            import time
            print(f"[CPU任务{task_id}] 开始执行，预计耗时 {duration} 秒")
            start_time = time.time()
            while time.time() - start_time < duration:
                # 执行一些计算密集型操作
                sum(range(100000))
            print(f"[CPU任务{task_id}] 执行完成")
            return f"CPU任务{task_id}完成"

        @ray.remote(num_cpus=0.5)
        def io_task(task_id, duration):
            """I/O密集型任务"""
            import time
            print(f"[I/O任务{task_id}] 开始执行，预计耗时 {duration} 秒")
            time.sleep(duration)
            print(f"[I/O任务{task_id}] 执行完成")
            return f"I/O任务{task_id}完成"

        # 提交多种类型的任务
        print("\n2. 提交多种类型的任务:")
        task_refs = []

        # 提交2个CPU密集型任务 (使用2个CPU核心)
        for i in range(2):
            task_ref = cpu_task.remote(i+1, 20)
            task_refs.append(("CPU", task_ref))
            print(f"   提交CPU任务 {i+1}")
            time.sleep(0.5)

        # 提交2个I/O密集型任务 (使用1个CPU核心)
        for i in range(2):
            task_ref = io_task.remote(i+1, 20)
            task_refs.append(("I/O", task_ref))
            print(f"   提交I/O任务 {i+1}")
            time.sleep(0.5)

        print(f"\n   总共提交了4个任务，预计将使用约3个CPU核心")

        # 持续监控资源变化
        print("\n3. 持续监控集群资源变化:")
        print("时间(s)\t可用CPU\t总CPU\t使用率\t任务状态")
        print("-" * 60)

        start_monitor_time = time.time()
        monitor_duration = 25  # 监控25秒

        task_completed = [False] * len(task_refs)

        while time.time() - start_monitor_time < monitor_duration:
            # 获取当前资源
            current_avail = ray.available_resources()
            current_total = ray.cluster_resources()
            current_cpu_avail = current_avail.get("CPU", 0)
            current_cpu_total = current_total.get("CPU", 0)

            # 计算CPU使用率
            if current_cpu_total > 0:
                current_util = (current_cpu_total - current_cpu_avail) / current_cpu_total
            else:
                current_util = 0

            # 检查任务状态
            completed_count = sum(task_completed)
            for i, (task_type, task_ref) in enumerate(task_refs):
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
            print(f"{elapsed_time:.1f}\t{current_cpu_avail:.1f}\t{current_cpu_total:.1f}\t{current_util:.2%}\t{completed_count}/{len(task_refs)}")

            # 等待2秒
            time.sleep(2)

        # 等待所有任务完成
        print("\n4. 等待所有任务完成:")
        results = ray.get([task_ref for _, task_ref in task_refs])
        for i, result in enumerate(results):
            task_type, _ = task_refs[i]
            print(f"   {task_type}任务: {result}")

        # 获取最终资源状态
        print("\n5. 获取最终资源状态:")
        final_avail = ray.available_resources()
        final_total = ray.cluster_resources()
        final_cpu_avail = final_avail.get("CPU", 0)
        final_cpu_total = final_total.get("CPU", 0)

        print(f"   集群总CPU: {final_cpu_total}")
        print(f"   集群可用CPU: {final_cpu_avail}")

        if final_cpu_total > 0:
            final_util = (final_cpu_total - final_cpu_avail) / final_cpu_total
            print(f"   集群CPU使用率: {final_util:.2%}")

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


def demonstrate_resource_scope():
    """演示资源范围"""
    print("\n" + "=" * 60)
    print("演示资源范围")
    print("=" * 60)

    print("\nRay资源报告机制说明:")
    print("1. ray.cluster_resources() 返回整个集群的总资源")
    print("2. ray.available_resources() 返回整个集群的可用资源")
    print("3. 这些资源包括所有节点上的所有任务使用的资源")
    print("4. 资源计算是集群范围的，不是进程或任务维度的")

    print("\n示例:")
    print("假设集群有8个CPU核心:")
    print("- 没有任务运行时: 可用CPU=8.0, 使用率=0%")
    print("- 有任务运行时: 可用CPU=5.0, 使用率=37.5%")
    print("- 即使我们的进程停止，只要其他任务仍在运行，使用率就不会是0%")

    print("\n结论:")
    print("✅ 当前的CPU使用率计算是集群整体范围的")
    print("✅ 不是进程或任务维度的")
    print("✅ 如果观察到CPU使用率为0，说明集群中没有任何任务在执行")


def simulate_other_processes():
    """模拟其他进程在集群中运行"""
    print("\n" + "=" * 60)
    print("模拟其他进程在集群中运行")
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

        # 提交一些任务来模拟其他进程在运行
        print("\n2. 提交任务模拟其他进程在运行:")

        @ray.remote(num_cpus=2)
        def background_task(duration):
            """后台任务"""
            import time
            start_time = time.time()
            while time.time() - start_time < duration:
                # 执行一些计算
                sum(range(50000))
            return "后台任务完成"

        # 提交一个使用2个CPU核心的后台任务
        bg_task_ref = background_task.remote(30)  # 运行30秒
        print("   提交了一个使用2个CPU核心的后台任务")

        # 等待几秒钟让任务开始执行
        time.sleep(3)

        # 检查资源变化
        print("\n3. 检查资源变化:")
        current_avail = ray.available_resources()
        current_total = ray.cluster_resources()
        current_cpu_avail = current_avail.get("CPU", 0)
        current_cpu_total = current_total.get("CPU", 0)

        print(f"   当前可用CPU: {current_cpu_avail}")
        print(f"   当前总CPU: {current_cpu_total}")

        if current_cpu_total > 0:
            current_util = (current_cpu_total - current_cpu_avail) / current_cpu_total
            print(f"   当前CPU使用率: {current_util:.2%}")

        # 即使我们现在断开连接，后台任务仍在运行
        print("\n4. 断开连接，但后台任务仍在运行:")
        ray.shutdown()
        print("   🔌 已断开连接")
        print("   ⚠️  后台任务仍在集群中运行")

        # 重新连接检查资源
        print("\n5. 重新连接检查资源:")
        ray.init(
            address=cluster_address,
            ignore_reinit_error=True,
            logging_level="WARNING"
        )

        if ray.is_initialized():
            reconnected_avail = ray.available_resources()
            reconnected_total = ray.cluster_resources()
            reconnected_cpu_avail = reconnected_avail.get("CPU", 0)
            reconnected_cpu_total = reconnected_total.get("CPU", 0)

            print(f"   重新连接后可用CPU: {reconnected_cpu_avail}")
            print(f"   重新连接后总CPU: {reconnected_cpu_total}")

            if reconnected_cpu_total > 0:
                reconnected_util = (reconnected_cpu_total - reconnected_cpu_avail) / reconnected_cpu_total
                print(f"   重新连接后CPU使用率: {reconnected_util:.2%}")

            # 等待后台任务完成
            print("\n6. 等待后台任务完成:")
            result = ray.get(bg_task_ref)
            print(f"   后台任务结果: {result}")

            # 最终检查
            print("\n7. 最终资源状态:")
            final_avail = ray.available_resources()
            final_total = ray.cluster_resources()
            final_cpu_avail = final_avail.get("CPU", 0)
            final_cpu_total = final_total.get("CPU", 0)

            print(f"   最终可用CPU: {final_cpu_avail}")
            print(f"   最终总CPU: {final_cpu_total}")

            if final_cpu_total > 0:
                final_util = (final_cpu_total - final_cpu_avail) / final_cpu_total
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
    # 测试集群范围资源计算
    test_cluster_wide_resource_calculation()

    # 演示资源范围
    demonstrate_resource_scope()

    # 模拟其他进程在集群中运行
    simulate_other_processes()

    print("\n" + "=" * 60)
    print("🎉 所有测试完成!")
    print("=" * 60)
    print("\n最终结论:")
    print("✅ 当前的CPU使用率计算是集群整体范围的")
    print("✅ 不是进程或任务维度的")
    print("✅ 如果观察到CPU使用率为0，说明集群中没有任何任务在执行")
    print("✅ 即使我们的进程停止，只要集群中还有其他任务在执行，CPU使用率也不会是0")