#!/usr/bin/env python3
"""
并发Actor执行测试用例
演示如何使用submit_actor统一调度接口实现10个并发任务执行
"""

import sys
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

import ray
from ray import actor
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    UnifiedScheduler,
    initialize_scheduler_environment,
    submit_actor,
    submit_task
)


# 定义一个简单的Actor类用于测试
@ray.remote
class TestActor:
    """测试Actor类"""

    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.execution_count = 0

    def process_task(self, task_data):
        """处理任务方法"""
        # 模拟任务处理时间
        time.sleep(0.5)
        self.execution_count += 1
        result = f"Actor {self.actor_id} processed task {task_data} (count: {self.execution_count})"
        print(result)
        return result

    def get_execution_count(self):
        """获取执行次数"""
        return self.execution_count

    def get_actor_info(self):
        """获取Actor信息"""
        return f"Actor {self.actor_id}"


def test_concurrent_actor_execution():
    """测试并发Actor执行"""
    print("=" * 60)
    print("测试并发Actor执行")
    print("=" * 60)

    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")

        # 2. 创建10个并发Actor
        print("\n2. 创建10个并发Actor...")
        actors = []
        actor_futures = []

        for i in range(10):
            print(f"  创建第 {i+1} 个Actor...")
            actor_id, actor_instance = submit_actor(
                actor_class=TestActor,
                args=(f"Actor-{i}",),
                kwargs={},
                resource_requirements={"CPU": 0.5},  # 每个Actor需要0.5个CPU核心
                tags=["test", "concurrent"],
                name=f"ConcurrentTestActor-{i}",
                preferred_cluster=None  # 让调度器自动选择集群
            )

            actors.append((actor_id, actor_instance))
            actor_futures.append(actor_instance)
            print(f"  ✅ Actor {i} 创建成功，ID: {actor_id}")

        print(f"\n✅ 成功创建 {len(actors)} 个Actor")

        # 3. 并发执行任务
        print("\n3. 并发执行任务...")
        task_results = []

        # 为每个Actor提交多个任务
        for i, (actor_id, actor_instance) in enumerate(actors):
            for j in range(3):  # 每个Actor执行3个任务
                print(f"  为Actor {i} 提交任务 {j+1}...")
                # 使用Actor的方法执行任务
                result = actor_instance.process_task.remote(f"Task-{i}-{j}")
                task_results.append((f"Actor-{i}-Task-{j}", result))

        print(f"\n✅ 已提交 {len(task_results)} 个任务")

        # 4. 收集任务结果
        print("\n4. 收集任务结果...")
        completed_tasks = 0

        for task_name, future in task_results:
            try:
                result = ray.get(future, timeout=10)  # 设置10秒超时
                print(f"  ✅ {task_name} 完成: {result}")
                completed_tasks += 1
            except Exception as e:
                print(f"  ❌ {task_name} 失败: {e}")

        print(f"\n✅ 成功完成 {completed_tasks}/{len(task_results)} 个任务")

        # 5. 验证Actor状态
        print("\n5. 验证Actor状态...")
        for i, (actor_id, actor_instance) in enumerate(actors):
            try:
                execution_count = ray.get(actor_instance.get_execution_count.remote(), timeout=5)
                actor_info = ray.get(actor_instance.get_actor_info.remote(), timeout=5)
                print(f"  Actor {i}: {actor_info}, 执行次数: {execution_count}")
            except Exception as e:
                print(f"  ❌ 无法获取Actor {i} 状态: {e}")

        # 6. 清理资源
        print("\n6. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")

        print("\n🎉 并发Actor执行测试完成!")
        return True

    except Exception as e:
        print(f"❌ 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()

        # 尝试清理资源
        try:
            from ray_multicluster_scheduler.app.client_api.submit_actor import _task_lifecycle_manager
            if _task_lifecycle_manager:
                _task_lifecycle_manager.stop()
                print("✅ 任务生命周期管理器已停止")
        except:
            pass

        return False


def test_concurrent_actor_with_threadpool():
    """使用线程池测试并发Actor执行"""
    print("\n" + "=" * 60)
    print("使用线程池测试并发Actor执行")
    print("=" * 60)

    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")

        # 2. 使用线程池并发创建Actor
        print("\n2. 使用线程池并发创建Actor...")
        actors = []

        def create_actor(i):
            """创建单个Actor"""
            actor_id, actor_instance = submit_actor(
                actor_class=TestActor,
                args=(f"ThreadPoolActor-{i}",),
                kwargs={},
                resource_requirements={"CPU": 0.3},
                tags=["test", "threadpool"],
                name=f"ThreadPoolActor-{i}",
                preferred_cluster=None
            )
            return i, actor_id, actor_instance

        # 使用ThreadPoolExecutor并发创建10个Actor
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有创建Actor的任务
            future_to_index = {executor.submit(create_actor, i): i for i in range(10)}

            # 收集结果
            for future in as_completed(future_to_index):
                try:
                    i, actor_id, actor_instance = future.result(timeout=30)
                    actors.append((actor_id, actor_instance))
                    print(f"  ✅ Actor {i} 创建成功，ID: {actor_id}")
                except Exception as e:
                    index = future_to_index[future]
                    print(f"  ❌ Actor {index} 创建失败: {e}")

        print(f"\n✅ 成功创建 {len(actors)} 个Actor")

        # 3. 并发执行任务
        print("\n3. 并发执行任务...")
        all_results = []

        def execute_actor_task(actor_index, actor_instance, task_index):
            """执行Actor任务"""
            result = actor_instance.process_task.remote(f"ThreadPoolTask-{actor_index}-{task_index}")
            return actor_index, task_index, result

        # 使用ThreadPoolExecutor并发执行任务
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 为每个Actor提交2个任务
            futures = []
            for i, (actor_id, actor_instance) in enumerate(actors):
                for j in range(2):
                    future = executor.submit(execute_actor_task, i, actor_instance, j)
                    futures.append(future)

            # 收集任务结果
            for future in as_completed(futures):
                try:
                    actor_index, task_index, result_future = future.result(timeout=30)
                    result = ray.get(result_future, timeout=10)
                    all_results.append(result)
                    print(f"  ✅ Actor {actor_index} 任务 {task_index} 完成")
                except Exception as e:
                    print(f"  ❌ 任务执行失败: {e}")

        print(f"\n✅ 成功执行 {len(all_results)} 个任务")

        # 4. 清理资源
        print("\n4. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")

        print("\n🎉 线程池并发Actor执行测试完成!")
        return True

    except Exception as e:
        print(f"❌ 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def demonstrate_actor_load_distribution():
    """演示Actor负载分布"""
    print("\n" + "=" * 60)
    print("演示Actor负载分布")
    print("=" * 60)

    try:
        # 1. 初始化调度器环境
        print("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ 调度器环境初始化成功")

        # 2. 获取集群信息
        cluster_monitor = task_lifecycle_manager.cluster_monitor
        cluster_monitor.refresh_resource_snapshots(force=True)
        cluster_info = cluster_monitor.get_all_cluster_info()

        print(f"\n发现 {len(cluster_info)} 个集群:")
        for name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']
            print(f"  集群 [{name}]: 偏好={metadata.prefer}, 地址={metadata.head_address}")
            if snapshot:
                cpu_free = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                print(f"    CPU资源: {cpu_free}/{cpu_total}")

        # 3. 创建多个Actor观察分布情况
        print("\n2. 创建多个Actor观察分布情况...")
        actors = []

        for i in range(8):
            actor_id, actor_instance = submit_actor(
                actor_class=TestActor,
                args=(f"DistributedActor-{i}",),
                kwargs={},
                resource_requirements={"CPU": 0.5},
                tags=["test", "distribution"],
                name=f"DistributedActor-{i}",
                preferred_cluster=None  # 让调度器自动分配
            )
            actors.append((actor_id, actor_instance))
            print(f"  ✅ DistributedActor-{i} 创建成功")
            time.sleep(0.1)  # 短暂延迟以观察调度行为

        # 4. 执行任务验证
        print("\n3. 执行任务验证...")
        results = []
        for i, (actor_id, actor_instance) in enumerate(actors[:5]):  # 只测试前5个
            result = actor_instance.get_actor_info.remote()
            results.append(ray.get(result, timeout=5))

        print("Actor信息:")
        for result in results:
            print(f"  {result}")

        # 5. 清理资源
        print("\n4. 清理资源...")
        if task_lifecycle_manager:
            task_lifecycle_manager.stop()
            print("✅ 任务生命周期管理器已停止")

        print("\n🎉 Actor负载分布演示完成!")
        return True

    except Exception as e:
        print(f"❌ 演示过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # 运行并发Actor执行测试
    success1 = test_concurrent_actor_execution()

    # 运行线程池并发Actor执行测试
    success2 = test_concurrent_actor_with_threadpool()

    # 运行Actor负载分布演示
    success3 = demonstrate_actor_load_distribution()

    print("\n" + "=" * 60)
    if success1 and success2 and success3:
        print("🎉 所有测试通过!")
    else:
        print("⚠️  部分测试失败，请检查上述错误信息")
    print("=" * 60)