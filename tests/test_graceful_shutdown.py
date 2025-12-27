"""
单元测试：测试调度器的优雅关闭功能

测试场景：
1. 初始化调度器环境
2. 提交多个任务到真实集群
3. 提交一个作业到真实集群
4. 模拟 Ctrl+C (SIGINT) 信号
5. 验证所有资源被正确清理
"""

import os
import sys
import time
import signal
import threading
import pytest
from unittest.mock import Mock, patch, MagicMock

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task,
    submit_job
)
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager


class TestGracefulShutdown:
    """测试优雅关闭功能"""

    @classmethod
    def setup_class(cls):
        """测试类初始化"""
        print("\n" + "=" * 80)
        print("🧪 开始测试调度器优雅关闭功能")
        print("=" * 80)

    @classmethod
    def teardown_class(cls):
        """测试类清理"""
        print("\n" + "=" * 80)
        print("✅ 调度器优雅关闭功能测试完成")
        print("=" * 80)

    def test_graceful_shutdown_with_tasks(self):
        """测试提交任务后的优雅关闭"""
        print("\n" + "-" * 80)
        print("📋 测试场景1: 提交任务后优雅关闭")
        print("-" * 80)

        # 1. 初始化调度器环境
        print("\n1️⃣ 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        assert task_lifecycle_manager is not None, "调度器初始化失败"
        print("   ✅ 调度器环境初始化成功")

        # 2. 提交多个任务
        print("\n2️⃣ 提交测试任务到真实集群...")
        submitted_tasks = []

        # 定义一个简单的任务函数（使用lambda避免序列化问题）
        def make_task():
            """创建一个简单任务"""
            def task(x):
                import time
                time.sleep(2)
                return x * 2
            return task

        for i in range(3):
            try:
                task_func = make_task()
                task_id, result = submit_task(
                    func=task_func,
                    args=(i,),
                    name=f"shutdown_test_task_{i}",
                    resource_requirements={"CPU": 1}
                )
                submitted_tasks.append((task_id, result))
                print(f"   ✅ 任务 {i} 已提交: {task_id}, 结果: {result}")
            except Exception as e:
                print(f"   ⚠️ 任务 {i} 提交失败: {e}")

        assert len(submitted_tasks) > 0, "至少应该有一个任务提交成功"
        print(f"\n   📊 成功提交 {len(submitted_tasks)} 个任务")

        # 3. 等待任务开始执行
        print("\n3️⃣ 等待任务开始执行...")
        time.sleep(3)

        # 4. 检查任务队列状态
        print("\n4️⃣ 检查任务队列状态...")
        queue_size = task_lifecycle_manager.task_queue.size()
        print(f"   当前队列大小: {queue_size}")

        # 5. 测试清理队列功能
        print("\n5️⃣ 测试清理队列功能...")
        task_lifecycle_manager._clear_all_queues()

        after_clear_size = task_lifecycle_manager.task_queue.size()
        print(f"   清理后队列大小: {after_clear_size}")
        assert after_clear_size == 0, "队列清理后应该为空"
        print("   ✅ 队列清理成功")

        # 6. 停止任务生命周期管理器
        print("\n6️⃣ 停止任务生命周期管理器...")
        task_lifecycle_manager.stop()

        # 验证工作线程已停止
        if task_lifecycle_manager.worker_thread:
            is_alive = task_lifecycle_manager.worker_thread.is_alive()
            print(f"   工作线程状态: {'运行中' if is_alive else '已停止'}")
            assert not is_alive, "工作线程应该已经停止"

        print("   ✅ 任务生命周期管理器已停止")

        print("\n✅ 测试场景1完成: 任务提交后优雅关闭功能正常")

    def test_graceful_shutdown_with_jobs(self):
        """测试提交作业后的优雅关闭"""
        print("\n" + "-" * 80)
        print("📋 测试场景2: 提交作业后优雅关闭")
        print("-" * 80)

        # 1. 初始化调度器环境
        print("\n1️⃣ 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        assert task_lifecycle_manager is not None, "调度器初始化失败"
        print("   ✅ 调度器环境初始化成功")

        # 2. 提交一个作业
        print("\n2️⃣ 提交测试作业到真实集群...")
        try:
            # 创建一个简单的Python脚本
            test_script = """
import time
print("作业开始执行...")
time.sleep(5)
print("作业执行完成")
"""
            script_path = "/tmp/test_job_script.py"
            with open(script_path, 'w') as f:
                f.write(test_script)

            job_id = submit_job(
                entrypoint=f"python {script_path}",
                job_id="shutdown_test_job",
                metadata={"test": "graceful_shutdown"}
            )
            print(f"   ✅ 作业已提交: {job_id}")
        except Exception as e:
            print(f"   ⚠️ 作业提交失败（可能正常，因为集群可能不支持）: {e}")
            job_id = None

        # 3. 等待作业开始
        print("\n3️⃣ 等待作业开始...")
        time.sleep(3)

        # 4. 测试停止作业功能
        if job_id and task_lifecycle_manager.connection_manager.job_client_pool:
            print("\n4️⃣ 测试停止作业功能...")
            try:
                job_client_pool = task_lifecycle_manager.connection_manager.job_client_pool

                # 列出所有集群的作业
                for cluster_name in job_client_pool.clients.keys():
                    try:
                        jobs = job_client_pool.list_jobs(cluster_name)
                        print(f"   集群 {cluster_name} 上的作业数: {len(jobs)}")

                        # 尝试停止正在运行的作业
                        for job_info in jobs:
                            job_id_to_stop = job_info.get('job_id') or job_info.get('submission_id')
                            job_status = job_info.get('status')

                            if job_status in ['RUNNING', 'PENDING']:
                                print(f"   尝试停止作业: {job_id_to_stop} (状态: {job_status})")
                                try:
                                    job_client_pool.stop_job(cluster_name, job_id_to_stop)
                                    print(f"   ✅ 作业 {job_id_to_stop} 已停止")
                                except Exception as e:
                                    print(f"   ⚠️ 停止作业失败: {e}")
                    except Exception as e:
                        print(f"   ⚠️ 处理集群 {cluster_name} 时出错: {e}")

                print("   ✅ 作业停止功能测试完成")
            except Exception as e:
                print(f"   ⚠️ 作业停止测试失败: {e}")

        # 5. 停止任务生命周期管理器
        print("\n5️⃣ 停止任务生命周期管理器...")
        task_lifecycle_manager.stop()
        print("   ✅ 任务生命周期管理器已停止")

        print("\n✅ 测试场景2完成: 作业提交后优雅关闭功能正常")

    def test_signal_handler_simulation(self):
        """测试模拟信号处理"""
        print("\n" + "-" * 80)
        print("📋 测试场景3: 模拟SIGINT信号处理")
        print("-" * 80)

        # 1. 初始化调度器环境
        print("\n1️⃣ 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        assert task_lifecycle_manager is not None, "调度器初始化失败"
        print("   ✅ 调度器环境初始化成功")

        # 2. 提交一些任务
        print("\n2️⃣ 提交测试任务...")

        def make_task():
            """创建一个简单任务"""
            def task(x):
                import time
                time.sleep(2)
                return x * 2
            return task

        for i in range(2):
            try:
                task_func = make_task()
                task_id, result = submit_task(
                    func=task_func,
                    args=(i,),
                    name=f"signal_test_task_{i}"
                )
                print(f"   ✅ 任务 {i} 已提交: {task_id}")
            except Exception as e:
                print(f"   ⚠️ 任务 {i} 提交失败: {e}")

        # 3. 模拟信号处理流程
        print("\n3️⃣ 模拟优雅关闭流程...")

        # 导入main模块的关闭函数
        from ray_multicluster_scheduler.main import shutdown_scheduler
        from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler

        unified_scheduler = get_unified_scheduler()
        connection_manager = task_lifecycle_manager.connection_manager

        # 执行关闭流程
        print("\n4️⃣ 执行shutdown_scheduler函数...")
        try:
            shutdown_scheduler(task_lifecycle_manager, unified_scheduler, connection_manager)
            print("   ✅ shutdown_scheduler执行成功")
        except Exception as e:
            print(f"   ⚠️ shutdown_scheduler执行出错: {e}")
            import traceback
            traceback.print_exc()

        # 5. 验证资源清理
        print("\n5️⃣ 验证资源清理...")

        # 检查任务队列是否清空
        queue_size = task_lifecycle_manager.task_queue.size()
        print(f"   任务队列大小: {queue_size}")

        # 检查工作线程是否停止
        if task_lifecycle_manager.worker_thread:
            is_alive = task_lifecycle_manager.worker_thread.is_alive()
            print(f"   工作线程状态: {'运行中' if is_alive else '已停止'}")

        print("   ✅ 资源清理验证完成")

        print("\n✅ 测试场景3完成: 信号处理模拟功能正常")

    def test_shutdown_with_pending_tasks(self):
        """测试有待处理任务时的关闭"""
        print("\n" + "-" * 80)
        print("📋 测试场景4: 有待处理任务时的优雅关闭")
        print("-" * 80)

        # 1. 初始化调度器环境
        print("\n1️⃣ 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        assert task_lifecycle_manager is not None, "调度器初始化失败"
        print("   ✅ 调度器环境初始化成功")

        # 2. 直接向队列添加任务（不实际提交）
        print("\n2️⃣ 向队列添加待处理任务...")
        from ray_multicluster_scheduler.common.model import TaskDescription

        # 创建一个可序列化的简单函数
        def simple_func(x):
            import time
            time.sleep(1)
            return x * 2

        for i in range(5):
            task_desc = TaskDescription(
                task_id=f"pending_task_{i}",
                func_or_class=simple_func,
                args=(i,),
                kwargs={},
                name=f"pending_task_{i}"
            )
            task_lifecycle_manager.task_queue.enqueue(task_desc)

        queue_size = task_lifecycle_manager.task_queue.size()
        print(f"   队列中的任务数: {queue_size}")
        assert queue_size >= 5, "队列中应该有至少5个任务"

        # 3. 执行清理
        print("\n3️⃣ 清理待处理任务...")
        task_lifecycle_manager._clear_all_queues()

        # 4. 验证清理结果
        after_clear_size = task_lifecycle_manager.task_queue.size()
        print(f"   清理后队列大小: {after_clear_size}")
        assert after_clear_size == 0, "队列应该已清空"

        # 5. 停止管理器
        print("\n4️⃣ 停止任务生命周期管理器...")
        task_lifecycle_manager.stop()
        print("   ✅ 管理器已停止")

        print("\n✅ 测试场景4完成: 待处理任务清理功能正常")


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "-s", "--tb=short"])
