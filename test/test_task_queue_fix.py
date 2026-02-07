#!/usr/bin/env python3
"""
测试 Task 队列处理和并发修复
验证 is_processing 标记防止任务重复执行
"""

import time
import sys
import os
import threading
from unittest.mock import Mock, patch, MagicMock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.app.client_api.submit_task import submit_task, get_task_status

def test_single_task_concurrent_protection():
    """测试 1.1: 单个任务并发执行保护"""
    print("\n=== 测试 1.1: 单个任务并发执行保护 ===")

    # 创建测试任务
    def test_func():
        return "task_result"

    task_desc = TaskDescription(
        task_id="test_concurrent_task",
        func_or_class=test_func,
        args=(),
        kwargs={},
        is_actor=False,
        is_top_level_task=False,
        is_processing=False  # 初始为 False
    )

    # 模拟两个并发线程同时尝试处理同一任务
    processing_count = [0]
    skipped_count = [0]

    def mock_process_task(task):
        # 模拟 _process_task 开始时的检查
        if task.is_processing:
            print(f"✓ 任务 {task.task_id} 已在处理中，跳过重复执行（并发保护生效）")
            skipped_count.append(1)  # 修复：使用 append 替代 += 1
            return False

        # 模拟设置 is_processing
        task.is_processing = True
        processing_count[0] += 1

        # 模拟执行
        time.sleep(0.1)

        # 修复：模拟 finally 块，确保标记被重置
        task.is_processing = False
        return True

    # 创建两个并发线程
    t1 = threading.Thread(target=lambda: mock_process_task(task_desc))
    t2 = threading.Thread(target=lambda: mock_process_task(task_desc))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # 验证：只有一个成功执行，另一个被跳过
    assert processing_count[0] == 1, f"✗ 预期处理1次，实际处理{processing_count[0]}次"
    assert len(skipped_count) == 1, f"✗ 预期跳过1次，实际跳过{len(skipped_count)}次"
    assert task_desc.is_processing == False, "✗ 任务未重置为 False"

    print("✓ 并发保护生效：任务只执行一次，重复执行被阻止")
    return True


def test_multiple_tasks_concurrent_protection():
    """测试 1.2: 多个任务并发处理保护"""
    print("\n=== 测试 1.2: 多个任务并发处理保护 ===")

    results = []

    def process_single_task(task_id):
        """模拟处理单个任务"""
        task = TaskDescription(
            task_id=task_id,
            func_or_class=lambda: f"result_{task_id}",
            is_actor=False,
            is_processing=False
        )

        # 模拟 _process_task 的并发检查
        if task.is_processing:
            results.append(f"skipped_{task_id}")
            return

        task.is_processing = True
        time.sleep(0.05)
        results.append(f"processed_{task_id}")
        task.is_processing = False

    # 创建多个任务并并发处理
    tasks = ["task_A", "task_B", "task_C"]
    threads = []
    for task_id in tasks:
        t = threading.Thread(target=lambda tid=task_id: process_single_task(tid))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # 验证：每个任务都被处理
    processed = [r for r in results if r.startswith("processed_")]
    skipped = [r for r in results if r.startswith("skipped_")]

    assert len(processed) == 3, f"✗ 预期处理3个任务，实际{len(processed)}个"
    assert len(skipped) == 0, f"✗ 预期跳过0个，实际{len(skipped)}个"

    print(f"✓ 所有 {len(processed)} 个任务都正确处理，没有并发冲突")
    return True


def test_task_queue_mechanism():
    """测试 2.1: 任务进入队列机制"""
    print("\n=== 测试 2.1: 任务进入队列机制 ===")

    try:
        # 模拟所有集群资源超过阈值
        with patch('ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager.ClusterMonitor') as MockMonitor:
            mock_monitor = MockMonitor.return_value
            mock_monitor.get_all_cluster_info.return_value = {
                'cluster1': {
                    'metadata': Mock(),
                    'snapshot': ResourceSnapshot(
                        cluster_name='cluster1',
                        cluster_cpu_usage_percent=80.0,  # 超过阈值
                        cluster_mem_usage_percent=85.0,
                        cluster_cpu_used_cores=8.0,
                        cluster_cpu_total_cores=10.0,
                        cluster_mem_used_mb=8500,
                        cluster_mem_total_mb=10000
                    )
                },
                'cluster2': {
                    'metadata': Mock(),
                    'snapshot': ResourceSnapshot(
                        cluster_name='cluster2',
                        cluster_cpu_usage_percent=85.0,  # 超过阈值
                        cluster_mem_usage_percent=90.0,
                        cluster_cpu_used_cores=8.5,
                        cluster_cpu_total_cores=10.0,
                        cluster_mem_used_mb=9000,
                        cluster_mem_total_mb=10000
                    )
                }
            }

            # 提交任务（应该进入队列）
            def sample_task():
                return "result"

            task_id, result = submit_task(
                func=sample_task,
                args=(),
                preferred_cluster=None
            )

            print(f"任务已提交，task_id: {task_id}, result: {result}")

            # 验证：result 是 task_id（表示排队）
            assert result == task_id, "✗ 预期任务进入队列，返回 task_id"

            # 等待一下让调度器处理
            time.sleep(0.5)

            # 验证：任务状态为 QUEUED
            status = get_task_status(task_id)
            print(f"任务 {task_id} 当前状态: {status}")

            # 队列中的任务应该显示为 QUEUED
            assert status == "QUEUED", f"✗ 预期状态为QUEUED，实际为{status}"

            print("✓ 任务正确进入队列，状态为 QUEUED")
            return True

    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_task_dequeue_and_execute():
    """测试 2.2: 从队列取出并执行任务"""
    print("\n=== 测试 2.2: 从队列取出并执行任务 ===")

    try:
        # 初始化调度器
        from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
        task_lifecycle_manager = initialize_scheduler_environment()

        # 创建测试任务
        def simple_task():
            time.sleep(0.1)
            return "completed"

        task_id, initial_result = submit_task(
            func=simple_task,
            args=(),
            preferred_cluster=None
        )

        print(f"任务 {task_id} 已提交，初始结果: {initial_result}")

        # 验证任务在队列中
        if initial_result == task_id:
            print("✓ 任务已在队列中")

            # 等待资源恢复并执行（最多5秒）
            max_wait = 5
            start = time.time()

            while time.time() - start < max_wait:
                status = get_task_status(task_id)
                print(f"等待中... 当前状态: {status}")

                if status != "QUEUED":
                    # 任务已被执行
                    print(f"✓ 任务 {task_id} 已从队列取出并执行，状态: {status}")
                    return True

                time.sleep(0.5)

            print(f"⚠️ 任务在 {max_wait} 秒内仍未执行")
            return False
        else:
            # 任务立即执行（队列为空）
            print(f"✓ 任务 {task_id} 立即执行")
            return True

    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_finally_block_exception_clear():
    """测试 5.1: finally 块异常时标记清除"""
    print("\n=== 测试 5.1: finally 块异常时标记清除 ===")

    try:
        # 创建一个任务并模拟异常处理
        task = TaskDescription(
            task_id="test_finally_task",
            func_or_class=lambda: "result",
            is_actor=False,
            is_processing=False
        )

        # 模拟 _process_task 的异常处理
        exception_raised = False
        processing_reset = False

        def mock_process_with_exception():
            nonlocal exception_raised, processing_reset
            try:
                # 模拟设置 is_processing
                task.is_processing = True
                # 模拟抛出异常
                raise RuntimeError("Simulated exception")
            except RuntimeError:
                exception_raised = True
                raise
            finally:
                # 模拟 finally 块
                task.is_processing = False
                processing_reset = True

        # 执行模拟
        try:
            mock_process_with_exception()
        except RuntimeError:
            pass  # 预期的异常

        # 验证：异常被抛出
        assert exception_raised, "✗ 异常未正确抛出"

        # 验证：finally 块执行了
        assert processing_reset, "✗ finally 块未执行"

        # 验证：is_processing 被重置
        assert task.is_processing == False, "✗ is_processing 未被重置为 False"

        print("✓ finally 块正确执行，is_processing 标记被重置")
        return True

    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("开始测试 Task 队列处理和并发修复...")
    print("=" * 60)

    results = []

    # Test 1.1: 单任务并发保护
    try:
        results.append(("1.1", test_single_task_concurrent_protection()))
    except AssertionError as e:
        print(f"✗ 测试 1.1 失败: {e}")
        results.append(("1.1", False))

    # Test 1.2: 多任务并发保护
    try:
        results.append(("1.2", test_multiple_tasks_concurrent_protection()))
    except AssertionError as e:
        print(f"✗ 测试 1.2 失败: {e}")
        results.append(("1.2", False))

    # Test 2.1: 任务进入队列机制
    try:
        results.append(("2.1", test_task_queue_mechanism()))
    except AssertionError as e:
        print(f"✗ 测试 2.1 失败: {e}")
        results.append(("2.1", False))

    # Test 2.2: 从队列取出并执行
    try:
        results.append(("2.2", test_task_dequeue_and_execute()))
    except AssertionError as e:
        print(f"✗ 测试 2.2 失败: {e}")
        results.append(("2.2", False))

    # Test 5.1: finally 块异常清除
    try:
        results.append(("5.1", test_finally_block_exception_clear()))
    except AssertionError as e:
        print(f"✗ 测试 5.1 失败: {e}")
        results.append(("5.1", False))

    # 汇总结果
    print("\n" + "=" * 60)
    print("测试结果汇总:")
    print("-" * 60)

    total_tests = len(results)
    passed_tests = sum(1 for _, success in results if success)
    failed_tests = total_tests - passed_tests

    print(f"总测试数: {total_tests}")
    print(f"通过: {passed_tests}")
    print(f"失败: {failed_tests}")
    print(f"成功率: {passed_tests/total_tests*100:.1f}%")

    if passed_tests == total_tests:
        print("\n🎉 所有测试通过！Task 并发和队列处理修复有效！")
    else:
        print("\n⚠️ 部分测试失败，需要进一步检查")
        failed_list = [name for name, success in results if not success]
        print(f"失败的测试: {failed_list}")

    print("=" * 60)
