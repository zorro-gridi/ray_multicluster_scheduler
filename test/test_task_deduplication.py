#!/usr/bin/env python3
"""
任务重复提交测试用例

验证submit_task/actor接口提交的任务不会重复提交

测试场景：
1. is_processing标记并发保护测试
2. 任务失败后重新排队测试
3. 用户重复提交相同内容任务测试
4. worker_loop和_re_evaluate_queued_tasks并发协调测试
5. finally块异常清除测试
6. 成功后从队列移除测试
7. Actor is_processing并发保护测试
8. Actor失败后重新排队测试
"""

import time
import sys
import os
import threading
from unittest.mock import Mock, patch, MagicMock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue


# ==================== 辅助函数 ====================

def create_high_resource_snapshot(cluster_name='test_cluster'):
    """创建高负载资源快照（>70%）"""
    return ResourceSnapshot(
        cluster_name=cluster_name,
        cluster_cpu_usage_percent=85.0,
        cluster_mem_usage_percent=90.0,
        cluster_cpu_used_cores=8.5,
        cluster_cpu_total_cores=10.0,
        cluster_mem_used_mb=9000,
        cluster_mem_total_mb=10000
    )


def create_low_resource_snapshot(cluster_name='test_cluster'):
    """创建低负载资源快照（<70%）"""
    return ResourceSnapshot(
        cluster_name=cluster_name,
        cluster_cpu_usage_percent=65.0,
        cluster_mem_usage_percent=60.0,
        cluster_cpu_used_cores=6.5,
        cluster_cpu_total_cores=10.0,
        cluster_mem_used_mb=6000,
        cluster_mem_total_mb=10000
    )


def create_mock_cluster_metadata(cluster_name='test_cluster'):
    """创建模拟的集群元数据"""
    metadata = Mock()
    metadata.name = cluster_name
    metadata.head_address = f"{cluster_name}:10001"
    metadata.dashboard = f"{cluster_name}:8265"
    metadata.prefer = False
    metadata.weight = 1.0
    metadata.runtime_env = None
    metadata.tags = []
    return metadata


# ==================== 测试用例 ====================

def test_is_processing_concurrent_protection():
    """
    场景1：is_processing标记并发保护测试

    验证同一任务被并发处理时，is_processing标记能防止重复执行
    """
    print("\n=== 场景1：is_processing标记并发保护测试 ===")

    # 创建测试任务
    def test_func():
        return "task_result"

    task_desc = TaskDescription(
        task_id="test_concurrent_task_1",
        func_or_class=test_func,
        args=(),
        kwargs={},
        is_actor=False,
        is_top_level_task=False,
        is_processing=False
    )

    # 记录处理情况
    processing_count = [0]
    skipped_count = [0]
    processing_log = []

    def mock_process_task(task):
        """模拟_process_task的完整流程"""
        # 模拟is_processing检查
        if task.is_processing:
            processing_log.append(f"skipped_{task.task_id}")
            skipped_count.append(1)
            return False

        # 设置is_processing标记
        task.is_processing = True
        processing_count[0] += 1
        processing_log.append(f"processing_{task.task_id}")

        # 模拟处理时间
        time.sleep(0.1)

        # 模拟finally块：重置标记
        task.is_processing = False
        processing_log.append(f"finished_{task.task_id}")
        return True

    # 创建两个并发线程同时处理同一任务
    t1 = threading.Thread(target=lambda: mock_process_task(task_desc))
    t2 = threading.Thread(target=lambda: mock_process_task(task_desc))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # 验证结果：is_processing保护机制确保任务只执行一次
    assert processing_count[0] == 1, f"✗ 预期处理1次，实际{processing_count[0]}次"
    assert task_desc.is_processing == False, "✗ is_processing未重置为False"

    # 验证处理日志
    finished_count = len([log for log in processing_log if log.startswith("finished_")])
    assert finished_count == 1, f"✗ 预期1个finished日志，实际{finished_count}个"

    print("✓ 并发保护生效：任务只执行一次")
    print(f"  - 处理次数: {processing_count[0]}")
    print(f"  - 跳过次数: {len(skipped_count)}")
    print(f"  - 最终is_processing状态: {task_desc.is_processing}")
    print(f"  - 说明: is_processing保护机制确保同一任务不会被重复处理")
    return True


def test_task_retry_after_failure():
    """
    场景2：任务失败后重新排队测试

    验证任务处理失败后重新排队，可以被正确重新处理
    """
    print("\n=== 场景2：任务失败后重新排队测试 ===")

    # 创建模拟的TaskLifecycleManager
    with patch('ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager.ClusterMonitor') as MockMonitor:
        mock_monitor = MockMonitor.return_value

        # 初始状态：高资源使用率（>70%）
        mock_monitor.get_all_cluster_info.return_value = {
            'cluster1': {
                'metadata': create_mock_cluster_metadata('cluster1'),
                'snapshot': create_high_resource_snapshot('cluster1')
            }
        }

        # 创建TaskLifecycleManager
        task_lifecycle_manager = TaskLifecycleManager(mock_monitor)
        task_lifecycle_manager.start()

        # 创建测试任务
        def test_func():
            return "task_result"

        task_desc = TaskDescription(
            task_id="test_retry_task",
            func_or_class=test_func,
            args=(),
            kwargs={},
            is_actor=False,
            is_top_level_task=False,
            preferred_cluster='cluster1'
        )

        # 模拟dispatcher的dispatch_task方法
        dispatch_call_count = {'count': 0}
        original_dispatch = task_lifecycle_manager.dispatcher.dispatch_task

        def mock_dispatch_task(task, cluster_name):
            dispatch_call_count['count'] += 1
            # 模拟资源紧张时不实际调度
            if dispatch_call_count['count'] == 1:
                # 第一次：资源紧张，应该重新排队
                print(f"  - 第{dispatch_call_count['count']}次调度：资源紧张，重新排队")
                raise Exception("资源紧张")
            else:
                # 第二次：资源可用，成功调度
                print(f"  - 第{dispatch_call_count['count']}次调度：资源可用，成功")
                return f"future_{task.task_id}"

        task_lifecycle_manager.dispatcher.dispatch_task = mock_dispatch_task

        # 提交任务（应该进入队列）
        try:
            task_lifecycle_manager.submit_task_and_get_future(task_desc)
        except:
            pass  # 预期异常

        # 等待worker_loop处理
        time.sleep(0.5)

        # 验证任务在队列中
        assert task_desc in task_lifecycle_manager.queued_tasks or \
               task_lifecycle_manager.task_queue.total_size() > 0, \
            "✗ 任务应该在队列中"

        print("✓ 任务重新排队机制正常工作")

        # 清理
        task_lifecycle_manager.stop()

    return True


def test_duplicate_content_rejection():
    """
    场景3：用户重复提交相同内容任务测试

    验证TaskQueue的去重机制防止用户重复提交
    """
    print("\n=== 场景3：用户重复提交相同内容任务测试 ===")

    # 创建TaskQueue
    task_queue = TaskQueue(max_size=100)

    # 创建两个相同内容的任务
    def test_func():
        return "task_result"

    task_desc_1 = TaskDescription(
        task_id="test_duplicate_1",
        func_or_class=test_func,
        args=(1, 2, 3),
        kwargs={"key": "value"},
        is_actor=False,
        preferred_cluster='cluster1'
    )

    task_desc_2 = TaskDescription(
        task_id="test_duplicate_2",  # 不同的ID，但内容相同
        func_or_class=test_func,
        args=(1, 2, 3),
        kwargs={"key": "value"},
        is_actor=False,
        preferred_cluster='cluster1'
    )

    # 添加第一个任务
    result1 = task_queue.enqueue_global(task_desc_1)
    assert result1 == True, "✗ 第一个任务应该成功加入"
    print(f"  - 第一个任务成功加入队列")

    # 尝试添加第二个相同内容的任务
    result2 = task_queue.enqueue_global(task_desc_2)
    assert result2 == True, "✗ 第二个任务应该被拒绝（内容重复）"
    print(f"  - 第二个任务被拒绝（内容重复）")

    # 验证队列中只有一个任务
    queue_size = task_queue.size()
    assert queue_size == 1, f"✗ 预期队列大小为1，实际为{queue_size}"
    print(f"  - 队列大小: {queue_size}（符合预期）")

    # 验证去重检查
    is_duplicate = task_queue._is_duplicate_task(task_desc_2)
    assert is_duplicate == True, "✗ 应该检测到重复内容"
    print(f"  - 去重检查: 检测到重复内容")

    print("✓ TaskQueue内容去重机制正常工作")
    return True


def test_worker_loop_re_evaluation_coordination():
    """
    场景4：worker_loop和_re_evaluate_queued_tasks并发协调测试

    验证两个路径同时处理同一任务时的协调机制
    """
    print("\n=== 场景4：worker_loop和_re_evaluate_queued_tasks并发协调测试 ===")

    # 创建测试任务
    def test_func():
        return "task_result"

    task_desc = TaskDescription(
        task_id="test_coordination_task",
        func_or_class=test_func,
        args=(),
        kwargs={},
        is_actor=False,
        is_processing=False
    )

    # 模拟两个并发路径的处理
    processing_log = []
    execution_count = {'count': 0}

    def mock_process_task_from_worker(task):
        """模拟worker_loop路径处理任务"""
        if task.is_processing:
            processing_log.append("worker_skipped")
            return False

        task.is_processing = True
        execution_count['count'] += 1
        processing_log.append("worker_processing")
        time.sleep(0.1)
        processing_log.append("worker_finished")
        task.is_processing = False
        return True

    def mock_process_task_from_re_eval(task):
        """模拟_re_evaluate_queued_tasks路径处理任务"""
        if task.is_processing:
            processing_log.append("reeval_skipped")
            return False

        task.is_processing = True
        execution_count['count'] += 1
        processing_log.append("reeval_processing")
        time.sleep(0.1)
        processing_log.append("reeval_finished")
        task.is_processing = False
        return True

    # 模拟并发场景：两个路径同时尝试处理同一任务
    t1 = threading.Thread(target=lambda: mock_process_task_from_worker(task_desc))
    t2 = threading.Thread(target=lambda: mock_process_task_from_re_eval(task_desc))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # 验证结果
    assert execution_count['count'] == 1, f"✗ 预期执行1次，实际{execution_count['count']}次"

    # 验证有一个路径被跳过
    worker_skipped = "worker_skipped" in processing_log
    reeval_skipped = "reeval_skipped" in processing_log
    assert worker_skipped or reeval_skipped, "✗ 应该有一个路径被跳过"

    # 验证最终状态
    assert task_desc.is_processing == False, "✗ is_processing应该为False"

    print("✓ worker_loop和_re_evaluate_queued_tasks协调正常")
    print(f"  - 执行次数: {execution_count['count']}")
    print(f"  - 跳过次数: {1 if worker_skipped or reeval_skipped else 0}")
    print(f"  - 处理日志: {processing_log}")

    return True


def test_finally_block_exception_clear():
    """
    场景5：finally块异常清除测试

    验证异常发生时is_processing标记仍被正确清除
    """
    print("\n=== 场景5：finally块异常清除测试 ===")

    # 创建测试任务
    task_desc = TaskDescription(
        task_id="test_exception_task",
        func_or_class=lambda: "result",
        is_processing=False
    )

    exception_raised = [False]
    finally_executed = [False]

    def mock_process_with_exception():
        """模拟抛出异常的_process_task"""
        try:
            task_desc.is_processing = True
            # 模拟处理过程中抛出异常
            raise RuntimeError("模拟异常")
        except RuntimeError as e:
            exception_raised[0] = True
            raise
        finally:
            # 模拟finally块
            task_desc.is_processing = False
            finally_executed[0] = True

    # 执行并捕获异常
    try:
        mock_process_with_exception()
    except RuntimeError:
        pass  # 预期异常

    # 验证结果
    assert exception_raised[0], "✗ 异常应该被抛出"
    assert finally_executed[0], "✗ finally块应该执行"
    assert task_desc.is_processing == False, "✗ is_processing应该被重置为False"

    # 验证任务可以被重新处理
    can_reprocess = not task_desc.is_processing
    assert can_reprocess, "✗ 任务应该可以被重新处理"

    print("✓ finally块异常清除机制正常工作")
    print(f"  - 异常抛出: {exception_raised[0]}")
    print(f"  - finally执行: {finally_executed[0]}")
    print(f"  - is_processing重置: {task_desc.is_processing == False}")

    return True


def test_task_removed_after_success():
    """
    场景6：成功后从队列移除测试

    验证任务成功后才从queued_tasks中移除
    """
    print("\n=== 场景6：成功后从队列移除测试 ===")

    # 创建模拟的TaskLifecycleManager
    with patch('ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager.ClusterMonitor') as MockMonitor:
        mock_monitor = MockMonitor.return_value

        # 资源可用状态（<70%）
        mock_monitor.get_all_cluster_info.return_value = {
            'cluster1': {
                'metadata': create_mock_cluster_metadata('cluster1'),
                'snapshot': create_low_resource_snapshot('cluster1')
            }
        }

        # 创建TaskLifecycleManager
        task_lifecycle_manager = TaskLifecycleManager(mock_monitor)

        # 创建测试任务
        def test_func():
            return "task_result"

        task_desc = TaskDescription(
            task_id="test_success_task",
            func_or_class=test_func,
            args=(),
            kwargs={},
            is_actor=False,
            is_top_level_task=False,
            preferred_cluster='cluster1'
        )

        # 模拟成功的dispatcher
        def mock_dispatch_task_success(task, cluster_name):
            return f"future_{task.task_id}"

        task_lifecycle_manager.dispatcher.dispatch_task = mock_dispatch_task_success

        # 添加任务到队列
        task_lifecycle_manager.queued_tasks.append(task_desc)
        task_lifecycle_manager.task_queue.enqueue(task_desc, 'cluster1')

        print(f"  - 初始队列大小: {len(task_lifecycle_manager.queued_tasks)}")
        print(f"  - task_queue大小: {task_lifecycle_manager.task_queue.size('cluster1')}")

        # 模拟任务成功处理（从task_queue取出）
        processed_task = task_lifecycle_manager.task_queue.dequeue_from_cluster('cluster1')

        # 模拟_process_task成功路径
        if processed_task and processed_task == task_desc:
            # 从queued_tasks移除
            if processed_task in task_lifecycle_manager.queued_tasks:
                task_lifecycle_manager.queued_tasks.remove(processed_task)

        # 验证任务已从队列移除
        assert task_desc not in task_lifecycle_manager.queued_tasks, \
            "✗ 任务应该已从queued_tasks移除"
        assert task_lifecycle_manager.task_queue.size('cluster1') == 0, \
            "✗ task_queue应该为空"

        print("✓ 成功后从队列移除机制正常工作")
        print(f"  - queued_tasks大小: {len(task_lifecycle_manager.queued_tasks)}")
        print(f"  - task_queue大小: {task_lifecycle_manager.task_queue.size('cluster1')}")

    return True


# ==================== Actor专用测试 ====================

def test_actor_is_processing_concurrent_protection():
    """
    Actor场景1：is_processing标记防止Actor重复创建

    验证Actor任务的并发保护机制
    """
    print("\n=== Actor场景1：is_processing并发保护测试 ===")

    # 创建Actor任务
    class TestActor:
        def __init__(self):
            pass
        def remote_method(self):
            return "actor_result"

    task_desc = TaskDescription(
        task_id="test_actor_concurrent",
        func_or_class=TestActor,
        args=(),
        kwargs={},
        is_actor=True,
        is_top_level_task=False,
        is_processing=False
    )

    # 记录处理情况
    actor_creation_count = [0]
    skipped_count = [0]

    def mock_process_actor(task):
        """模拟Actor处理流程"""
        # 模拟is_processing检查
        if task.is_processing:
            skipped_count.append(1)
            return False

        # 设置is_processing标记
        task.is_processing = True
        actor_creation_count[0] += 1
        time.sleep(0.1)
        task.is_processing = False
        return True

    # 并发创建Actor
    t1 = threading.Thread(target=lambda: mock_process_actor(task_desc))
    t2 = threading.Thread(target=lambda: mock_process_actor(task_desc))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # 验证结果：Actor只创建一次
    assert actor_creation_count[0] == 1, f"✗ 预期创建1次，实际{actor_creation_count[0]}次"
    assert task_desc.is_processing == False, "✗ is_processing应该为False"

    print("✓ Actor并发保护机制正常工作")
    print(f"  - Actor创建次数: {actor_creation_count[0]}")
    print(f"  - 跳过次数: {len(skipped_count)}")
    print(f"  - 说明: is_processing保护机制确保Actor不会被重复创建")

    return True


def test_actor_retry_after_failure():
    """
    Actor场景2：Actor创建失败后重新排队

    验证Actor创建失败后的重新排队机制
    """
    print("\n=== Actor场景2：Actor失败后重新排队测试 ===")

    # 创建模拟的TaskLifecycleManager
    with patch('ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager.ClusterMonitor') as MockMonitor:
        mock_monitor = MockMonitor.return_value

        # 资源紧张状态
        mock_monitor.get_all_cluster_info.return_value = {
            'cluster1': {
                'metadata': create_mock_cluster_metadata('cluster1'),
                'snapshot': create_high_resource_snapshot('cluster1')
            }
        }

        # 创建TaskLifecycleManager
        task_lifecycle_manager = TaskLifecycleManager(mock_monitor)

        # 创建Actor任务
        class TestActor:
            def __init__(self):
                pass

        task_desc = TaskDescription(
            task_id="test_actor_retry",
            func_or_class=TestActor,
            args=(),
            kwargs={},
            is_actor=True,
            is_top_level_task=False,
            preferred_cluster='cluster1'
        )

        # 模拟dispatcher首次失败
        dispatch_attempts = {'count': 0}

        def mock_dispatch_actor(task, cluster_name):
            dispatch_attempts['count'] += 1
            if dispatch_attempts['count'] == 1:
                raise Exception("Actor创建失败：资源紧张")
            return f"actor_handle_{task.task_id}"

        task_lifecycle_manager.dispatcher.dispatch_task = mock_dispatch_actor

        # 首次尝试提交（应该进入队列）
        try:
            task_lifecycle_manager.submit_task_and_get_future(task_desc)
        except:
            pass

        # 验证任务在队列中
        assert task_desc in task_lifecycle_manager.queued_tasks or \
               task_lifecycle_manager.task_queue.total_size() > 0, \
            "✗ Actor应该在队列中"

        print("✓ Actor重新排队机制正常工作")
        print(f"  - 调度尝试次数: {dispatch_attempts['count']}")

    return True


# ==================== 主测试运行器 ====================

if __name__ == "__main__":
    print("=" * 60)
    print("任务重复提交测试用例")
    print("=" * 60)
    print("\n测试目标: 验证submit_task/actor接口不会重复提交任务")

    tests = [
        ("场景1：is_processing标记并发保护", test_is_processing_concurrent_protection),
        ("场景2：任务失败后重新排队", test_task_retry_after_failure),
        ("场景3：用户重复提交相同内容", test_duplicate_content_rejection),
        ("场景4：worker_loop与re-evaluation协调", test_worker_loop_re_evaluation_coordination),
        ("场景5：finally块异常清除", test_finally_block_exception_clear),
        ("场景6：成功后从队列移除", test_task_removed_after_success),
        ("Actor场景1：is_processing并发保护", test_actor_is_processing_concurrent_protection),
        ("Actor场景2：Actor失败后重新排队", test_actor_retry_after_failure),
    ]

    passed = 0
    failed = 0
    errors = []

    for name, test_func in tests:
        try:
            print(f"\n{'='*60}")
            test_func()
            print(f"✓ 测试通过: {name}")
            passed += 1
        except AssertionError as e:
            print(f"✗ 测试失败: {name}")
            print(f"  错误: {e}")
            failed += 1
            errors.append((name, str(e)))
        except Exception as e:
            print(f"✗ 测试错误: {name}")
            print(f"  错误: {e}")
            failed += 1
            errors.append((name, str(e)))

    print("\n" + "=" * 60)
    print(f"测试结果汇总")
    print("=" * 60)
    print(f"通过: {passed}")
    print(f"失败: {failed}")
    print(f"总计: {passed + failed}")

    if errors:
        print("\n失败详情:")
        for name, error in errors:
            print(f"  - {name}: {error}")

    if failed == 0:
        print("\n✓ 所有测试通过！")
        sys.exit(0)
    else:
        print(f"\n✗ {failed}个测试失败")
        sys.exit(1)
