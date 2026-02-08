"""
Strict unit tests for is_cluster_busy interface.
Validates the correctness of cluster busy state management for serial mode.
"""

import pytest
import threading
import time
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import ClusterSubmissionHistory


class TestIsClusterBusyBasicFunctionality:
    """基础功能测试 - Basic Functionality Tests"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_empty_cluster_not_busy(self):
        """无运行任务时，集群应返回 False"""
        result = TaskQueue.is_cluster_busy("centos")
        assert result is False, "Empty cluster should not be busy"

    def test_single_task_makes_busy(self):
        """注册1个task后，集群应返回 True"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        result = TaskQueue.is_cluster_busy("centos")
        assert result is True, "Cluster with a running task should be busy"

    def test_single_job_makes_busy(self):
        """注册1个job后，集群应返回 True"""
        TaskQueue.register_running_task("job_001", "centos", "job")
        result = TaskQueue.is_cluster_busy("centos")
        assert result is True, "Cluster with a running job should be busy"

    def test_unregister_after_completion(self):
        """注销任务后，集群应返回 False"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        assert TaskQueue.is_cluster_busy("centos") is True
        TaskQueue.unregister_running_task("task_001")
        result = TaskQueue.is_cluster_busy("centos")
        assert result is False, "Cluster should not be busy after task completion"

    def test_multiple_tasks_same_cluster(self):
        """多个任务同集群时，繁忙状态正确，计数正确"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        TaskQueue.register_running_task("task_002", "centos", "task")
        TaskQueue.register_running_task("task_003", "centos", "task")

        assert TaskQueue.is_cluster_busy("centos") is True
        assert TaskQueue.get_cluster_running_task_count("centos") == 3

    def test_tasks_different_clusters(self):
        """不同集群的繁忙状态应独立"""
        TaskQueue.register_running_task("task_a", "cluster1", "task")
        TaskQueue.register_running_task("task_b", "cluster2", "task")

        assert TaskQueue.is_cluster_busy("cluster1") is True
        assert TaskQueue.is_cluster_busy("cluster2") is True
        assert TaskQueue.is_cluster_busy("cluster3") is False

        # Clean up
        TaskQueue.unregister_running_task("task_a")
        TaskQueue.unregister_running_task("task_b")


class TestIsClusterBusyEdgeCases:
    """边界情况测试 - Edge Cases Tests"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_unregister_nonexistent_task(self):
        """注销不存在的任务应返回 False"""
        result = TaskQueue.unregister_running_task("nonexistent_task")
        assert result is False, "Unregistering non-existent task should return False"

    def test_idempotent_unregister(self):
        """多次注销同一任务只生效一次"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        assert TaskQueue.is_cluster_busy("centos") is True

        # First unregister
        result1 = TaskQueue.unregister_running_task("task_001")
        assert result1 is True
        assert TaskQueue.is_cluster_busy("centos") is False

        # Second unregister should still return False
        result2 = TaskQueue.unregister_running_task("task_001")
        assert result2 is False, "Second unregister should return False"

    def test_empty_string_cluster_name(self):
        """空字符串作为集群名"""
        TaskQueue.register_running_task("task_001", "", "task")
        assert TaskQueue.is_cluster_busy("") is True
        TaskQueue.unregister_running_task("task_001")
        assert TaskQueue.is_cluster_busy("") is False

    def test_special_chars_in_cluster_name(self):
        """特殊字符集群名"""
        special_names = ["cluster-1", "cluster_1", "cluster.1", "cluster/1"]
        for name in special_names:
            TaskQueue.register_running_task(f"task_{name}", name, "task")
            assert TaskQueue.is_cluster_busy(name) is True, f"Cluster {name} should be busy"
            TaskQueue.unregister_running_task(f"task_{name}")

    def test_very_long_task_id(self):
        """超长任务ID"""
        long_id = "task_" + "a" * 1000
        TaskQueue.register_running_task(long_id, "centos", "task")
        assert TaskQueue.is_cluster_busy("centos") is True
        TaskQueue.unregister_running_task(long_id)
        assert TaskQueue.is_cluster_busy("centos") is False


class TestIsClusterBusyThreadSafety:
    """并发安全性测试 - Thread Safety Tests"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_concurrent_registration(self):
        """多线程同时注册"""
        errors = []
        threads = []

        def register_task(task_id: str):
            try:
                TaskQueue.register_running_task(task_id, "centos", "task")
            except Exception as e:
                errors.append(e)

        # Create 10 threads to register tasks
        for i in range(10):
            t = threading.Thread(target=register_task, args=(f"task_{i}",))
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Check results
        assert len(errors) == 0, f"Errors during concurrent registration: {errors}"
        assert TaskQueue.get_cluster_running_task_count("centos") == 10

    def test_concurrent_registration_unregistration(self):
        """同时注册和注销"""
        errors = []
        lock = threading.Lock()
        barrier = threading.Barrier(10)

        def register_and_unregister(task_id: str):
            try:
                # Wait for all threads to be ready
                barrier.wait()
                # Register
                TaskQueue.register_running_task(task_id, "centos", "task")
                time.sleep(0.01)
                # Unregister
                TaskQueue.unregister_running_task(task_id)
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = []
        for i in range(10):
            t = threading.Thread(target=register_and_unregister, args=(f"task_{i}",))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors during concurrent operations: {errors}"
        # All tasks should be unregistered
        assert TaskQueue.is_cluster_busy("centos") is False

    def test_stress_test_high_concurrency(self):
        """高并发压力测试"""
        errors = []
        iterations = 100

        def stress_task(task_id: str):
            try:
                # Register
                TaskQueue.register_running_task(task_id, "centos", "task")
                time.sleep(0.001)
                # Check busy status multiple times
                for _ in range(5):
                    _ = TaskQueue.is_cluster_busy("centos")
                # Unregister
                TaskQueue.unregister_running_task(task_id)
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(iterations):
            t = threading.Thread(target=stress_task, args=(f"stress_task_{i}",))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors during stress test: {errors}"
        assert TaskQueue.is_cluster_busy("centos") is False


class TestIsClusterBusyTaskTypeIsolation:
    """任务类型隔离测试 - Task Type Isolation Tests"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_task_and_job_same_cluster(self):
        """Task和Job混合场景"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        TaskQueue.register_running_task("job_001", "centos", "job")

        assert TaskQueue.is_cluster_busy("centos") is True
        assert TaskQueue.get_cluster_running_task_count("centos") == 2

        # Unregister task only
        TaskQueue.unregister_running_task("task_001")
        assert TaskQueue.is_cluster_busy("centos") is True

        # Unregister job
        TaskQueue.unregister_running_task("job_001")
        assert TaskQueue.is_cluster_busy("centos") is False

    def test_correct_task_type_stored(self):
        """验证存储的任务类型正确"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        TaskQueue.register_running_task("job_001", "centos", "job")

        all_tasks = TaskQueue.get_all_running_tasks()
        assert all_tasks["task_001"]["type"] == "task"
        assert all_tasks["job_001"]["type"] == "job"

    def test_type_filtering(self):
        """类型过滤功能验证"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        TaskQueue.register_running_task("task_002", "centos", "task")
        TaskQueue.register_running_task("job_001", "centos", "job")

        all_tasks = TaskQueue.get_all_running_tasks()
        task_count = sum(1 for t in all_tasks.values() if t["type"] == "task")
        job_count = sum(1 for t in all_tasks.values() if t["type"] == "job")

        assert task_count == 2
        assert job_count == 1


class TestIsClusterBusySerialModeIntegration:
    """串行模式集成测试 - Serial Mode Integration Tests"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_serial_mode_rejects_new_job_when_busy(self):
        """验证串行模式下，同一集群同时只能有一个Job执行"""
        # Simulate Job1 starts executing
        TaskQueue.register_running_task("job_001", "centos", "job")

        # Check busy status
        assert TaskQueue.is_cluster_busy("centos") is True, "Cluster should be busy with job running"

        # Simulate Job2 attempting to start - should be rejected in serial mode
        # Verify is_cluster_busy returns True
        result = TaskQueue.is_cluster_busy("centos")
        assert result is True, "Should return True when busy"

        # Job1 completes
        TaskQueue.unregister_running_task("job_001")

        # Now should not be busy
        assert TaskQueue.is_cluster_busy("centos") is False, "Cluster should be idle after job completion"

    def test_serial_mode_accepts_job_when_idle(self):
        """验证空闲时接受新Job"""
        assert TaskQueue.is_cluster_busy("centos") is False, "Cluster should be idle initially"

        TaskQueue.register_running_task("new_job", "centos", "job")
        assert TaskQueue.is_cluster_busy("centos") is True, "Cluster should be busy after job starts"

    def test_serial_mode_with_subtasks_allowed(self):
        """子任务不受串行模式限制 - 验证实现正确"""
        # Main job
        TaskQueue.register_running_task("main_job", "centos", "job")

        # Subtasks can still be tracked
        TaskQueue.register_running_task("subtask_1", "centos", "task")
        TaskQueue.register_running_task("subtask_2", "centos", "task")

        # All tracked tasks contribute to busy count
        assert TaskQueue.get_cluster_running_task_count("centos") == 3
        assert TaskQueue.is_cluster_busy("centos") is True

    def test_parallel_mode_always_allows(self):
        """并行模式不检查繁忙状态 - 验证繁忙状态本身正确"""
        # Even in parallel mode, we track running tasks correctly
        TaskQueue.register_running_task("task_1", "centos", "task")
        TaskQueue.register_running_task("task_2", "centos", "task")

        # is_cluster_busy still returns correct status
        # (parallel mode just doesn't use this check)
        assert TaskQueue.is_cluster_busy("centos") is True

        TaskQueue.unregister_running_task("task_1")
        TaskQueue.unregister_running_task("task_2")
        assert TaskQueue.is_cluster_busy("centos") is False


class TestIsClusterBusy40sRuleIntegration:
    """40秒规则与串行模式结合测试"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_40s_rule_with_serial_mode(self):
        """验证40秒规则和串行模式协同工作"""
        TaskQueue.register_running_task("job_001", "centos", "job")

        # Verify both checks work together
        assert TaskQueue.is_cluster_busy("centos") is True

        # 40s timeout check with ClusterSubmissionHistory
        history = ClusterSubmissionHistory()
        history.record_submission("centos")
        is_available = history.is_cluster_available("centos")

        # Within 40s, cluster should not be available
        assert is_available is False

    def test_combined_busy_and_timeout_check(self):
        """繁忙检查和超时检查组合验证"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        history = ClusterSubmissionHistory()

        # Cluster is busy
        assert TaskQueue.is_cluster_busy("centos") is True

        # Record submission
        history.record_submission("centos")

        # Both checks indicate cluster is not available
        assert TaskQueue.is_cluster_busy("centos") is True
        assert not history.is_cluster_available("centos")


class TestIsClusterBusyEdgeCaseIsolation:
    """边界情况隔离测试"""

    def setup_method(self):
        """Clean up before each test"""
        TaskQueue._running_tasks.clear()

    def teardown_method(self):
        """Clean up after each test"""
        TaskQueue._running_tasks.clear()

    def test_get_all_running_tasks_returns_copy(self):
        """验证 get_all_running_tasks 返回副本"""
        TaskQueue.register_running_task("task_001", "centos", "task")
        TaskQueue.register_running_task("task_002", "centos", "task")

        all_tasks = TaskQueue.get_all_running_tasks()
        original_count = len(all_tasks)

        # Modify the returned dict
        all_tasks["task_003"] = {"cluster": "centos", "type": "task", "submitted_at": time.time()}

        # Original should not be affected
        assert TaskQueue.get_cluster_running_task_count("centos") == original_count

    def test_timestamp_tracking(self):
        """验证时间戳正确记录"""
        before_register = time.time()
        TaskQueue.register_running_task("task_001", "centos", "task")
        after_register = time.time()

        all_tasks = TaskQueue.get_all_running_tasks()
        submitted_at = all_tasks["task_001"]["submitted_at"]

        assert before_register <= submitted_at <= after_register

    def test_multiple_clusters_isolation(self):
        """多集群隔离测试"""
        # Register tasks on different clusters
        TaskQueue.register_running_task("task_1", "cluster_a", "task")
        TaskQueue.register_running_task("task_2", "cluster_b", "task")
        TaskQueue.register_running_task("task_3", "cluster_c", "task")

        # Each cluster should have correct count
        assert TaskQueue.get_cluster_running_task_count("cluster_a") == 1
        assert TaskQueue.get_cluster_running_task_count("cluster_b") == 1
        assert TaskQueue.get_cluster_running_task_count("cluster_c") == 1
        assert TaskQueue.get_cluster_running_task_count("cluster_d") == 0

        # Unregister from one cluster should not affect others
        TaskQueue.unregister_running_task("task_1")
        assert TaskQueue.get_cluster_running_task_count("cluster_a") == 0
        assert TaskQueue.get_cluster_running_task_count("cluster_b") == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
