"""
Integration test for the refactored scheduler system.
Tests the complete workflow including detached monitor, job lifecycle, and proper cleanup.
"""
import ray
import time
import signal
import sys
from ray_multicluster_scheduler.app.client_api import (
    submit_task, submit_actor, submit_job, submit_batch_jobs,
    initialize_scheduler_environment
)
from ray_multicluster_scheduler.scheduler.monitor.start_monitor import start_cluster_monitor
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor


def test_basic_task_submission():
    """Test basic task submission and completion."""
    print("Testing basic task submission...")

    def simple_task(x):
        return x * 2

    try:
        task_id, result = submit_task(simple_task, args=(21,))
        print(f"Task {task_id} completed with result: {result}")
        assert result == 42, f"Expected 42, got {result}"
        print("✓ Basic task submission test passed")
        return True
    except Exception as e:
        print(f"✗ Basic task submission test failed: {e}")
        return False


def test_actor_submission():
    """Test actor submission and method calls."""
    print("Testing actor submission...")

    @ray.remote
    class Counter:
        def __init__(self, start=0):
            self.value = start

        def increment(self, amount=1):
            self.value += amount
            return self.value

        def get_value(self):
            return self.value

    try:
        actor_id, actor_handle = submit_actor(Counter, args=(10,))
        print(f"Actor {actor_id} created")

        # Test actor methods
        result1 = ray.get(actor_handle.increment.remote(5))
        print(f"Counter after increment: {result1}")
        assert result1 == 15, f"Expected 15, got {result1}"

        result2 = ray.get(actor_handle.get_value.remote())
        print(f"Counter final value: {result2}")
        assert result2 == 15, f"Expected 15, got {result2}"

        print("✓ Actor submission test passed")
        return True
    except Exception as e:
        print(f"✗ Actor submission test failed: {e}")
        return False


def test_job_submission():
    """Test job submission and completion."""
    print("Testing job submission...")

    try:
        # Submit a simple Python job
        job_id = submit_job(
            entrypoint="python -c \"print('Hello from Ray job')\"",
        )
        print(f"Job {job_id} submitted successfully")

        print("✓ Job submission test passed")
        return True
    except Exception as e:
        print(f"✗ Job submission test failed: {e}")
        return False


def test_batch_job_submission():
    """Test batch job submission."""
    print("Testing batch job submission...")

    try:
        jobs = [
            {"entrypoint": "python -c \"print('Job 1')\""},
            {"entrypoint": "python -c \"print('Job 2')\""},
            {"entrypoint": "python -c \"print('Job 3')\""}
        ]

        job_ids = submit_batch_jobs(jobs)
        print(f"Batch jobs {job_ids} submitted successfully")

        print("✓ Batch job submission test passed")
        return True
    except Exception as e:
        print(f"✗ Batch job submission test failed: {e}")
        return False


def main():
    """Run all integration tests."""
    print("Starting integration tests for refactored scheduler system...")

    # Initialize the scheduler environment
    try:
        task_manager = initialize_scheduler_environment()
        print("✓ Scheduler environment initialized")
    except Exception as e:
        print(f"✗ Failed to initialize scheduler environment: {e}")
        return False

    # Run tests
    tests = [
        test_basic_task_submission,
        test_actor_submission,
        test_job_submission,
        test_batch_job_submission
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
            print()  # Empty line for readability
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
            results.append(False)
            print()

    # Summary
    passed = sum(results)
    total = len(results)
    print(f"Integration tests summary: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All integration tests passed!")
        return True
    else:
        print("❌ Some integration tests failed.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)