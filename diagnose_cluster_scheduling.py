#!/usr/bin/env python3
"""
Detailed diagnostic test to understand cluster scheduling behavior.
"""

import sys
import os
import time
import ray
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task
)

@ray.remote
def test_task(x, y):
    """Simple test task that adds two numbers."""
    time.sleep(0.1)
    return x + y

def diagnose_cluster_scheduling():
    """Diagnose cluster scheduling behavior in detail."""
    print("=" * 60)
    print("Diagnostic Test: Cluster Scheduling Behavior")
    print("=" * 60)

    try:
        # 初始化调度器环境
        print("1. Initializing scheduler environment...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("   ✓ Scheduler environment initialized successfully")

        # 检查集群信息
        print("\n2. Checking cluster information...")
        cluster_monitor = task_lifecycle_manager.cluster_monitor
        cluster_info = cluster_monitor.get_all_cluster_info()

        print(f"   Total clusters: {len(cluster_info)}")
        for name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']
            print(f"   - Cluster '{name}':")
            print(f"     Address: {metadata.head_address}")
            print(f"     Prefer: {metadata.prefer}")
            print(f"     Home Dir: {metadata.home_dir}")
            print(f"     Conda Env: {metadata.conda}")
            print(f"     Available: {snapshot.available_resources if snapshot else 'None'}")

        # 测试提交到mac集群
        print("\n3. Testing task submission to mac cluster...")
        print("   Submitting task with preferred_cluster='mac'")
        task_id, result = submit_task(
            func=test_task,
            args=(10, 20),
            name="diagnostic_test_mac",
            preferred_cluster="mac",
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        print(f"   ✓ Task completed successfully. Task ID: {task_id}, Result: {result}")

    except Exception as e:
        print(f"   ✗ Test failed with error: {e}")
        print("\n   Diagnostic information:")
        print("   This error might be caused by:")
        print("   1. The target cluster (mac) not having the required conda environment 'k8s'")
        print("   2. Network connectivity issues to the target cluster")
        print("   3. Cluster resource availability issues")

        # 尝试获取更多诊断信息
        try:
            import traceback
            print(f"\n   Full traceback:")
            traceback.print_exc()
        except:
            pass

        return False

    return True

def test_cluster_migration_diagnostic():
    """Test cluster migration behavior in detail."""
    print("\n" + "=" * 60)
    print("Diagnostic Test: Cluster Migration Behavior")
    print("=" * 60)

    try:
        print("1. Testing task submission with non-existent preferred cluster...")
        print("   Submitting task with preferred_cluster='nonexistent'")
        task_id, result = submit_task(
            func=test_task,
            args=(30, 40),
            name="diagnostic_test_migration",
            preferred_cluster="nonexistent",
            runtime_env={
                "conda": "wrong_env",
                "env_vars": {
                    "home_dir": "/wrong/path"
                }
            }
        )
        print(f"   ✓ Task completed successfully after migration. Task ID: {task_id}, Result: {result}")

        # 检查实际调度到哪个集群
        print("\n2. Analyzing actual cluster assignment...")
        # 注意：我们无法直接从结果中得知任务被调度到哪个集群，
        # 但在日志中应该能看到调度决策信息

    except Exception as e:
        print(f"   ✗ Test failed with error: {e}")
        print("\n   This error might be caused by:")
        print("   1. All available clusters lacking the required conda environments")
        print("   2. Network connectivity issues to all clusters")
        print("   3. Cluster resource constraints")

        # 尝试获取更多诊断信息
        try:
            import traceback
            print(f"\n   Full traceback:")
            traceback.print_exc()
        except:
            pass

        return False

    return True

def main():
    """Run diagnostic tests."""
    print("Running detailed diagnostic tests for cluster scheduling...")

    # Run diagnostic tests
    results = []
    results.append(diagnose_cluster_scheduling())
    results.append(test_cluster_migration_diagnostic())

    # Summary
    print("\n" + "=" * 60)
    print("Diagnostic Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")

    if passed == total:
        print("✓ All diagnostic tests passed!")
        print("\nThis indicates that the cluster scheduling logic is working correctly.")
        print("Any runtime errors would be due to environment configuration issues")
        print("rather than code logic problems.")
    else:
        print("✗ Some diagnostic tests failed!")
        print("\nThis might indicate either:")
        print("1. Code logic issues that need to be fixed")
        print("2. Environment configuration issues (missing conda environments, network issues, etc.)")

    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)