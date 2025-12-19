#!/usr/bin/env python3
"""
Test script to verify cluster migration with correct runtime_env handling.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment, submit_task

@ray.remote
def test_function(x, y):
    """Simple test function that adds two numbers."""
    return x + y

def test_cluster_migration():
    """Test cluster migration with correct runtime_env handling."""
    print("Initializing scheduler environment...")
    task_lifecycle_manager = initialize_scheduler_environment()

    print("\n=== Test 1: Submit task to mac cluster with mac runtime_env ===")
    try:
        # 提交任务到mac集群，使用mac集群的runtime_env配置
        task_id, result = submit_task(
            func=test_function,
            args=(10, 5),
            name='test_mac_cluster',
            preferred_cluster='mac',
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        print(f"✓ Task {task_id} completed successfully with result: {result}")
    except Exception as e:
        print(f"✗ Task submission to mac cluster failed: {e}")
        # 不打印完整的traceback，因为我们知道这可能会失败

    print("\n=== Test 2: Submit task with wrong runtime_env to test migration ===")
    try:
        # 提交任务到mac集群，但故意使用错误的runtime_env配置（模拟迁移场景）
        task_id, result = submit_task(
            func=test_function,
            args=(20, 30),
            name='test_migration',
            preferred_cluster='mac',  # 指定mac集群
            runtime_env={
                "conda": "wrong_env",  # 故意使用错误的conda环境，应该被替换为mac集群的"k8s"
                "env_vars": {
                    "home_dir": "/wrong/path"  # 故意使用错误的home_dir，应该被替换为mac集群的"/Users/zorro"
                }
            }
        )
        print(f"✓ Task {task_id} completed successfully with result: {result}")
    except Exception as e:
        print(f"✗ Task submission with wrong runtime_env failed: {e}")
        # 不打印完整的traceback，因为我们知道这可能会失败

    print("\n=== Test 3: Submit task with no runtime_env to test default behavior ===")
    try:
        # 提交任务到mac集群，不指定runtime_env
        task_id, result = submit_task(
            func=test_function,
            args=(50, 60),
            name='test_no_runtime_env',
            preferred_cluster='mac',
            # 不提供runtime_env参数
        )
        print(f"✓ Task {task_id} completed successfully with result: {result}")
    except Exception as e:
        print(f"✗ Task submission with no runtime_env failed: {e}")
        # 不打印完整的traceback，因为我们知道这可能会失败

if __name__ == "__main__":
    test_cluster_migration()