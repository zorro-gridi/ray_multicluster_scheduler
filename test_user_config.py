#!/usr/bin/env python3
"""
Test script to verify that the scheduler correctly uses user-specified cluster configuration.
"""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment, submit_task

@ray.remote
def test_function(x, y):
    """Simple test function that adds two numbers."""
    time.sleep(0.1)  # Simulate some work
    return x + y

def test_user_specified_config():
    """Test that scheduler uses user-specified cluster configuration."""
    print("=== Testing User-Specified Cluster Configuration ===")
    
    # Test: Initialize scheduler with user-specified config file
    print("\n1. Initializing scheduler with user-specified mac-only config...")
    try:
        task_lifecycle_manager = initialize_scheduler_environment("./mac_only_clusters.yaml")
        print("✓ Scheduler initialized with user-specified config")
        
        # Check that we have only one cluster
        cluster_info = task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
        print(f"Available clusters: {list(cluster_info.keys())}")
        assert len(cluster_info) == 1, f"Expected 1 cluster, got {len(cluster_info)}"
        assert "mac" in cluster_info, "Expected 'mac' cluster to be available"
        
    except Exception as e:
        print(f"✗ Error initializing scheduler: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test: Submit task using submit_task function (should not try to re-initialize with default config)
    print("\n2. Submitting task using submit_task function...")
    try:
        task_id, result = submit_task(
            func=test_function,
            args=(10, 20),
            name="test_user_config",
            preferred_cluster="mac"
        )
        print(f"✓ Task submitted successfully. Task ID: {task_id}, Result: {result}")
        assert result == 30, f"Expected 30, got {result}"
        
    except Exception as e:
        print(f"✗ Error submitting task: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n=== All tests completed successfully ===")
    return True

if __name__ == "__main__":
    success = test_user_specified_config()
    sys.exit(0 if success else 1)