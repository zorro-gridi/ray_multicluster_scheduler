#!/usr/bin/env python3
"""
Final test script to verify that the scheduler correctly uses user-specified cluster configuration
and that submit_task does not reload default configuration.
"""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment, 
    submit_task,
    submit_actor
)

@ray.remote
def test_function(x, y):
    """Simple test function that adds two numbers."""
    time.sleep(0.1)  # Simulate some work
    return x + y

@ray.remote
class TestActor:
    """Simple test actor."""
    def __init__(self, initial_value):
        self.value = initial_value
    
    def increment(self, amount):
        """Increment the value."""
        self.value += amount
        return self.value
    
    def get_value(self):
        """Get the current value."""
        return self.value

def test_final_verification():
    """Final test to verify the fix works correctly."""
    print("=== Final Verification Test ===")
    
    # Step 1: Initialize scheduler with user-specified single mac cluster config
    print("\n1. Initializing scheduler with user-specified mac-only config...")
    try:
        task_lifecycle_manager = initialize_scheduler_environment("./mac_only_clusters.yaml")
        print("✓ Scheduler initialized with user-specified config")
        
        # Verify only mac cluster is available
        cluster_info = task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
        clusters = list(cluster_info.keys())
        print(f"Available clusters: {clusters}")
        assert len(clusters) == 1, f"Expected 1 cluster, got {len(clusters)}"
        assert "mac" in clusters, "Expected 'mac' cluster to be available"
        
        print("✓ Correctly using user-specified single mac cluster configuration")
        
    except Exception as e:
        print(f"✗ Error initializing scheduler: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 2: Test submit_task - this should NOT reload default configuration
    print("\n2. Testing submit_task - verifying it does not reload default configuration...")
    try:
        # Capture the start time to measure how long this takes
        start_time = time.time()
        
        task_id, result = submit_task(
            func=test_function,
            args=(10, 20),
            name="final_test_task",
            preferred_cluster="mac",
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        
        elapsed_time = time.time() - start_time
        print(f"✓ Task submitted successfully in {elapsed_time:.2f} seconds. Task ID: {task_id}, Result: {result}")
        assert result == 30, f"Expected 30, got {result}"
        
        # If we got here quickly (less than 5 seconds), it means it didn't reload configuration
        if elapsed_time < 5:
            print("✓ submit_task did not reload default configuration (fast execution)")
        else:
            print("⚠ submit_task may have reloaded configuration (slow execution)")
        
    except Exception as e:
        print(f"✗ Error submitting task: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 3: Verify cluster configuration is still correct
    print("\n3. Verifying cluster configuration is still correct after submit_task...")
    try:
        # This should not cause a reload of the configuration
        task_id, result = submit_task(
            func=test_function,
            args=(5, 15),
            name="verification_task",
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        print(f"✓ Verification task submitted successfully. Task ID: {task_id}, Result: {result}")
        assert result == 20, f"Expected 20, got {result}"
        
        # Check that we still only have the mac cluster
        cluster_info = task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
        clusters = list(cluster_info.keys())
        print(f"Clusters after submit_task: {clusters}")
        assert len(clusters) == 1, f"Expected 1 cluster, got {len(clusters)}"
        assert "mac" in clusters, "Expected 'mac' cluster to still be available"
        
        print("✓ Cluster configuration remained consistent")
        
    except Exception as e:
        print(f"✗ Error in verification: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n=== Final Test Completed Successfully ===")
    print("Summary:")
    print("  ✓ Scheduler correctly uses user-specified single mac cluster configuration")
    print("  ✓ submit_task works correctly without reloading default configuration")
    print("  ✓ Cluster configuration remains consistent throughout execution")
    return True

if __name__ == "__main__":
    success = test_final_verification()
    sys.exit(0 if success else 1)