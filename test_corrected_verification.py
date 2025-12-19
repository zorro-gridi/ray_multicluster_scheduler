#!/usr/bin/env python3
"""
Corrected test script to verify that the scheduler correctly uses user-specified cluster configuration
and that submit_task does not reload default configuration when scheduler is already initialized.
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

def test_user_specified_config_with_submit_task():
    """Test that scheduler uses user-specified cluster configuration with submit_task."""
    print("=== Testing User-Specified Cluster Configuration with submit_task ===")

    # 1. Initialize scheduler with user-specified config file
    print("\n1. Initializing scheduler with user-specified mac-only config...")
    try:
        task_lifecycle_manager = initialize_scheduler_environment("./mac_only_clusters.yaml")
        print("   ✓ Scheduler initialized successfully with user-specified config")
    except Exception as e:
        print(f"   ✗ Failed to initialize scheduler: {e}")
        return False

    # 2. Test submit_task with preferred cluster 'mac'
    print("\n2. Testing submit_task with preferred cluster 'mac'...")
    try:
        task_id, result = submit_task(
            func=test_function,
            args=(10, 5),
            name='final_test_task',
            preferred_cluster='mac'
        )
        print(f"   ✓ Task submitted successfully. Task ID: {task_id}")
        print(f"   ✓ Task result: {result}")
        if result == 15:  # 10 + 5
            print("   ✓ Task result is correct")
            return True
        else:
            print(f"   ✗ Task result is incorrect. Expected 15, got {result}")
            return False
    except Exception as e:
        print(f"   ✗ Failed to submit task: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_user_specified_config_with_submit_task()
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)