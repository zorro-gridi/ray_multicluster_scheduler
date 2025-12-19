#!/usr/bin/env python3
"""
Complete test script to verify cluster scheduling with single mac cluster configuration.
This test specifically checks that submit_task works correctly with user-specified config.
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

def test_single_mac_cluster_with_submit_task():
    """Test cluster scheduling with single mac cluster configuration including submit_task."""
    print("=== Testing Single Mac Cluster Configuration with submit_task ===")
    
    # Step 1: Initialize scheduler with user-specified single mac cluster config
    print("\n1. Initializing scheduler with user-specified mac-only config...")
    try:
        task_lifecycle_manager = initialize_scheduler_environment("./mac_only_clusters.yaml")
        print("✓ Scheduler initialized with user-specified config")
        
        # Verify only mac cluster is available
        cluster_info = task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
        print(f"Available clusters: {list(cluster_info.keys())}")
        assert len(cluster_info) == 1, f"Expected 1 cluster, got {len(cluster_info)}"
        assert "mac" in cluster_info, "Expected 'mac' cluster to be available"
        
        # Verify cluster details
        mac_cluster = cluster_info["mac"]
        metadata = mac_cluster['metadata']
        print(f"Mac cluster details:")
        print(f"  - Address: {metadata.head_address}")
        print(f"  - Prefer: {metadata.prefer}")
        print(f"  - Weight: {metadata.weight}")
        print(f"  - Home dir: {metadata.home_dir}")
        print(f"  - Tags: {metadata.tags}")
        
    except Exception as e:
        print(f"✗ Error initializing scheduler: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 2: Test submit_task with preferred cluster
    print("\n2. Testing submit_task with preferred cluster 'mac'...")
    try:
        task_id, result = submit_task(
            func=test_function,
            args=(10, 20),
            name="test_mac_cluster_task",
            preferred_cluster="mac",
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        print(f"✓ Task submitted successfully. Task ID: {task_id}, Result: {result}")
        assert result == 30, f"Expected 30, got {result}"
        
    except Exception as e:
        print(f"✗ Error submitting task: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 3: Test submit_task without preferred cluster (should use load balancing)
    print("\n3. Testing submit_task without preferred cluster...")
    try:
        task_id, result = submit_task(
            func=test_function,
            args=(5, 15),
            name="test_load_balancing_task",
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        print(f"✓ Task submitted successfully. Task ID: {task_id}, Result: {result}")
        assert result == 20, f"Expected 20, got {result}"
        
    except Exception as e:
        print(f"✗ Error submitting task: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 4: Test submit_actor with preferred cluster
    print("\n4. Testing submit_actor with preferred cluster 'mac'...")
    try:
        actor_id, actor = submit_actor(
            actor_class=TestActor,
            args=(100,),
            name="test_mac_cluster_actor",
            preferred_cluster="mac",
            runtime_env={
                "conda": "k8s",
                "env_vars": {
                    "home_dir": "/Users/zorro"
                }
            }
        )
        print(f"✓ Actor submitted successfully. Actor ID: {actor_id}")
        
        # Test actor methods
        increment_result = ray.get(actor.increment.remote(50))
        print(f"✓ Actor increment result: {increment_result}")
        assert increment_result == 150, f"Expected 150, got {increment_result}"
        
        value_result = ray.get(actor.get_value.remote())
        print(f"✓ Actor get_value result: {value_result}")
        assert value_result == 150, f"Expected 150, got {value_result}"
        
    except Exception as e:
        print(f"✗ Error submitting actor: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n=== All tests completed successfully ===")
    print("Summary:")
    print("  ✓ Scheduler correctly uses user-specified single mac cluster configuration")
    print("  ✓ submit_task works correctly with preferred cluster")
    print("  ✓ submit_task works correctly with load balancing")
    print("  ✓ submit_actor works correctly with preferred cluster")
    return True

if __name__ == "__main__":
    success = test_single_mac_cluster_with_submit_task()
    sys.exit(0 if success else 1)