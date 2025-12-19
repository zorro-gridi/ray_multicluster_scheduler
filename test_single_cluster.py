#!/usr/bin/env python3
"""
Test script to verify cluster scheduling with only mac cluster configuration.
"""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import UnifiedScheduler
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor

@ray.remote
def test_function(x, y):
    """Simple test function that adds two numbers."""
    time.sleep(0.1)  # Simulate some work
    return x + y

def test_single_cluster_configuration():
    """Test cluster scheduling with only mac cluster."""
    print("=== Testing Single Cluster Configuration (Mac Only) ===")

    # Test 1: Initialize cluster monitor with mac-only configuration
    print("\n1. Testing cluster monitor initialization with mac-only config...")
    try:
        cluster_monitor = ClusterMonitor(config_file_path="./mac_only_clusters.yaml")
        print("✓ ClusterMonitor initialized successfully")

        # Check cluster configurations
        print("Cluster configurations:")
        for name, config in cluster_monitor.cluster_manager.clusters.items():
            print(f"  - {name}: {config.head_address} (prefer={config.prefer}, conda={config.conda})")

        # Check cluster info
        cluster_info = cluster_monitor.get_all_cluster_info()
        print(f"Cluster info loaded for {len(cluster_info)} clusters:")
        for name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']
            print(f"  - {name}: head_address={metadata.head_address}, conda={getattr(metadata, 'conda', 'N/A')}")
            if snapshot and snapshot.available_resources:
                print(f"    Resources: {snapshot.available_resources}")
            else:
                print(f"    Resources: Not available")

    except Exception as e:
        print(f"✗ Error initializing ClusterMonitor: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Test 2: Initialize unified scheduler with mac-only configuration
    print("\n2. Testing unified scheduler initialization with mac-only config...")
    try:
        scheduler = UnifiedScheduler()
        task_lifecycle_manager = scheduler.initialize_environment(config_file_path="./mac_only_clusters.yaml")
        print("✓ UnifiedScheduler initialized successfully")

        # Check that we have only one cluster
        cluster_info = task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
        print(f"Available clusters: {list(cluster_info.keys())}")
        assert len(cluster_info) == 1, f"Expected 1 cluster, got {len(cluster_info)}"
        assert "mac" in cluster_info, "Expected 'mac' cluster to be available"

    except Exception as e:
        print(f"✗ Error initializing UnifiedScheduler: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n=== All tests completed successfully ===")
    return True

if __name__ == "__main__":
    success = test_single_cluster_configuration()
    sys.exit(0 if success else 1)