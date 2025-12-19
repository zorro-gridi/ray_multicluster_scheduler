#!/usr/bin/env python3
"""
Simple test script to verify cluster configuration loading.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor

def test_cluster_config_loading():
    """Test that cluster configurations are loaded correctly."""
    print("=== Testing Cluster Configuration Loading ===")

    # Test 1: Load default configuration
    print("\n1. Testing default configuration loading...")
    config_manager = ConfigManager()
    clusters = config_manager.get_cluster_configs()

    print(f"Loaded {len(clusters)} clusters:")
    for cluster in clusters:
        print(f"  - {cluster.name}: {cluster.head_address} (prefer={cluster.prefer}, conda={cluster.conda})")

    # Test 2: Initialize cluster monitor with default config
    print("\n2. Testing cluster monitor initialization...")
    cluster_monitor = ClusterMonitor()

    print("Cluster configurations in monitor:")
    for name, config in cluster_monitor.cluster_manager.clusters.items():
        print(f"  - {name}: {config.head_address} (prefer={config.prefer}, conda={config.conda})")

    print("\n=== Test completed ===")

if __name__ == "__main__":
    test_cluster_config_loading()