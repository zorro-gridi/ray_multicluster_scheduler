"""
Script to start the detached cluster resource monitor actor.
This script should be run once to deploy the monitor as a daemon service.
"""
import ray
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_resource_monitor import ClusterResourceMonitor


def start_cluster_monitor(config_file_path: str = "clusters.yaml"):
    """
    Start the detached cluster resource monitor.

    Args:
        config_file_path: Path to the cluster configuration file
    """
    # Initialize Ray if not already initialized
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    # Load cluster configurations
    config_manager = ConfigManager(config_file_path)
    cluster_configs = config_manager.get_cluster_configs()

    # Start the detached monitor actor
    monitor = ClusterResourceMonitor.remote(cluster_configs)

    print(f"Cluster resource monitor started as a detached actor.")
    print(f"Actor name: cluster_resource_monitor")
    print(f"Configured for {len(cluster_configs)} clusters: {list(cluster_configs.keys())}")

    # Verify the monitor is running
    try:
        result = ray.get(monitor.ping.remote())
        print(f"Monitor health check: {result}")
    except Exception as e:
        print(f"Error checking monitor health: {e}")

    return monitor


if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else "clusters.yaml"
    monitor = start_cluster_monitor(config_path)
    print("Monitor started successfully. It will continue running as a detached actor.")