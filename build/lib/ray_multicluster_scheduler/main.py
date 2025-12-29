"""
Main entry point for the ray multicluster scheduler.
"""

import time
import ray
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.logging import configure_logging, get_logger
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
from ray_multicluster_scheduler.scheduler.cluster.cluster_metadata import ClusterMetadataManager
from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.control_plane.admin_api import AdminAPI
from ray_multicluster_scheduler.common.circuit_breaker import ClusterCircuitBreakerManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.health.metrics_aggregator import MetricsAggregator
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager
from ray_multicluster_scheduler.app.client_api.unified_scheduler import UnifiedScheduler

logger = get_logger(__name__)


def main():
    """Main entry point for the scheduler."""
    # Configure logging
    configure_logging()
    logger.info("Starting Ray Multi-Cluster Scheduler")

    # Load configuration
    config_manager = ConfigManager()
    cluster_configs = config_manager.get_cluster_configs()

    # Initialize components
    # 1. Cluster metadata management
    metadata_manager = ClusterMetadataManager(cluster_configs)

    # 2. Health checking
    client_pool = RayClientPool(config_manager)
    health_checker = HealthChecker(cluster_configs, client_pool)

    # 3. Cluster registry
    cluster_registry = ClusterRegistry(metadata_manager, health_checker)

    # 4. Connection management
    # 初始化连接管理器，不初始化job_client_pool（仅在提交作业时按需初始化）
    connection_manager = ConnectionLifecycleManager(client_pool, initialize_job_client_pool_on_init=False)

    # Register clusters with connection manager
    for cluster_config in cluster_configs:
        try:
            connection_manager.register_cluster(cluster_config)
        except Exception as e:
            logger.error(f"Failed to register cluster {cluster_config.name}: {e}")

    # 10. Cluster Monitor
    # Extract config file path or use None to let ClusterMonitor use default
    cluster_monitor = ClusterMonitor()  # Will use default config or attempt to load from standard locations

    # 5. Policy engine
    policy_engine = PolicyEngine(cluster_monitor)

    # 6. Circuit breaker manager
    circuit_breaker_manager = ClusterCircuitBreakerManager()

    # 7. Dispatcher
    dispatcher = Dispatcher(connection_manager, circuit_breaker_manager)

    # 10. Task lifecycle manager
    task_lifecycle_manager = TaskLifecycleManager(
        cluster_monitor=cluster_monitor
    )

    # 11. Metrics Aggregator
    metrics_aggregator = MetricsAggregator(health_checker, task_lifecycle_manager.task_queue, cluster_registry, cluster_monitor)

    # 12. Admin API
    admin_api = AdminAPI(cluster_registry, task_lifecycle_manager.task_queue, health_checker, metrics_aggregator, cluster_monitor)

    # Initialize unified scheduler for health checker thread management
    unified_scheduler = UnifiedScheduler()
    unified_scheduler.initialize_environment()

    # Start the task lifecycle manager
    task_lifecycle_manager.start()

    # Keep the scheduler running
    try:
        logger.info("Ray Multi-Cluster Scheduler is running. Press Ctrl+C to stop.")
        while True:
            # Print some stats every 30 seconds
            time.sleep(30)
            stats = admin_api.get_scheduler_stats()
            logger.info(f"Scheduler stats: {stats}")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        # Stop the task lifecycle manager
        task_lifecycle_manager.stop()
        # Clean up unified scheduler resources
        unified_scheduler.cleanup()
        logger.info("Ray Multi-Cluster Scheduler stopped")


if __name__ == "__main__":
    main()