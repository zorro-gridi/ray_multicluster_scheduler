"""
Main entry point for the ray multicluster scheduler.
"""

import time
import signal
import sys
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
from ray_multicluster_scheduler.scheduler.lifecycle.job_tracker import JobTracker

logger = get_logger(__name__)

# Global flag for graceful shutdown
_shutdown_flag = False


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    global _shutdown_flag
    _shutdown_flag = True
    logger.info("\n🛑 收到中断信号，开始优雅关闭调度器...")


def shutdown_scheduler(task_lifecycle_manager, unified_scheduler, connection_manager, job_tracker=None):
    """Gracefully shutdown the scheduler and cleanup all resources."""
    logger.info("正在终止所有调度进程和Ray任务...")

    # 1. Stop the task lifecycle manager (stops processing new tasks)
    logger.info("1️⃣ 停止任务生命周期管理器...")
    task_lifecycle_manager.stop()

    # 2. Cancel all running tasks and jobs
    logger.info("2️⃣ 取消所有正在运行的任务和作业...")
    try:
        # Get all running tasks from Ray
        if ray.is_initialized():
            # List all running tasks/actors and cancel them
            try:
                # Cancel all pending tasks in the task queue
                cancelled_count = 0
                while True:
                    task = task_lifecycle_manager.task_queue.dequeue()
                    if task is None:
                        break
                    logger.info(f"   取消待处理任务: {task.task_id if hasattr(task, 'task_id') else 'unknown'}")
                    cancelled_count += 1

                # Also clear cluster-specific queues
                for cluster_name in task_lifecycle_manager.task_queue.get_cluster_queue_names():
                    while True:
                        task = task_lifecycle_manager.task_queue.dequeue_from_cluster(cluster_name)
                        if task is None:
                            break
                        logger.info(f"   取消集群 {cluster_name} 的待处理任务: {task.task_id if hasattr(task, 'task_id') else 'unknown'}")
                        cancelled_count += 1

                logger.info(f"   ✅ 已取消 {cancelled_count} 个待处理任务")
            except Exception as e:
                logger.warning(f"   ⚠️ 取消任务时出错: {e}")
    except Exception as e:
        logger.error(f"取消运行中的任务时出错: {e}")

    # 3. Stop all tracked jobs (submitted by this scheduler)
    logger.info("3️⃣ 停止本次调度提交的所有作业...")
    try:
        job_client_pool = connection_manager.job_client_pool
        if job_client_pool and job_tracker:
            tracked_jobs = job_tracker.get_tracked_jobs()
            total_tracked = job_tracker.get_total_count()

            if total_tracked > 0:
                logger.info(f"   发现 {total_tracked} 个需要停止的作业")

                for cluster_name, job_ids in tracked_jobs.items():
                    if not job_ids:
                        continue

                    logger.info(f"   集群 {cluster_name} 上有 {len(job_ids)} 个待停止的作业")
                    stopped_count = 0

                    for job_id in list(job_ids):  # 使用 list() 避免迭代时修改
                        try:
                            # 不管状态如何，直接尝试停止
                            # 这样可以确保即使 Job 已完成但进程未退出的情况也能被清理
                            job_client_pool.stop_job(cluster_name, job_id)
                            logger.info(f"   停止作业: {job_id}")

                            # 从跟踪列表中移除
                            job_tracker.unregister_job(cluster_name, job_id)
                            stopped_count += 1

                        except Exception as e:
                            logger.warning(f"   ⚠️ 停止作业 {job_id} 失败: {e}")
                            # 即使失败也从跟踪列表移除，避免重复尝试
                            job_tracker.unregister_job(cluster_name, job_id)

                    if stopped_count > 0:
                        logger.info(f"   ✅ 集群 {cluster_name} 上已停止 {stopped_count} 个作业")
            else:
                logger.info("   没有需要停止的作业")

        elif not job_tracker:
            logger.warning("   ⚠️ Job 跟踪器未初始化，跳过 Job 停止")

    except Exception as e:
        logger.error(f"停止作业时出错: {e}")
        import traceback
        traceback.print_exc()

    # 4. Clean up unified scheduler resources (health checker thread)
    logger.info("4️⃣ 清理统一调度器资源...")
    unified_scheduler.cleanup()

    # 5. Disconnect from all Ray clusters
    logger.info("5️⃣ 断开所有集群连接...")
    try:
        # Shutdown Ray if initialized
        if ray.is_initialized():
            ray.shutdown()
            logger.info("   ✅ Ray 已关闭")
    except Exception as e:
        logger.error(f"关闭Ray连接时出错: {e}")

    logger.info("✅ 调度器已优雅关闭，所有资源已清理")


def main():
    """Main entry point for the scheduler."""
    global _shutdown_flag

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Configure logging
    configure_logging()
    logger.info("Starting Ray Multi-Cluster Scheduler")

    # Initialize unified scheduler which will start the detached monitor
    unified_scheduler = UnifiedScheduler()
    task_lifecycle_manager = unified_scheduler.initialize_environment()

    # Initialize components that depend on task_lifecycle_manager
    cluster_monitor = task_lifecycle_manager.cluster_monitor
    connection_manager = task_lifecycle_manager.connection_manager
    job_tracker = task_lifecycle_manager.job_tracker

    # Initialize admin API
    from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
    from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
    from ray_multicluster_scheduler.scheduler.health.metrics_aggregator import MetricsAggregator
    from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

    # Create required components for AdminAPI
    cluster_registry = ClusterRegistry(cluster_monitor.cluster_manager, None)  # HealthChecker will be passed later if needed
    health_checker = HealthChecker([], cluster_monitor.client_pool)  # Will be updated with actual clusters
    metrics_aggregator = MetricsAggregator(health_checker, task_lifecycle_manager.task_queue, cluster_registry, cluster_monitor)
    admin_api = AdminAPI(cluster_registry, task_lifecycle_manager.task_queue, health_checker, metrics_aggregator, cluster_monitor)

    # Start the task lifecycle manager
    task_lifecycle_manager.start()

    # Keep the scheduler running
    try:
        logger.info("Ray Multi-Cluster Scheduler is running. Press Ctrl+C to stop.")
        while not _shutdown_flag:
            # Print some stats every 30 seconds
            for _ in range(30):
                if _shutdown_flag:
                    break
                time.sleep(1)
            if not _shutdown_flag:
                stats = admin_api.get_scheduler_stats()
                logger.info(f"Scheduler stats: {stats}")
    except KeyboardInterrupt:
        logger.info("\n🛑 收到键盘中断信号...")
    finally:
        # Gracefully shutdown the scheduler
        shutdown_scheduler(task_lifecycle_manager, unified_scheduler, connection_manager, job_tracker)
        logger.info("Ray Multi-Cluster Scheduler stopped")


if __name__ == "__main__":
    main()