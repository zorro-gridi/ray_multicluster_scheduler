"""
Task lifecycle manager that orchestrates the complete task execution flow.
"""

import time
import threading
from typing import Dict, Any, Optional, List
import ray  # 添加ray导入
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.queue.backpressure_controller import BackpressureController
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError
# 添加Dispatcher和相关导入
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.common.circuit_breaker import ClusterCircuitBreakerManager

logger = get_logger(__name__)

# 全局连接管理器实例，确保在整个应用程序生命周期中只初始化一次
_global_client_pool = None
_global_connection_manager = None
_global_circuit_breaker_manager = None


class TaskLifecycleManager:
    """Manages the complete lifecycle of tasks from submission to result collection."""

    def __init__(self, cluster_monitor: ClusterMonitor):
        self.cluster_monitor = cluster_monitor
        self.policy_engine = PolicyEngine(cluster_monitor)
        self.task_queue = TaskQueue(max_size=1000)
        self.backpressure_controller = BackpressureController(threshold=0.8)  # 从0.9改为0.8以匹配规则要求
        self.running = False
        self.worker_thread = None
        self.queued_tasks: List[TaskDescription] = []  # Store queued tasks for re-evaluation

        # 使用全局连接管理器实例（延迟初始化）
        self.client_pool = None
        self.connection_manager = None
        self.circuit_breaker_manager = None
        self._initialized = False

    def _initialize_connections(self):
        """延迟初始化连接管理器和相关组件，确保全局只初始化一次"""
        global _global_client_pool, _global_connection_manager, _global_circuit_breaker_manager

        # 检查是否已经初始化
        if self._initialized:
            return

        try:
            # 如果全局连接管理器还未初始化，则创建它们
            if _global_client_pool is None:
                _global_client_pool = RayClientPool()
            if _global_connection_manager is None:
                _global_connection_manager = ConnectionLifecycleManager(_global_client_pool)
            if _global_circuit_breaker_manager is None:
                _global_circuit_breaker_manager = ClusterCircuitBreakerManager()

            # 设置实例引用
            self.client_pool = _global_client_pool
            self.connection_manager = _global_connection_manager
            self.circuit_breaker_manager = _global_circuit_breaker_manager

            # 注册集群到连接管理器（只在第一次初始化时进行）
            cluster_configs = getattr(self.cluster_monitor.cluster_manager, 'clusters', {})
            successful_registrations = 0
            for cluster_name, cluster_config in cluster_configs.items():
                try:
                    # 检查集群是否已经注册
                    if cluster_name not in self.connection_manager.cluster_metadata:
                        # 将ClusterConfig转换为ClusterMetadata
                        from ray_multicluster_scheduler.common.model import ClusterMetadata
                        cluster_metadata = ClusterMetadata(
                            name=cluster_config.name,
                            head_address=cluster_config.head_address,
                            dashboard=cluster_config.dashboard,
                            prefer=cluster_config.prefer,
                            weight=cluster_config.weight,
                            home_dir=cluster_config.home_dir,
                            conda=cluster_config.conda,  # 新增：传递conda属性
                            tags=cluster_config.tags
                        )
                        self.connection_manager.register_cluster(cluster_metadata)
                        successful_registrations += 1
                    else:
                        logger.info(f"Cluster {cluster_name} already registered, skipping")
                except Exception as e:
                    logger.warning(f"Failed to register cluster {cluster_name} (will retry later): {e}")

            logger.info(f"Successfully registered {successful_registrations}/{len(cluster_configs)} new clusters")

            # 创建dispatcher实例
            self.dispatcher = Dispatcher(self.policy_engine, self.connection_manager, self.circuit_breaker_manager)

            self._initialized = True
            logger.info("Connection managers initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize connection managers: {e}")
            import traceback
            traceback.print_exc()

    def start(self):
        """Start the task lifecycle manager."""
        if self.running:
            logger.warning("Task lifecycle manager is already running")
            return

        # 初始化连接
        self._initialize_connections()

        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("Task lifecycle manager started")

    def stop(self):
        """Stop the task lifecycle manager."""
        if not self.running:
            logger.warning("Task lifecycle manager is not running")
            return

        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)  # Wait up to 5 seconds for thread to finish
        logger.info("Task lifecycle manager stopped")

    def submit_task(self, task_desc: TaskDescription) -> bool:
        """Submit a task to the queue."""
        try:
            # 确保连接已初始化
            if not self._initialized:
                self._initialize_connections()

            # Get cluster snapshots (refresh happens in ClusterMonitor as needed)
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                              if info['snapshot'] is not None}

            # Check if backpressure should be applied
            if self.backpressure_controller.should_apply_backpressure(cluster_snapshots):
                # Apply backpressure by enqueuing the task
                logger.info(f"集群资源使用率超过90%，任务 {task_desc.task_id} 进入排队器等待")
                self.queued_tasks.append(task_desc)  # Track queued tasks
                return self.task_queue.enqueue(task_desc)

            # Update policy engine with current cluster metadata
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Make scheduling decision
            decision = self.policy_engine.schedule(task_desc, cluster_snapshots)

            if not decision or not decision.cluster_name:
                # If no cluster is available, enqueue the task
                logger.warning(f"没有可用集群，任务 {task_desc.task_id} 进入排队器等待")
                self.queued_tasks.append(task_desc)  # Track queued tasks
                return self.task_queue.enqueue(task_desc)

            logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
            # 实际调度任务到选定的集群
            self._process_task(task_desc, cluster_snapshots)

            return True
        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Even on error, we can try to queue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            return self.task_queue.enqueue(task_desc)

    def submit_task_and_get_future(self, task_desc: TaskDescription) -> Optional[Any]:
        """Submit a task and return a future for the result."""
        try:
            # 确保连接已初始化
            if not self._initialized:
                self._initialize_connections()

            # Get cluster snapshots (refresh happens in ClusterMonitor as needed)
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                              if info['snapshot'] is not None}

            # Update policy engine with current cluster metadata
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Make scheduling decision
            decision = self.policy_engine.schedule(task_desc, cluster_snapshots)

            if not decision or not decision.cluster_name:
                logger.error(f"没有可用集群处理任务 {task_desc.task_id}")
                return None

            logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
            # 实际调度任务到选定的集群并返回future
            future = self.dispatcher.dispatch_task(task_desc, cluster_snapshots)
            return future

        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _worker_loop(self):
        """Main worker loop that processes tasks from the queue."""
        last_re_evaluation = time.time()

        while self.running:
            try:
                # Ensure connections are initialized
                if not self._initialized:
                    self._initialize_connections()

                # Refresh cluster resource snapshots periodically
                self.cluster_monitor.refresh_resource_snapshots()

                # Get cluster snapshots
                cluster_info = self.cluster_monitor.get_all_cluster_info()
                cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                                  if info['snapshot'] is not None}

                # Periodically re-evaluate queued tasks (every 30 seconds to reduce pressure)
                current_time = time.time()
                if current_time - last_re_evaluation > 30.0:
                    self._re_evaluate_queued_tasks(cluster_snapshots, cluster_info)
                    last_re_evaluation = current_time

                # Check if backpressure should still be applied
                if self.backpressure_controller.should_apply_backpressure(cluster_snapshots):
                    # Apply backpressure by sleeping
                    backoff_time = self.backpressure_controller.get_backoff_time()
                    logger.info(f"应用背压控制，休眠 {backoff_time} 秒")
                    time.sleep(backoff_time)
                    continue

                # Dequeue a task
                task_desc = self.task_queue.dequeue()
                if not task_desc:
                    # No tasks in queue, sleep briefly
                    time.sleep(0.1)
                    continue

                # Process the task
                self._process_task(task_desc, cluster_snapshots)

            except Exception as e:
                logger.error(f"任务生命周期工作者循环错误: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1.0)  # Sleep to avoid tight loop on repeated errors

    def _re_evaluate_queued_tasks(self, cluster_snapshots: Dict[str, ResourceSnapshot],
                                  cluster_info: Dict[str, Any]):
        """Re-evaluate queued tasks to see if they can be migrated to better clusters."""
        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            # Update policy engine with current cluster metadata
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Get current backpressure status
            backpressure_active = self.backpressure_controller.should_apply_backpressure(cluster_snapshots)

            if not backpressure_active and self.queued_tasks:
                logger.info(f"重新评估 {len(self.queued_tasks)} 个排队任务的调度可能性")

                # Try to reschedule some queued tasks
                remaining_tasks = []
                rescheduled_count = 0

                for task_desc in self.queued_tasks:
                    try:
                        # Make a new scheduling decision
                        decision = self.policy_engine.schedule(task_desc, cluster_snapshots)

                        if decision and decision.cluster_name:
                            # Found a suitable cluster, process this task immediately
                            logger.info(f"任务 {task_desc.task_id} 重新调度到集群 {decision.cluster_name}")
                            self._process_task(task_desc, cluster_snapshots)
                            rescheduled_count += 1
                        else:
                            # Still no suitable cluster, keep in queue
                            remaining_tasks.append(task_desc)
                    except Exception as e:
                        logger.error(f"重新评估任务 {task_desc.task_id} 时出错: {e}")
                        # Keep task in queue on error
                        remaining_tasks.append(task_desc)

                # Update tracked queued tasks
                self.queued_tasks = remaining_tasks

                if rescheduled_count > 0:
                    logger.info(f"成功重新调度 {rescheduled_count} 个任务到更优集群")

        except Exception as e:
            logger.error(f"重新评估排队任务时出错: {e}")
            import traceback
            traceback.print_exc()

    def _process_task(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]):
        """Process a single task."""
        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            logger.info(f"处理任务 {task_desc.task_id}")

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Make scheduling decision
            decision = self.policy_engine.schedule(task_desc, cluster_snapshots)

            if not decision or not decision.cluster_name:
                # If no cluster is available, re-enqueue the task
                logger.warning(f"没有可用集群处理任务 {task_desc.task_id}，重新加入队列")
                self.queued_tasks.append(task_desc)  # Track queued tasks
                self.task_queue.enqueue(task_desc)
                return

            logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
            # 实际调度任务到选定的集群
            future = self.dispatcher.dispatch_task(task_desc, cluster_snapshots)

            # 等待任务执行完成并获取结果
            result = ray.get(future)
            logger.info(f"任务 {task_desc.task_id} 执行完成，结果: {result}")

        except NoHealthyClusterError as e:
            logger.error(f"任务 {task_desc.task_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            self.task_queue.enqueue(task_desc)
        except TaskSubmissionError as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            self.task_queue.enqueue(task_desc)
        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            self.task_queue.enqueue(task_desc)
