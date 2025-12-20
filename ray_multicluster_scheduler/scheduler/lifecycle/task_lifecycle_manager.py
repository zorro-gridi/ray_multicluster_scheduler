import ray
import time
import logging
import threading
from typing import Dict, List, Optional, Any, Callable
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
# 注释掉不存在的模块
# from ray_multicluster_scheduler.scheduler.backpressure.backpressure_controller import BackpressureController
from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TaskLifecycleManager:
    """Manages the lifecycle of tasks in the multicluster scheduler."""

    def __init__(self, cluster_monitor: ClusterMonitor):
        self.cluster_monitor = cluster_monitor
        self.policy_engine = PolicyEngine()
        # 初始化client_pool和connection_manager
        self.client_pool = RayClientPool()
        self.connection_manager = ConnectionLifecycleManager(self.client_pool)
        self.dispatcher = Dispatcher(self.policy_engine, self.connection_manager)
        self.task_queue = TaskQueue()
        # 注释掉不存在的模块
        # self.backpressure_controller = BackpressureController()
        self.running = False
        self.worker_thread = None
        self._initialized = False
        # Track queued tasks for re-evaluation
        self.queued_tasks: List[TaskDescription] = []

    def start(self):
        """Start the task lifecycle manager."""
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            logger.info("Task lifecycle manager started")

    def stop(self):
        """Stop the task lifecycle manager."""
        logger.info("Stopping task lifecycle manager...")
        self.running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5.0)  # Wait up to 5 seconds for graceful shutdown
            if self.worker_thread.is_alive():
                logger.warning("Task lifecycle manager worker thread did not stop gracefully")
        logger.info("Task lifecycle manager stopped")

    def _initialize_connections(self):
        """Initialize connections to all registered clusters."""
        if self._initialized:
            return

        try:
            logger.info("Initializing cluster connections...")

            # 获取集群配置
            cluster_configs = getattr(self.cluster_monitor.cluster_manager, 'clusters', {})

            # 注册集群到连接管理器（只在第一次初始化时进行）
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
                            runtime_env=cluster_config.runtime_env,  # 使用runtime_env属性
                            tags=cluster_config.tags
                        )
                        self.connection_manager.register_cluster(cluster_metadata)
                        successful_registrations += 1
                    else:
                        logger.info(f"Cluster {cluster_name} already registered, skipping")
                except Exception as e:
                    logger.warning(f"Failed to register cluster {cluster_name} (will retry later): {e}")

            logger.info(f"Initialized connections to {successful_registrations} clusters")
            self._initialized = True

        except Exception as e:
            logger.error(f"Failed to initialize cluster connections: {e}")
            raise

    def submit_task(self, task_desc: TaskDescription) -> Optional[str]:
        """Submit a task to the scheduler."""
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

            # 如果决策返回空集群名称，说明任务需要排队
            if not decision or not decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 需要排队等待资源释放")
                # 将任务加入队列
                self.queued_tasks.append(task_desc)
                self.task_queue.enqueue(task_desc)
                return task_desc.task_id

            logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
            # 实际调度任务到选定的集群
            future = self.dispatcher.dispatch_task(task_desc, cluster_snapshots)

            # 等待任务执行完成并获取结果
            result = ray.get(future)
            logger.info(f"任务 {task_desc.task_id} 执行完成，结果: {result}")
            return task_desc.task_id

        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Even on error, we can try to queue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            self.task_queue.enqueue(task_desc)
            return task_desc.task_id

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

            # 如果决策返回空集群名称，说明任务需要排队
            if not decision or not decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 需要排队等待资源释放")
                # 将任务加入队列
                self.queued_tasks.append(task_desc)
                self.task_queue.enqueue(task_desc)
                return None

            logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
            # 实际调度任务到选定的集群并返回future
            future = self.dispatcher.dispatch_task(task_desc, cluster_snapshots)
            return future

        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # 将任务加入队列以便稍后重试
            self.queued_tasks.append(task_desc)
            self.task_queue.enqueue(task_desc)
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
                # 注释掉不存在的模块调用
                # if self.backpressure_controller.should_apply_backpressure(cluster_snapshots):
                if False:  # 暂时禁用背压控制
                    # Apply backpressure by sleeping
                    # backoff_time = self.backpressure_controller.get_backoff_time()
                    backoff_time = 1.0  # 默认休眠时间
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
            # 注释掉不存在的模块调用
            # backpressure_active = self.backpressure_controller.should_apply_backpressure(cluster_snapshots)
            backpressure_active = False  # 暂时禁用背压控制

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

            # 如果决策返回空集群名称，说明任务仍需要排队
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