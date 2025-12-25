import ray
import time
import logging
import threading
from typing import Dict, List, Optional, Any, Callable
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class TaskLifecycleManager:
    """Manages the lifecycle of tasks in the multicluster scheduler."""

    def __init__(self, cluster_monitor: ClusterMonitor):
        self.cluster_monitor = cluster_monitor
        self.policy_engine = PolicyEngine()
        # 初始化client_pool和connection_manager
        self.client_pool = cluster_monitor.client_pool
        self.connection_manager = ConnectionLifecycleManager(self.client_pool)
        self.dispatcher = Dispatcher(self.connection_manager)
        self.task_queue = TaskQueue()
        self.running = False
        self.worker_thread = None
        self._initialized = False
        # Track queued tasks for re-evaluation
        self.queued_tasks: List[TaskDescription] = []
        # Track queued jobs for re-evaluation
        self.queued_jobs: List[JobDescription] = []

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

            # 初始化 JobClientPool
            self.connection_manager.initialize_job_client_pool(self.cluster_monitor.config_manager)

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

            if decision and decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
                # 实际调度任务到选定的集群
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)

                # 等待任务执行完成并获取结果
                result = ray.get(future)
                logger.info(f"任务 {task_desc.task_id} 执行完成，结果: {result}")
                return task_desc.task_id
            else:
                logger.info(f"任务 {task_desc.task_id} 需要排队等待资源释放")
                # 将任务加入队列，如果是首选集群资源紧张，则加入该集群的队列
                self.queued_tasks.append(task_desc)
                if task_desc.preferred_cluster:
                    self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue(task_desc)
                return task_desc.task_id

        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Even on error, we can try to queue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
            return task_desc.task_id

    def submit_job(self, job_desc: JobDescription) -> Optional[str]:
        """Submit a job to the scheduler using JobSubmissionClient."""
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

            # Make scheduling decision for job
            decision = self.policy_engine.schedule_job(job_desc, cluster_snapshots)

            if decision and decision.cluster_name:
                logger.info(f"作业 {job_desc.job_id} 调度到集群 {decision.cluster_name}")

                # 获取目标集群的配置，用于路径转换
                target_cluster_metadata = cluster_metadata[decision.cluster_name]

                # 执行路径转换
                converted_job_desc = self._convert_job_path(job_desc, target_cluster_metadata)

                # 实际调度作业到选定的集群
                job_id = self.dispatcher.dispatch_job(converted_job_desc, decision.cluster_name)
                logger.info(f"作业 {job_desc.job_id} 提交完成，作业ID: {job_id}")
                return job_id
            else:
                logger.info(f"作业 {job_desc.job_id} 需要排队等待资源释放")
                # 将作业加入队列，如果是首选集群资源紧张，则加入该集群的队列
                self.queued_jobs.append(job_desc)
                if job_desc.preferred_cluster:
                    self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue_job(job_desc)
                return job_desc.job_id

        except Exception as e:
            logger.error(f"作业 {job_desc.job_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Even on error, we can try to queue the job for later retry
            self.queued_jobs.append(job_desc)  # Track queued jobs
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)
            return job_desc.job_id

    def _convert_job_path(self, job_desc: JobDescription, target_cluster_metadata) -> JobDescription:
        """Convert job entrypoint paths based on target cluster configuration."""
        import re
        import copy

        # 复制 job_desc 以避免修改原始对象
        new_job_desc = copy.deepcopy(job_desc)

        # 提取entrypoint中的文件路径部分
        entrypoint = new_job_desc.entrypoint

        # 从目标集群的runtime_env中获取home_dir
        target_home_dir = None
        if hasattr(target_cluster_metadata, 'runtime_env') and target_cluster_metadata.runtime_env:
            env_vars = target_cluster_metadata.runtime_env.get('env_vars', {})
            target_home_dir = env_vars.get('home_dir')

        # 如果没有找到目标集群的home_dir，直接返回原对象
        if not target_home_dir:
            logger.warning(f"目标集群 {target_cluster_metadata.name} 没有配置 home_dir，跳过路径转换")
            return new_job_desc

        # 检查是否是python命令并提取路径
        path_match = re.search(r'\bpython\s+(/[^\s]+)', entrypoint, re.IGNORECASE)
        if path_match:
            script_path = path_match.group(1)

            # 检查当前路径是否包含源集群的home_dir模式
            # 假设当前路径是基于某个已知的home_dir格式
            possible_source_home_dirs = ['/home/', '/Users/', '/root/']

            for source_home in possible_source_home_dirs:
                if script_path.startswith(source_home):
                    # 提取相对路径部分，移除源home_dir部分（包含用户名）
                    # 例如: /Users/zorro/project/script.py -> project/script.py
                    source_username_start = len(source_home)  # 例如 /Users/ 的长度
                    # 找到用户名后的斜杠位置
                    remaining_path = script_path[source_username_start:]
                    username_end = remaining_path.find('/')
                    if username_end != -1:
                        # 移除用户名，保留剩余路径
                        relative_path = remaining_path[username_end + 1:]
                    else:
                        # 如果没有找到用户名，只移除源home_dir
                        relative_path = remaining_path

                    # 构建目标路径 - 保留目标home_dir的完整路径（包括用户名）
                    target_path = f"{target_home_dir}/{relative_path}"

                    # 替换entrypoint中的路径
                    new_entrypoint = entrypoint.replace(script_path, target_path)
                    new_job_desc.entrypoint = new_entrypoint

                    logger.info(f"路径转换: {script_path} -> {target_path}")
                    break
        else:
            # 如果不是python命令，检查其他可能的绝对路径
            possible_paths = re.findall(r'(/[^\s]+)', entrypoint)
            for path in possible_paths:
                if path.startswith('/'):
                    # 检查是否包含源home_dir模式
                    possible_source_home_dirs = ['/home/', '/Users/', '/root/']

                    for source_home in possible_source_home_dirs:
                        if path.startswith(source_home):
                            # 提取相对路径部分，移除源home_dir部分（包含用户名）
                            # 例如: /Users/zorro/project/script.py -> project/script.py
                            source_username_start = len(source_home)  # 例如 /Users/ 的长度
                            # 找到用户名后的斜杠位置
                            remaining_path = path[source_username_start:]
                            username_end = remaining_path.find('/')
                            if username_end != -1:
                                # 移除用户名，保留剩余路径
                                relative_path = remaining_path[username_end + 1:]
                            else:
                                # 如果没有找到用户名，只移除源home_dir
                                relative_path = remaining_path

                            # 构建目标路径 - 保留目标home_dir的完整路径（包括用户名）
                            target_path = f"{target_home_dir}/{relative_path}"

                            # 替换entrypoint中的路径
                            new_entrypoint = new_job_desc.entrypoint.replace(path, target_path)
                            new_job_desc.entrypoint = new_entrypoint

                            logger.info(f"路径转换: {path} -> {target_path}")
                            break

        return new_job_desc

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

            if decision and decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
                # 实际调度任务到选定的集群并返回future
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)
                return future
            else:
                logger.info(f"任务 {task_desc.task_id} 需要排队等待资源释放")
                # 将任务加入队列，如果是首选集群资源紧张，则加入该集群的队列
                self.queued_tasks.append(task_desc)
                if task_desc.preferred_cluster:
                    self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue(task_desc)
                return None

        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # 将任务加入队列以便稍后重试
            self.queued_tasks.append(task_desc)
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
            return None

    def _worker_loop(self):
        """Main worker loop that processes tasks and jobs from the queue."""
        last_re_evaluation = time.time()

        while self.running:
            try:
                # Ensure connections are initialized
                if not self._initialized:
                    self._initialize_connections()

                # Get cluster snapshots
                cluster_info = self.cluster_monitor.get_all_cluster_info()
                cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                                  if info['snapshot'] is not None}

                # Periodically re-evaluate queued tasks and jobs (every 30 seconds to reduce pressure)
                current_time = time.time()
                if current_time - last_re_evaluation > 30.0:
                    self._re_evaluate_queued_tasks(cluster_snapshots, cluster_info)
                    self._re_evaluate_queued_jobs(cluster_snapshots, cluster_info)
                    last_re_evaluation = current_time

                # Try to get a task from cluster-specific queues first, then from global queue
                task_desc = None
                source_cluster = None
                task_type = 'task'  # Flag to indicate whether we're processing a task or a job

                # Check if there are any cluster-specific queues with tasks
                cluster_queue_names = self.task_queue.get_cluster_queue_names()

                if cluster_queue_names:
                    # Try to get tasks from cluster queues first
                    for cluster_name in cluster_queue_names:
                        # Try to get a task from this specific cluster queue
                        task_desc = self.task_queue.dequeue_from_cluster(cluster_name)
                        if task_desc:
                            source_cluster = cluster_name
                            break

                # If no task from cluster queues, try global queue
                if not task_desc:
                    task_desc = self.task_queue.dequeue()
                    # source_cluster remains None for global queue tasks

                # If no tasks in task queues, try job queues
                if not task_desc:
                    job_desc = None
                    cluster_job_queue_names = self.task_queue.get_cluster_job_queue_names()

                    if cluster_job_queue_names:
                        # Try to get jobs from cluster job queues first
                        for cluster_name in cluster_job_queue_names:
                            # Try to get a job from this specific cluster job queue
                            job_desc = self.task_queue.dequeue_from_cluster_job(cluster_name)
                            if job_desc:
                                source_cluster = cluster_name
                                task_type = 'job'
                                break

                    # If no job from cluster job queues, try global job queue
                    if not job_desc:
                        job_desc = self.task_queue.dequeue_job()
                        # source_cluster remains None for global queue jobs
                        if job_desc:
                            task_type = 'job'

                    if job_desc:
                        task_desc = job_desc

                if not task_desc:
                    # No tasks or jobs in any queue, sleep briefly
                    time.sleep(0.1)
                    continue

                # Process the task or job, passing the source cluster information
                if task_type == 'job':
                    self._process_job(task_desc, cluster_snapshots, source_cluster)
                else:
                    self._process_task(task_desc, cluster_snapshots, source_cluster)

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
                            # 重新评估的任务来自全局排队列表，没有特定的源集群
                            self._process_task(task_desc, cluster_snapshots, None)
                            rescheduled_count += 1
                        else:
                            # Still no suitable cluster, keep in queue
                            remaining_tasks.append(task_desc)
                    except Exception as e:
                        logger.error(f"重新评估任务 {task_desc.task_id} 时出错: {e}")
                        # 如果是首选集群不可用的异常，直接记录错误并重新加入队列
                        if "不在线或无法连接" in str(e):
                            logger.error(f"首选集群不可用，任务 {task_desc.task_id} 重新加入队列: {e}")
                        # Keep task in queue on error - add back to appropriate queue
                        if task_desc.preferred_cluster:
                            self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                        else:
                            self.task_queue.enqueue(task_desc)
                        remaining_tasks.append(task_desc)

                # Update tracked queued tasks
                self.queued_tasks = remaining_tasks

                if rescheduled_count > 0:
                    logger.info(f"成功重新调度 {rescheduled_count} 个任务到更优集群")

        except Exception as e:
            logger.error(f"重新评估排队任务时出错: {e}")
            import traceback
            traceback.print_exc()

    def _re_evaluate_queued_jobs(self, cluster_snapshots: Dict[str, ResourceSnapshot],
                                cluster_info: Dict[str, Any]):
        """Re-evaluate queued jobs to see if they can be migrated to better clusters."""
        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            # Update policy engine with current cluster metadata
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Get current backpressure status
            backpressure_active = False  # 暂时禁用背压控制

            if not backpressure_active and self.queued_jobs:
                logger.info(f"重新评估 {len(self.queued_jobs)} 个排队作业的调度可能性")

                # Try to reschedule some queued jobs
                remaining_jobs = []
                rescheduled_count = 0

                for job_desc in self.queued_jobs:
                    try:
                        # Make a new scheduling decision for job
                        decision = self.policy_engine.schedule_job(job_desc, cluster_snapshots)

                        if decision and decision.cluster_name:
                            # Found a suitable cluster, process this job immediately
                            logger.info(f"作业 {job_desc.job_id} 重新调度到集群 {decision.cluster_name}")
                            # 重新评估的作业来自全局排队列表，没有特定的源集群
                            self._process_job(job_desc, cluster_snapshots, None)
                            rescheduled_count += 1
                        else:
                            # Still no suitable cluster, keep in queue
                            remaining_jobs.append(job_desc)
                    except Exception as e:
                        logger.error(f"重新评估作业 {job_desc.job_id} 时出错: {e}")
                        # 如果是首选集群不可用的异常，直接记录错误并重新加入队列
                        if "不在线或无法连接" in str(e):
                            logger.error(f"首选集群不可用，作业 {job_desc.job_id} 重新加入队列: {e}")
                        # Keep job in queue on error - add back to appropriate queue
                        if job_desc.preferred_cluster:
                            self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                        else:
                            self.task_queue.enqueue_job(job_desc)
                        remaining_jobs.append(job_desc)

                # Update tracked queued jobs
                self.queued_jobs = remaining_jobs

                if rescheduled_count > 0:
                    logger.info(f"成功重新调度 {rescheduled_count} 个作业到更优集群")

        except Exception as e:
            logger.error(f"重新评估排队作业时出错: {e}")
            import traceback
            traceback.print_exc()

    def _process_task(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot], source_cluster_queue: str = None):
        """Process a single task.

        Args:
            task_desc: The task to process
            cluster_snapshots: Current cluster resource snapshots
            source_cluster_queue: Name of the cluster queue this task came from, if any
        """
        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            logger.info(f"处理任务 {task_desc.task_id}")

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # 如果任务来自特定集群队列，直接调度到该集群（符合规则1：指定集群任务排队队列中的task只能进入指定集群执行）
            if source_cluster_queue:
                logger.info(f"任务 {task_desc.task_id} 来自集群 {source_cluster_queue} 的队列，直接调度到该集群")
                # 检查目标集群是否健康可用
                if source_cluster_queue not in cluster_snapshots:
                    logger.error(f"目标集群 {source_cluster_queue} 不在线或无法连接，任务 {task_desc.task_id} 重新加入队列")
                    self.queued_tasks.append(task_desc)  # Track queued tasks
                    if task_desc.preferred_cluster:
                        self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                    else:
                        self.task_queue.enqueue(task_desc)
                    return

                # 检查目标集群资源是否仍然紧张
                snapshot = cluster_snapshots[source_cluster_queue]
                # 使用新的资源指标
                cpu_used_cores = snapshot.cluster_cpu_used_cores
                cpu_total_cores = snapshot.cluster_cpu_total_cores
                # 估算可用CPU资源
                cpu_available = cpu_total_cores - cpu_used_cores
                cpu_total = cpu_total_cores

                # 假设没有GPU指标，使用默认值
                gpu_available = 0
                gpu_total = 0

                cpu_utilization = cpu_used_cores / cpu_total_cores if cpu_total_cores > 0 else 0
                gpu_utilization = 0  # GPU指标暂不可用

                # 检查内存使用率
                mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                # 检查资源使用率是否仍超过阈值
                if cpu_utilization > self.policy_engine.RESOURCE_THRESHOLD or gpu_utilization > self.policy_engine.RESOURCE_THRESHOLD or mem_utilization > self.policy_engine.RESOURCE_THRESHOLD:
                    logger.warning(f"目标集群 {source_cluster_queue} 资源使用率仍然超过阈值，任务 {task_desc.task_id} 重新进入该集群队列等待")
                    # 重新加入该集群的队列等待
                    self.task_queue.enqueue(task_desc, source_cluster_queue)
                    self.queued_tasks.append(task_desc)  # Track queued tasks
                    return

                # 直接调度到指定集群
                future = self.dispatcher.dispatch_task(task_desc, source_cluster_queue)
                result = ray.get(future)
                logger.info(f"任务 {task_desc.task_id} 在集群 {source_cluster_queue} 执行完成，结果: {result}")
                return

            # 如果任务来自全局队列，按照规则2：需要经过policy调度策略评估，决策目标调度集群
            decision = self.policy_engine.schedule(task_desc, cluster_snapshots)

            if decision and decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
                # 实际调度任务到选定的集群
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)

                # 等待任务执行完成并获取结果
                result = ray.get(future)
                logger.info(f"任务 {task_desc.task_id} 执行完成，结果: {result}")
            else:
                # If no cluster is available, re-enqueue the task
                logger.warning(f"没有可用集群处理任务 {task_desc.task_id}，重新加入队列")
                self.queued_tasks.append(task_desc)  # Track queued tasks
                if task_desc.preferred_cluster:
                    self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue(task_desc)
                return

        except NoHealthyClusterError as e:
            logger.error(f"任务 {task_desc.task_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
        except TaskSubmissionError as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the task for later retry
            self.queued_tasks.append(task_desc)  # Track queued tasks
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)

    def _process_job(self, job_desc: JobDescription, cluster_snapshots: Dict[str, ResourceSnapshot], source_cluster_queue: str = None):
        """Process a single job.

        Args:
            job_desc: The job to process
            cluster_snapshots: Current cluster resource snapshots
            source_cluster_queue: Name of the cluster queue this job came from, if any
        """
        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            logger.info(f"处理作业 {job_desc.job_id}")

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # 如果作业来自特定集群队列，直接调度到该集群
            if source_cluster_queue:
                logger.info(f"作业 {job_desc.job_id} 来自集群 {source_cluster_queue} 的队列，直接调度到该集群")
                # 检查目标集群是否健康可用
                if source_cluster_queue not in cluster_snapshots:
                    logger.error(f"目标集群 {source_cluster_queue} 不在线或无法连接，作业 {job_desc.job_id} 重新加入队列")
                    self.queued_jobs.append(job_desc)  # Track queued jobs
                    if job_desc.preferred_cluster:
                        self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                    else:
                        self.task_queue.enqueue_job(job_desc)
                    return

                # 检查目标集群资源是否仍然紧张
                snapshot = cluster_snapshots[source_cluster_queue]
                # 使用新的资源指标
                cpu_used_cores = snapshot.cluster_cpu_used_cores
                cpu_total_cores = snapshot.cluster_cpu_total_cores
                # 估算可用CPU资源
                cpu_available = cpu_total_cores - cpu_used_cores
                cpu_total = cpu_total_cores

                # 假设没有GPU指标，使用默认值
                gpu_available = 0
                gpu_total = 0

                cpu_utilization = cpu_used_cores / cpu_total_cores if cpu_total_cores > 0 else 0
                gpu_utilization = 0  # GPU指标暂不可用

                # 检查内存使用率
                mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                # 检查资源使用率是否仍超过阈值
                if cpu_utilization > self.policy_engine.RESOURCE_THRESHOLD or gpu_utilization > self.policy_engine.RESOURCE_THRESHOLD or mem_utilization > self.policy_engine.RESOURCE_THRESHOLD:
                    logger.warning(f"目标集群 {source_cluster_queue} 资源使用率仍然超过阈值，作业 {job_desc.job_id} 重新进入该集群队列等待")
                    # 重新加入该集群的队列等待
                    self.task_queue.enqueue_job(job_desc, source_cluster_queue)
                    self.queued_jobs.append(job_desc)  # Track queued jobs
                    return

                # 获取目标集群的配置，用于路径转换
                target_cluster_metadata = cluster_metadata[source_cluster_queue]

                # 执行路径转换
                converted_job_desc = self._convert_job_path(job_desc, target_cluster_metadata)

                # 直接调度到指定集群
                job_id = self.dispatcher.dispatch_job(converted_job_desc, source_cluster_queue)
                logger.info(f"作业 {job_desc.job_id} 在集群 {source_cluster_queue} 提交完成，作业ID: {job_id}")
                return

            # 如果作业来自全局队列，需要经过policy调度策略评估，决策目标调度集群
            decision = self.policy_engine.schedule_job(job_desc, cluster_snapshots)

            if decision and decision.cluster_name:
                logger.info(f"作业 {job_desc.job_id} 调度到集群 {decision.cluster_name}")
                # 获取目标集群的配置，用于路径转换
                target_cluster_metadata = cluster_metadata[decision.cluster_name]

                # 执行路径转换
                converted_job_desc = self._convert_job_path(job_desc, target_cluster_metadata)

                # 实际调度作业到选定的集群
                job_id = self.dispatcher.dispatch_job(converted_job_desc, decision.cluster_name)
                logger.info(f"作业 {job_desc.job_id} 提交完成，作业ID: {job_id}")
            else:
                # If no cluster is available, re-enqueue the job
                logger.warning(f"没有可用集群处理作业 {job_desc.job_id}，重新加入队列")
                self.queued_jobs.append(job_desc)  # Track queued jobs
                if job_desc.preferred_cluster:
                    self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue_job(job_desc)
                return

        except NoHealthyClusterError as e:
            logger.error(f"作业 {job_desc.job_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the job for later retry
            self.queued_jobs.append(job_desc)  # Track queued jobs
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)
        except TaskSubmissionError as e:
            logger.error(f"作业 {job_desc.job_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the job for later retry
            self.queued_jobs.append(job_desc)  # Track queued jobs
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)
        except Exception as e:
            logger.error(f"作业 {job_desc.job_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # Re-enqueue the job for later retry
            self.queued_jobs.append(job_desc)  # Track queued jobs
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)