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
from ray_multicluster_scheduler.scheduler.queue.backpressure_controller import BackpressureController

from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError, PolicyEvaluationError
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)

# 导入 submit_actor 模块的全局变量
try:
    from ray_multicluster_scheduler.app.client_api.submit_actor import _task_results, _task_results_lock, _task_results_condition
except ImportError:
    # 如果导入失败，创建本地版本
    _task_results = {}
    _task_results_lock = threading.Lock()
    _task_results_condition = threading.Condition(_task_results_lock)


class TaskLifecycleManager:
    """Manages the lifecycle of tasks in the multicluster scheduler."""

    def __init__(self, cluster_monitor: ClusterMonitor):
        self.cluster_monitor = cluster_monitor
        self.policy_engine = PolicyEngine(cluster_monitor=cluster_monitor)
        # 初始化client_pool和connection_manager
        # 不在初始化时创建job_client_pool，只在需要时按需创建
        self.client_pool = cluster_monitor.client_pool
        self.connection_manager = ConnectionLifecycleManager(self.client_pool, initialize_job_client_pool_on_init=False)
        self.dispatcher = Dispatcher(self.connection_manager)
        self.task_queue = TaskQueue(max_size=100)
        # Initialize backpressure controller
        self.backpressure_controller = BackpressureController(threshold=0.7)
        self.running = False
        self.worker_thread = None
        self._initialized = False
        # Track queued tasks for re-evaluation
        self.queued_tasks: List[TaskDescription] = []
        # Track queued jobs for re-evaluation
        self.queued_jobs: List[JobDescription] = []
        # Track job submission cluster mapping
        self.job_cluster_mapping: Dict[str, str] = {}

    def _is_duplicate_task_in_tracked_list(self, task_desc: TaskDescription) -> bool:
        """Check if a task with the same content is already in the tracked queued tasks list."""
        for queued_task in self.queued_tasks:
            if (queued_task.func_or_class == task_desc.func_or_class and
                queued_task.args == task_desc.args and
                queued_task.kwargs == task_desc.kwargs and
                queued_task.resource_requirements == task_desc.resource_requirements and
                queued_task.tags == task_desc.tags and
                queued_task.preferred_cluster == task_desc.preferred_cluster):
                return True
        return False

    def _is_duplicate_job_in_tracked_list(self, job_desc: JobDescription) -> bool:
        """Check if a job with the same content is already in the tracked queued jobs list."""
        for queued_job in self.queued_jobs:
            if (queued_job.entrypoint == job_desc.entrypoint and
                queued_job.runtime_env == job_desc.runtime_env and
                queued_job.metadata == job_desc.metadata and
                queued_job.preferred_cluster == job_desc.preferred_cluster):
                return True
        return False

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

        # 确保在停止时清理连接
        try:
            self.connection_manager.shutdown()
        except Exception as e:
            logger.error(f"Error during connection manager shutdown: {e}")

        # 确保Ray连接也关闭
        try:
            if ray.is_initialized():
                ray.shutdown()
                logger.info("Ray connection shut down during lifecycle manager stop")
        except Exception as e:
            logger.error(f"Error during Ray shutdown: {e}")

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

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Make scheduling decision - policy engine will fetch latest snapshots directly
            decision = self.policy_engine.schedule(task_desc)

            if decision and decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
                # 实际调度任务到选定的集群
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)

                # 注意：不再直接等待结果，而是返回future，让调用方处理
                # 等待任务执行完成并获取结果
                # 检查Ray连接状态，避免在关闭时获取结果
                if ray.is_initialized():
                    result = ray.get(future)
                    logger.info(f"任务 {task_desc.task_id} 执行完成，结果: {result}")
                else:
                    logger.warning(f"Ray连接已关闭，无法获取任务 {task_desc.task_id} 的结果")
                    return task_desc.task_id
                return task_desc.task_id
            else:
                logger.info(f"任务 {task_desc.task_id} 需要排队等待资源释放")
                # 将任务加入队列，如果是首选集群资源紧张，则加入该集群的队列
                # 但检查任务是否已存在于跟踪列表中，避免重复添加
                if not self._is_duplicate_task_in_tracked_list(task_desc):
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
            # But check if task is already queued to avoid duplication
            if task_desc not in self.queued_tasks:
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

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Make scheduling decision for job - policy engine will fetch latest snapshots directly
            decision = self.policy_engine.schedule_job(job_desc)

            if decision and decision.cluster_name:
                logger.info(f"作业 {job_desc.job_id} 调度到集群 {decision.cluster_name}")

                # 获取目标集群的配置，用于路径转换
                target_cluster_metadata = cluster_metadata[decision.cluster_name]

                # 执行路径转换
                converted_job_desc = self._convert_job_path(job_desc, target_cluster_metadata)

                # 如果runtime_env为None，创建一个包含集群上下文的环境
                if converted_job_desc.runtime_env is None:
                    converted_job_desc.runtime_env = {
                        "env_vars": {
                            "RAY_MULTICLUSTER_CURRENT_CLUSTER": decision.cluster_name
                        }
                    }
                else:
                    # 如果已经存在env_vars，添加集群上下文
                    if "env_vars" not in converted_job_desc.runtime_env:
                        converted_job_desc.runtime_env["env_vars"] = {}
                    converted_job_desc.runtime_env["env_vars"]["RAY_MULTICLUSTER_CURRENT_CLUSTER"] = decision.cluster_name

                # 设置集群上下文，以便作业内部的submit_task/submit_actor调用可以继承相同的集群
                from ray_multicluster_scheduler.common.context_manager import ClusterContextManager
                previous_cluster = ClusterContextManager.get_current_cluster()
                ClusterContextManager.set_current_cluster(decision.cluster_name)

                try:
                    # 实际调度作业到选定的集群
                    job_id = self.dispatcher.dispatch_job(converted_job_desc, decision.cluster_name)
                finally:
                    # 恢复之前的集群上下文
                    if previous_cluster is not None:
                        ClusterContextManager.set_current_cluster(previous_cluster)
                    else:
                        ClusterContextManager.clear_current_cluster()

                # 记录作业ID到集群的映射关系
                self.job_cluster_mapping[job_id] = decision.cluster_name

                logger.info(f"作业 {job_desc.job_id} 提交完成，作业ID: {job_id}")
                return job_id
            else:
                logger.info(f"作业 {job_desc.job_id} 需要排队等待资源释放")
                # 将作业加入队列，如果是首选集群资源紧张，则加入该集群的队列
                # 但检查作业是否已存在于跟踪列表中，避免重复添加
                if not self._is_duplicate_job_in_tracked_list(job_desc):
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
            # But check if job is already queued to avoid duplication
            if job_desc not in self.queued_jobs:
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

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # Make scheduling decision - policy engine will fetch latest snapshots directly
            decision = self.policy_engine.schedule(task_desc)

            if decision and decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")
                # 实际调度任务到选定的集群并返回future
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)
                return future
            else:
                logger.info(f"任务 {task_desc.task_id} 需要排队等待资源释放")
                # 将任务加入队列，如果是首选集群资源紧张，则加入该集群的队列
                # 但检查任务是否已存在于跟踪列表中，避免重复添加
                if not self._is_duplicate_task_in_tracked_list(task_desc):
                    self.queued_tasks.append(task_desc)
                if task_desc.preferred_cluster:
                    self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue(task_desc)
                # 返回任务ID而不是None，这样客户端不会因为任务排队而失败
                return task_desc.task_id

        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # 将任务加入队列以便稍后重试
            # 但检查任务是否已排队以避免重复
            if task_desc not in self.queued_tasks:
                self.queued_tasks.append(task_desc)
                if task_desc.preferred_cluster:
                    self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue(task_desc)
            # 即使出现异常，也返回任务ID，让客户端知道任务已排队
            return task_desc.task_id

    def _worker_loop(self):
        """Main worker loop that processes tasks and jobs from the queue."""
        last_re_evaluation = time.time()

        while self.running:
            try:
                # Ensure connections are initialized
                if not self._initialized:
                    self._initialize_connections()

                # Check if there are queued tasks or jobs that need resource evaluation
                cluster_queue_names = self.task_queue.get_cluster_queue_names()
                cluster_job_queue_names = self.task_queue.get_cluster_job_queue_names()
                has_queued_tasks = len(self.queued_tasks) > 0
                has_queued_jobs = len(self.queued_jobs) > 0
                has_cluster_specific_tasks = len(cluster_queue_names) > 0
                has_cluster_specific_jobs = len(cluster_job_queue_names) > 0
                has_global_tasks = len(self.task_queue.global_queue) > 0
                has_global_jobs = len(self.task_queue.global_job_queue) > 0

                # Get cluster snapshots based on queue status (SPEC-3 requirement)
                # When queues are empty, get snapshots less frequently
                # When queues are not empty, get snapshots more frequently (every 15 seconds)
                if (has_queued_tasks or has_queued_jobs or
                    has_cluster_specific_tasks or has_cluster_specific_jobs or
                    has_global_tasks or has_global_jobs):
                    # When queues are not empty, get snapshots every 15 seconds for re-evaluation
                    cluster_info = self.cluster_monitor.get_all_cluster_info()
                    cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                                      if info['snapshot'] is not None}
                else:
                    # When queues are empty, get snapshots normally
                    cluster_info = self.cluster_monitor.get_all_cluster_info()
                    cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                                      if info['snapshot'] is not None}

                # Check backpressure status
                backpressure_active = self.backpressure_controller.should_apply_backpressure(cluster_snapshots)

                # If backpressure is active, apply backoff
                if backpressure_active:
                    backoff_time = self.backpressure_controller.get_backoff_time()
                    logger.info(f"Backpressure active, applying backoff for {backoff_time:.2f} seconds")
                    time.sleep(backoff_time)
                    continue  # Skip processing tasks and re-evaluate after backoff

                # Re-evaluate queued tasks and jobs when there are queued items
                # According to SPEC-3: when queue is not empty, update snapshots every 15 seconds
                current_time = time.time()
                # When there are queued tasks or jobs, re-evaluate every 15 seconds
                if has_queued_tasks or has_queued_jobs:
                    if current_time - last_re_evaluation > 15.0:  # Every 15 seconds when queues not empty
                        self._re_evaluate_queued_tasks(cluster_snapshots, cluster_info)
                        self._re_evaluate_queued_jobs(cluster_snapshots, cluster_info)
                        last_re_evaluation = current_time
                else:
                    # When queues are empty, re-evaluate less frequently
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
                    # No tasks or jobs in any queue, sleep for 15 seconds as per SPEC-2
                    time.sleep(15.0)
                    continue

                # Process the task or job, passing the source cluster information
                if task_type == 'job':
                    self._process_job(task_desc, cluster_snapshots, source_cluster)
                else:
                    self._process_task(task_desc, cluster_snapshots, source_cluster)

                # 根据项目规范，调用接口后需保证最小时间间隔，避免过于频繁的调度尝试
                # 当队列不为空时，添加短暂延迟以避免高速循环
                if (self.queued_tasks or self.queued_jobs or
                    self.task_queue.get_cluster_queue_names() or
                    self.task_queue.get_cluster_job_queue_names() or
                    self.task_queue.global_queue or self.task_queue.global_job_queue):
                    time.sleep(0.1)  # 短暂延迟，避免过度占用CPU

            except Exception as e:
                logger.error(f"任务生命周期工作者循环错误: {e}")
                import traceback
                traceback.print_exc()
                # Check if we should continue running
                if not self.running:
                    logger.info("Task lifecycle manager stopped, exiting worker loop")
                    break
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
                        # Make a new scheduling decision - policy engine will fetch latest snapshots directly
                        decision = self.policy_engine.schedule(task_desc)

                        if decision and decision.cluster_name:
                            # Found a suitable cluster, process this task immediately
                            logger.info(f"任务 {task_desc.task_id} 重新调度到集群 {decision.cluster_name}")
                            # 重新评估的任务来自全局排队列表，没有特定的源集群
                            self._process_task(task_desc, cluster_snapshots, None)
                            rescheduled_count += 1
                        else:
                            # Still no suitable cluster, keep in queue
                            # But make sure it's not a duplicate
                            if task_desc not in remaining_tasks:
                                remaining_tasks.append(task_desc)
                    except Exception as e:
                        logger.error(f"重新评估任务 {task_desc.task_id} 时出错: {e}")
                        # 如果是首选集群不可用的异常，直接记录错误并重新加入队列
                        if "不在线或无法连接" in str(e):
                            logger.error(f"首选集群不可用，任务 {task_desc.task_id} 重新加入队列: {e}")
                        # Task is already in queue, just add back to remaining tasks list
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
            backpressure_active = self.backpressure_controller.should_apply_backpressure(cluster_snapshots)

            if not backpressure_active and self.queued_jobs:
                logger.info(f"重新评估 {len(self.queued_jobs)} 个排队作业的调度可能性")

                # Try to reschedule some queued jobs
                remaining_jobs = []
                rescheduled_count = 0

                for job_desc in self.queued_jobs:
                    try:
                        # Make a new scheduling decision for job - policy engine will fetch latest snapshots directly
                        decision = self.policy_engine.schedule_job(job_desc)

                        if decision and decision.cluster_name:
                            # Found a suitable cluster, process this job immediately
                            logger.info(f"作业 {job_desc.job_id} 重新调度到集群 {decision.cluster_name}")
                            # 重新评估的作业来自全局排队列表，没有特定的源集群
                            self._process_job(job_desc, cluster_snapshots, None)
                            rescheduled_count += 1
                        else:
                            # Still no suitable cluster, keep in queue
                            # But make sure it's not a duplicate
                            if job_desc not in remaining_jobs:
                                remaining_jobs.append(job_desc)
                    except Exception as e:
                        logger.error(f"重新评估作业 {job_desc.job_id} 时出错: {e}")
                        # 如果是首选集群不可用的异常，直接记录错误并重新加入队列
                        if "不在线或无法连接" in str(e):
                            logger.error(f"首选集群不可用，作业 {job_desc.job_id} 重新加入队列: {e}")
                        # Job is already in queue, just add back to remaining jobs list
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

            # Check backpressure status before processing
            backpressure_active = self.backpressure_controller.should_apply_backpressure(cluster_snapshots)
            if backpressure_active:
                logger.info(f"Backpressure active, re-queuing task {task_desc.task_id} for later processing")
                # Re-queue the task for later processing
                if source_cluster_queue:
                    self.task_queue.enqueue(task_desc, source_cluster_queue)
                else:
                    self.task_queue.enqueue(task_desc)
                return

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # 如果任务来自特定集群队列，直接调度到该集群（符合规则1：指定集群任务排队队列中的task只能进入指定集群执行）
            if source_cluster_queue:
                logger.info(f"任务 {task_desc.task_id} 来自集群 {source_cluster_queue} 的队列，直接调度到该集群")

                # 检查目标集群连接状态
                connection_state = self.connection_manager.get_connection_state(source_cluster_queue)
                if connection_state not in ['connected', 'healthy']:
                    logger.warning(f"目标集群 {source_cluster_queue} 连接状态异常 ({connection_state})，尝试重新连接")
                    success = self.connection_manager.ensure_cluster_connection(source_cluster_queue)
                    if not success:
                        logger.error(f"无法连接到目标集群 {source_cluster_queue}，任务 {task_desc.task_id} 重新加入队列")
                        # Task is already in queue, no need to re-add
                        # Just ensure it's tracked in the queued_tasks list
                        if not self._is_duplicate_task_in_tracked_list(task_desc):
                            self.queued_tasks.append(task_desc)  # Track queued tasks
                        return

                # 检查目标集群是否健康可用
                if source_cluster_queue not in cluster_snapshots:
                    logger.error(f"目标集群 {source_cluster_queue} 不在线或无法连接，任务 {task_desc.task_id} 重新加入队列")
                    # Task is already in queue, no need to re-add
                    # Just ensure it's tracked in the queued_tasks list
                    if not self._is_duplicate_task_in_tracked_list(task_desc):
                        self.queued_tasks.append(task_desc)  # Track queued tasks
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
                    # Task is already in queue, no need to re-add
                    # Just ensure it's tracked in the queued_tasks list
                    if not self._is_duplicate_task_in_tracked_list(task_desc):
                        self.queued_tasks.append(task_desc)  # Track queued tasks
                    return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过任务 {task_desc.task_id} 的执行")
                    return

                # 直接调度到指定集群
                future = self.dispatcher.dispatch_task(task_desc, source_cluster_queue)

                # 检查Ray连接状态，避免在关闭时获取结果
                # 但首先检查任务类型，避免对actor handle调用ray.get()
                if task_desc.is_actor:
                    # 对于 actor，future 应该是 actor handle，不需要 ray.get()
                    logger.info(f"Actor {task_desc.task_id} 已成功创建在集群 {source_cluster_queue}")
                    # 将 actor handle 存储到结果中
                    with _task_results_condition:
                        _task_results[task_desc.task_id] = future
                        _task_results_condition.notify_all()  # 通知等待的线程
                else:
                    # 对于 function，future 应该是 ObjectRef，可以使用 ray.get()
                    # 但为了安全起见，先检查 future 类型
                    import ray.util.client.common
                    if ((hasattr(ray.util.client.common, 'ClientActorHandle') and
                        isinstance(future, ray.util.client.common.ClientActorHandle)) or
                        (hasattr(future, '_actor_id') and not isinstance(future, str))):
                        # 如果 future 实际上是 actor handle（即使 task_desc.is_actor 为 False），也按 actor 处理
                        logger.info(f"Actor {task_desc.task_id} 已成功创建在集群 {source_cluster_queue} (通过类型检查识别)")
                        # 将 actor handle 存储到结果中
                        with _task_results_condition:
                            _task_results[task_desc.task_id] = future
                            _task_results_condition.notify_all()  # 通知等待的线程
                    else:
                        # 对于 function，future 是 ObjectRef，可以使用 ray.get()
                        if ray.is_initialized():
                            try:
                                # 根据项目规范，获取Ray远程结果前需检查连接状态
                                connection_state = self.connection_manager.get_connection_state(source_cluster_queue)
                                if connection_state not in ['connected', 'healthy']:
                                    logger.warning(f"集群 {source_cluster_queue} 连接状态异常 ({connection_state})，无法获取任务 {task_desc.task_id} 的结果")
                                    with _task_results_condition:
                                        _task_results[task_desc.task_id] = future
                                        _task_results_condition.notify_all()  # 通知等待的线程
                                else:
                                    result = ray.get(future)
                                    logger.info(f"任务 {task_desc.task_id} 在集群 {source_cluster_queue} 执行完成，结果: {result}")
                            except Exception as e:
                                logger.error(f"获取任务 {task_desc.task_id} 结果时出错: {e}")
                                # 即使获取结果失败，也将 future 存储起来
                                with _task_results_condition:
                                    _task_results[task_desc.task_id] = future
                                    _task_results_condition.notify_all()  # 通知等待的线程
                        else:
                            logger.warning(f"Ray连接已关闭，无法获取任务 {task_desc.task_id} 的结果")
                            with _task_results_condition:
                                _task_results[task_desc.task_id] = future
                                _task_results_condition.notify_all()  # 通知等待的线程
                return

            # 如果任务来自全局队列，按照规则2：需要经过policy调度策略评估，决策目标调度集群
            # Policy engine will fetch latest snapshots directly
            decision = self.policy_engine.schedule(task_desc)

            if decision and decision.cluster_name:
                logger.info(f"任务 {task_desc.task_id} 调度到集群 {decision.cluster_name}")

                # 检查目标集群连接状态
                connection_state = self.connection_manager.get_connection_state(decision.cluster_name)
                if connection_state not in ['connected', 'healthy']:
                    logger.warning(f"目标集群 {decision.cluster_name} 连接状态异常 ({connection_state})，尝试重新连接")
                    success = self.connection_manager.ensure_cluster_connection(decision.cluster_name)
                    if not success:
                        logger.error(f"无法连接到目标集群 {decision.cluster_name}，任务 {task_desc.task_id} 重新加入队列")
                        # Task is already in queue, no need to re-add
                        # Just ensure it's tracked in the queued_tasks list
                        if not self._is_duplicate_task_in_tracked_list(task_desc):
                            self.queued_tasks.append(task_desc)  # Track queued tasks
                        return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过任务 {task_desc.task_id} 的执行")
                    return

                # 实际调度任务到选定的集群
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)

                # 处理任务结果 - 区分 actor 和 function
                if task_desc.is_actor:
                    # 对于 actor，future 应该是 actor handle，不需要 ray.get()
                    logger.info(f"Actor {task_desc.task_id} 已成功创建在集群 {decision.cluster_name}")
                    # 将 actor handle 存储到结果中
                    with _task_results_condition:
                        _task_results[task_desc.task_id] = future
                        _task_results_condition.notify_all()  # 通知等待的线程
                else:
                    # 对于 function，future 应该是 ObjectRef，可以使用 ray.get()
                    # 但为了安全起见，先检查 future 类型
                    import ray.util.client.common
                    if ((hasattr(ray.util.client.common, 'ClientActorHandle') and
                        isinstance(future, ray.util.client.common.ClientActorHandle)) or
                        (hasattr(future, '_actor_id') and not isinstance(future, str))):
                        # 如果 future 实际上是 actor handle（即使 task_desc.is_actor 为 False），也按 actor 处理
                        logger.info(f"Actor {task_desc.task_id} 已成功创建在集群 {decision.cluster_name} (通过类型检查识别)")
                        # 将 actor handle 存储到结果中
                        with _task_results_condition:
                            _task_results[task_desc.task_id] = future
                            _task_results_condition.notify_all()  # 通知等待的线程
                    else:
                        # 对于 function，future 是 ObjectRef，可以使用 ray.get()
                        # 检查Ray连接状态，避免在关闭时获取结果
                        if ray.is_initialized():
                            try:
                                # 根据项目规范，获取Ray远程结果前需检查连接状态
                                connection_state = self.connection_manager.get_connection_state(decision.cluster_name)
                                if connection_state not in ['connected', 'healthy']:
                                    logger.warning(f"集群 {decision.cluster_name} 连接状态异常 ({connection_state})，无法获取任务 {task_desc.task_id} 的结果")
                                    with _task_results_condition:
                                        _task_results[task_desc.task_id] = future
                                        _task_results_condition.notify_all()  # 通知等待的线程
                                else:
                                    result = ray.get(future)
                                    logger.info(f"任务 {task_desc.task_id} 执行完成，结果: {result}")
                            except Exception as e:
                                logger.error(f"获取任务 {task_desc.task_id} 结果时出错: {e}")
                                # 即使获取结果失败，也将 future 存储起来
                                with _task_results_condition:
                                    _task_results[task_desc.task_id] = future
                                    _task_results_condition.notify_all()  # 通知等待的线程
                        else:
                            logger.warning(f"Ray连接已关闭，无法获取任务 {task_desc.task_id} 的结果")
                            with _task_results_condition:
                                _task_results[task_desc.task_id] = future
                                _task_results_condition.notify_all()  # 通知等待的线程
            else:
                # If no cluster is available, re-enqueue the task
                logger.warning(f"没有可用集群处理任务 {task_desc.task_id}，重新加入队列")
                # Task is already in queue, no need to re-add
                # Just ensure it's tracked in the queued_tasks list
                if not self._is_duplicate_task_in_tracked_list(task_desc):
                    self.queued_tasks.append(task_desc)  # Track queued tasks
                return

        except NoHealthyClusterError as e:
            logger.error(f"任务 {task_desc.task_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # Task is already in queue, no need to re-add
            # Just ensure it's tracked in the queued_tasks list
            if not self._is_duplicate_task_in_tracked_list(task_desc):
                self.queued_tasks.append(task_desc)  # Track queued tasks
        except TaskSubmissionError as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Task is already in queue, no need to re-add
            # Just ensure it's tracked in the queued_tasks list
            if not self._is_duplicate_task_in_tracked_list(task_desc):
                self.queued_tasks.append(task_desc)  # Track queued tasks
        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # Task is already in queue, no need to re-add
            # Just ensure it's tracked in the queued_tasks list
            if not self._is_duplicate_task_in_tracked_list(task_desc):
                self.queued_tasks.append(task_desc)  # Track queued tasks

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

            # Check backpressure status before processing
            backpressure_active = self.backpressure_controller.should_apply_backpressure(cluster_snapshots)
            if backpressure_active:
                logger.info(f"Backpressure active, re-queuing job {job_desc.job_id} for later processing")
                # Re-queue the job for later processing
                if source_cluster_queue:
                    self.task_queue.enqueue_job(job_desc, source_cluster_queue)
                else:
                    self.task_queue.enqueue_job(job_desc)
                return

            # Update policy engine with current cluster metadata
            cluster_info = self.cluster_monitor.get_all_cluster_info()
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # 如果作业来自特定集群队列，直接调度到该集群
            if source_cluster_queue:
                logger.info(f"作业 {job_desc.job_id} 来自集群 {source_cluster_queue} 的队列，直接调度到该集群")

                # 检查目标集群连接状态
                connection_state = self.connection_manager.get_connection_state(source_cluster_queue)
                if connection_state not in ['connected', 'healthy']:
                    logger.warning(f"目标集群 {source_cluster_queue} 连接状态异常 ({connection_state})，尝试重新连接")
                    success = self.connection_manager.ensure_cluster_connection(source_cluster_queue)
                    if not success:
                        logger.error(f"无法连接到目标集群 {source_cluster_queue}，作业 {job_desc.job_id} 重新加入队列")
                        # Job is already in queue, no need to re-add
                        # Just ensure it's tracked in the queued_jobs list
                        if not self._is_duplicate_job_in_tracked_list(job_desc):
                            self.queued_jobs.append(job_desc)  # Track queued jobs
                        return

                # 检查目标集群是否健康可用
                if source_cluster_queue not in cluster_snapshots:
                    logger.error(f"目标集群 {source_cluster_queue} 不在线或无法连接，作业 {job_desc.job_id} 重新加入队列")
                    # Job is already in queue, no need to re-add
                    # Just ensure it's tracked in the queued_jobs list
                    if not self._is_duplicate_job_in_tracked_list(job_desc):
                        self.queued_jobs.append(job_desc)  # Track queued jobs
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
                    # Job is already in queue, no need to re-add
                    # Just ensure it's tracked in the queued_jobs list
                    if not self._is_duplicate_job_in_tracked_list(job_desc):
                        self.queued_jobs.append(job_desc)  # Track queued jobs
                    return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过作业 {job_desc.job_id} 的执行")
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
            # Policy engine will fetch latest snapshots directly
            decision = self.policy_engine.schedule_job(job_desc)

            if decision and decision.cluster_name:
                logger.info(f"作业 {job_desc.job_id} 调度到集群 {decision.cluster_name}")

                # 检查目标集群连接状态
                connection_state = self.connection_manager.get_connection_state(decision.cluster_name)
                if connection_state not in ['connected', 'healthy']:
                    logger.warning(f"目标集群 {decision.cluster_name} 连接状态异常 ({connection_state})，尝试重新连接")
                    success = self.connection_manager.ensure_cluster_connection(decision.cluster_name)
                    if not success:
                        logger.error(f"无法连接到目标集群 {decision.cluster_name}，作业 {job_desc.job_id} 重新加入队列")
                        # Job is already in queue, no need to re-add
                        # Just ensure it's tracked in the queued_jobs list
                        if not self._is_duplicate_job_in_tracked_list(job_desc):
                            self.queued_jobs.append(job_desc)  # Track queued jobs
                        return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过作业 {job_desc.job_id} 的执行")
                    return

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
                # Job is already in queue, no need to re-add
                # Just ensure it's tracked in the queued_jobs list
                if not self._is_duplicate_job_in_tracked_list(job_desc):
                    self.queued_jobs.append(job_desc)  # Track queued jobs
                return

        except NoHealthyClusterError as e:
            logger.error(f"作业 {job_desc.job_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # Job is already in queue, no need to re-add
            # Just ensure it's tracked in the queued_jobs list
            if not self._is_duplicate_job_in_tracked_list(job_desc):
                self.queued_jobs.append(job_desc)  # Track queued jobs
        except TaskSubmissionError as e:
            logger.error(f"作业 {job_desc.job_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # Job is already in queue, no need to re-add
            # Just ensure it's tracked in the queued_jobs list
            if not self._is_duplicate_job_in_tracked_list(job_desc):
                self.queued_jobs.append(job_desc)  # Track queued jobs
        except Exception as e:
            logger.error(f"作业 {job_desc.job_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # Job is already in queue, no need to re-add
            # Just ensure it's tracked in the queued_jobs list
            if not self._is_duplicate_job_in_tracked_list(job_desc):
                self.queued_jobs.append(job_desc)  # Track queued jobs