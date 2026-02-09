import ray
import time
import logging
import threading
from typing import Dict, List, Optional, Any, Callable, Tuple
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError, PolicyEvaluationError
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)

# 任务完成等待超时配置
JOB_COMPLETION_TIMEOUT = 600.0  # 等待 Job 完成的超时时间（秒）
TASK_COMPLETION_TIMEOUT = 600.0  # 等待 Task 完成的超时时间（秒）
JOB_STATUS_CHECK_INTERVAL = 5  # 检查 Job 状态的间隔（秒）

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
        self.running = False
        self.worker_thread = None
        self._initialized = False
        # Track queued tasks for re-evaluation
        self.queued_tasks: List[TaskDescription] = []
        # Track queued jobs for re-evaluation
        self.queued_jobs: List[JobDescription] = []
        # Track job submission cluster mapping
        self.job_cluster_mapping: Dict[str, str] = {}
        # Track job scheduling status mapping (job_id -> JobDescription)
        self.job_scheduling_mapping: Dict[str, JobDescription] = {}
        # Track actual submission_id to job_id mapping
        self.submission_to_job_mapping: Dict[str, str] = {}
        # 添加：job_id -> actual_submission_id 的映射，用于反向查找
        self.job_id_to_actual_submission_id: Dict[str, str] = {}
        # task_id -> Ray ObjectRef 的映射，用于任务状态查询
        self.task_to_future_mapping: Dict[str, Any] = {}
        # task_id -> 目标集群名称 的映射
        self.task_to_cluster_mapping: Dict[str, str] = {}

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
                    result = ray.get(future, timeout=TASK_COMPLETION_TIMEOUT)
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
                # 更新调度映射关系
                self.job_scheduling_mapping[job_desc.job_id] = job_desc
                self.submission_to_job_mapping[job_id] = job_desc.job_id

                logger.info(f"作业 {job_desc.job_id} 提交完成，实际submission_id: {job_id}")
                return job_id
            else:
                logger.info(f"作业 {job_desc.job_id} 需要排队等待资源释放")
                # 更新作业的调度状态
                job_desc.scheduling_status = "QUEUED"
                # 将作业加入队列，如果是首选集群资源紧张，则加入该集群的队列
                # 但检查作业是否已存在于跟踪列表中，避免重复添加
                if not self._is_duplicate_job_in_tracked_list(job_desc):
                    self.queued_jobs.append(job_desc)
                if job_desc.preferred_cluster:
                    self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue_job(job_desc)
                # 返回作业ID而不是虚假的submission_id
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

    def _wait_for_job_completion(self, job_desc: JobDescription, cluster_name: str,
                                  timeout: float = JOB_COMPLETION_TIMEOUT,
                                  wait_for_running: bool = True) -> str:
        """
        等待 Job 完成（SUCCEEDED/FAILED/STOPPED）

        Args:
            job_desc: Job 描述对象
            cluster_name: 目标集群名称
            timeout: 超时时间（秒）- 仅控制从达到终态到退出的等待时间
            wait_for_running: 如果为 True，等待状态变为 RUNNING；为 False，只等待退出

        Returns:
            最终状态: SUCCEEDED/FAILED/STOPPED/TIMEOUT
        """
        if not job_desc.actual_submission_id:
            logger.warning(f"Job {job_desc.job_id} 没有实际的 submission_id，无法等待完成")
            return "UNKNOWN"

        job_client = self.connection_manager.get_job_client(cluster_name)

        if wait_for_running:
            # 阶段1：无限等待状态变为 RUNNING 或终态（不受 timeout 限制）
            logger.info(f"开始等待 Job {job_desc.job_id} 进入运行状态...")
            while True:
                try:
                    status = job_client.get_job_status(job_desc.actual_submission_id)
                    logger.info(f"Job {job_desc.job_id} (submission_id: {job_desc.actual_submission_id}) "
                               f"当前状态: {status}")

                    if status in ["RUNNING", "SUCCEEDED", "FAILED", "STOPPED"]:
                        logger.info(f"Job {job_desc.job_id} 已达到 {status} 状态，切换到退出等待阶段")
                        break

                    time.sleep(JOB_STATUS_CHECK_INTERVAL)

                except Exception as e:
                    logger.error(f"检查 Job {job_desc.job_id} 状态时出错: {e}")
                    time.sleep(JOB_STATUS_CHECK_INTERVAL)

        # 阶段2：等待状态变为终态（受 timeout 限制）
        # 此时 job 已经在运行或已经完成
        logger.info(f"开始等待 Job {job_desc.job_id} 完成退出（超时: {timeout}秒）...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                status = job_client.get_job_status(job_desc.actual_submission_id)
                logger.info(f"Job {job_desc.job_id} (submission_id: {job_desc.actual_submission_id}) "
                           f"当前状态: {status}")

                if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
                    logger.info(f"Job {job_desc.job_id} 已完成，状态: {status}")
                    return status

                # 继续等待
                time.sleep(JOB_STATUS_CHECK_INTERVAL)

            except Exception as e:
                logger.error(f"检查 Job {job_desc.job_id} 状态时出错: {e}")
                time.sleep(JOB_STATUS_CHECK_INTERVAL)

        logger.warning(f"等待 Job {job_desc.job_id} 退出超时 ({timeout}秒)")
        return "TIMEOUT"

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

    def _can_task_be_processed(self, task_desc: TaskDescription,
                               cluster_name: str,
                               cluster_snapshots: Dict[str, ResourceSnapshot]) -> Tuple[bool, str]:
        """
        检查任务是否可以在指定集群上处理（出队前检查）。

        在实际出队之前预检查各项条件，避免任务出队后发现不可处理而重新入队，
        减少不必要的队列操作开销。

        Args:
            task_desc: 任务描述
            cluster_name: 目标集群名称
            cluster_snapshots: 当前集群资源快照

        Returns:
            (can_process: bool, reason: str) - 是否可以处理及原因
        """
        # 1. 检查连接状态
        connection_state = self.connection_manager.get_connection_state(cluster_name)
        if connection_state not in ['connected', 'healthy']:
            return False, f"连接状态异常 ({connection_state})"

        # 2. 检查集群是否在线
        if cluster_name not in cluster_snapshots:
            return False, "集群不在线"

        # 3. 检查资源阈值
        snapshot = cluster_snapshots[cluster_name]
        cpu_util = snapshot.cluster_cpu_used_cores / snapshot.cluster_cpu_total_cores if snapshot.cluster_cpu_total_cores > 0 else 0
        mem_util = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

        if cpu_util > self.policy_engine.RESOURCE_THRESHOLD or mem_util > self.policy_engine.RESOURCE_THRESHOLD:
            return False, f"资源紧张 (CPU: {cpu_util:.1%}, MEM: {mem_util:.1%})"

        # 4. 检查40秒规则
        is_top_level = getattr(task_desc, 'is_top_level_task', True)
        if is_top_level:
            if not self.policy_engine.cluster_submission_history.is_cluster_available(cluster_name):
                remaining = self.policy_engine.cluster_submission_history.get_remaining_wait_time(cluster_name)
                return False, f"40秒限制，还需等待 {remaining:.1f}s"

        # 5. 检查串行模式
        from ray_multicluster_scheduler.common.config import settings, ExecutionMode
        if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
            if TaskQueue.is_cluster_busy(cluster_name):
                return False, "串行模式，集群繁忙"

        return True, "OK"

    def _can_job_be_processed(self, job_desc: JobDescription,
                              cluster_name: str,
                              cluster_snapshots: Dict[str, ResourceSnapshot]) -> Tuple[bool, str]:
        """
        检查作业是否可以在指定集群上处理（出队前检查）。

        Args:
            job_desc: 作业描述
            cluster_name: 目标集群名称
            cluster_snapshots: 当前集群资源快照

        Returns:
            (can_process: bool, reason: str) - 是否可以处理及原因
        """
        # 1. 检查连接状态
        connection_state = self.connection_manager.get_connection_state(cluster_name)
        if connection_state not in ['connected', 'healthy']:
            return False, f"连接状态异常 ({connection_state})"

        # 2. 检查集群是否在线
        if cluster_name not in cluster_snapshots:
            return False, "集群不在线"

        # 3. 检查资源阈值
        snapshot = cluster_snapshots[cluster_name]
        cpu_util = snapshot.cluster_cpu_used_cores / snapshot.cluster_cpu_total_cores if snapshot.cluster_cpu_total_cores > 0 else 0
        mem_util = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

        if cpu_util > self.policy_engine.RESOURCE_THRESHOLD or mem_util > self.policy_engine.RESOURCE_THRESHOLD:
            return False, f"资源紧张 (CPU: {cpu_util:.1%}, MEM: {mem_util:.1%})"

        # 4. 检查40秒规则（作业也受40秒规则限制）
        if not self.policy_engine.cluster_submission_history.is_cluster_available(cluster_name):
            remaining = self.policy_engine.cluster_submission_history.get_remaining_wait_time(cluster_name)
            return False, f"40秒限制，还需等待 {remaining:.1f}s"

        # 5. 检查串行模式
        from ray_multicluster_scheduler.common.config import settings, ExecutionMode
        if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
            if TaskQueue.is_cluster_busy(cluster_name):
                return False, "串行模式，集群繁忙"

        return True, "OK"

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

                # Get cluster snapshots based on queue status (.spec-3 requirement)
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

                # NOTE: 移除全局 backpressure 检查，改用 PolicyEngine 的 per-cluster 资源管理
                # PolicyEngine 已经在 _make_scheduling_decision() 中实现了 per-cluster 的资源阈值检查
                # 40秒规则也已经提供了必要的背压机制，不需要额外的全局 backpressure

                # Re-evaluate queued tasks and jobs when there are queued items
                # According to .spec-3: when queue is not empty, update snapshots every 15 seconds
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

                # Peek-before-dequeue pattern: check if task can be processed before dequeuing
                if cluster_queue_names:
                    # Try to get tasks from cluster queues first
                    for cluster_name in cluster_queue_names:
                        # 1. 先 peek 查看队首任务
                        peeked_task = self.task_queue.peek_from_cluster(cluster_name)
                        if not peeked_task:
                            continue

                        # 2. 检查任务是否可以处理
                        can_process, reason = self._can_task_be_processed(
                            peeked_task, cluster_name, cluster_snapshots
                        )

                        if can_process:
                            # 3. 确认可处理后才出队
                            task_desc = self.task_queue.dequeue_from_cluster(cluster_name)
                            if task_desc:
                                source_cluster = cluster_name
                                break
                        else:
                            # 4. 不可处理，保持在队列中
                            logger.debug(f"任务 {peeked_task.task_id} 暂不可处理: {reason}")
                            continue

                # If no task from cluster queues, try global queue with peek-before-dequeue
                if not task_desc:
                    # 1. 先 peek 查看全局队首任务
                    peeked_task = self.task_queue.peek_global()
                    if peeked_task:
                        # 对于全局队列任务，需要先通过 PolicyEngine 决策目标集群
                        # 然后检查该集群是否可用
                        decision = self.policy_engine.schedule(peeked_task)
                        if decision and decision.cluster_name:
                            can_process, reason = self._can_task_be_processed(
                                peeked_task, decision.cluster_name, cluster_snapshots
                            )
                            if can_process:
                                # 确认可处理后才出队
                                task_desc = self.task_queue.dequeue_global()
                                # source_cluster remains None for global queue tasks (will be set by _process_task)
                            else:
                                logger.debug(f"全局任务 {peeked_task.task_id} 暂不可处理: {reason}")
                        else:
                            logger.debug(f"全局任务 {peeked_task.task_id} 无可用集群")
                    # source_cluster remains None for global queue tasks

                # If no tasks in task queues, try job queues with peek-before-dequeue
                if not task_desc:
                    job_desc = None
                    cluster_job_queue_names = self.task_queue.get_cluster_job_queue_names()

                    if cluster_job_queue_names:
                        # Try to get jobs from cluster job queues first
                        for cluster_name in cluster_job_queue_names:
                            # 1. 先 peek 查看队首作业
                            peeked_job = self.task_queue.peek_from_cluster_job(cluster_name)
                            if not peeked_job:
                                continue

                            # 2. 检查作业是否可以处理
                            can_process, reason = self._can_job_be_processed(
                                peeked_job, cluster_name, cluster_snapshots
                            )

                            if can_process:
                                # 3. 确认可处理后才出队
                                job_desc = self.task_queue.dequeue_from_cluster_job(cluster_name)
                                if job_desc:
                                    source_cluster = cluster_name
                                    task_type = 'job'
                                    break
                            else:
                                # 4. 不可处理，保持在队列中
                                logger.debug(f"作业 {peeked_job.job_id} 暂不可处理: {reason}")
                                continue

                    # If no job from cluster job queues, try global job queue with peek-before-dequeue
                    if not job_desc:
                        # 1. 先 peek 查看全局作业队首
                        peeked_job = self.task_queue.peek_global_job()
                        if peeked_job:
                            # 对于全局作业，需要先通过 PolicyEngine 决策目标集群
                            decision = self.policy_engine.schedule_job(peeked_job)
                            if decision and decision.cluster_name:
                                can_process, reason = self._can_job_be_processed(
                                    peeked_job, decision.cluster_name, cluster_snapshots
                                )
                                if can_process:
                                    # 确认可处理后才出队
                                    job_desc = self.task_queue.dequeue_global_job()
                                    if job_desc:
                                        task_type = 'job'
                                else:
                                    logger.debug(f"全局作业 {peeked_job.job_id} 暂不可处理: {reason}")
                            else:
                                logger.debug(f"全局作业 {peeked_job.job_id} 无可用集群")

                    if job_desc:
                        task_desc = job_desc

                if not task_desc:
                    # No tasks or jobs in any queue, sleep for 15 seconds as per .spec-2
                    time.sleep(15.0)
                    continue

                # Process the task or job, passing the source cluster information
                if task_type == 'job' and isinstance(task_desc, JobDescription):
                    self._process_job(task_desc, cluster_snapshots, source_cluster)
                elif isinstance(task_desc, TaskDescription):
                    self._process_task(task_desc, cluster_snapshots, source_cluster)
                else:
                    logger.warning(f"无法识别的任务类型: {type(task_desc)}")

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

            # 1. 同步清理：移除已不在实际队列中的任务
            try:
                global_task_ids = self.task_queue.get_all_global_task_ids()
                self.queued_tasks = [t for t in self.queued_tasks if t.task_id in global_task_ids]
            except Exception:
                pass  # 忽略可能的错误

            if not self.queued_tasks:
                return

            # Update policy engine with current cluster metadata
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # NOTE: 移除全局 backpressure 检查
            # 直接尝试重新评估队列中的任务，PolicyEngine 会检查每个任务的可用集群
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

            # 1. 同步清理：移除已不在实际队列中的作业
            try:
                global_job_ids = self.task_queue.get_all_global_job_ids()
                self.queued_jobs = [j for j in self.queued_jobs if j.job_id in global_job_ids]
            except Exception:
                pass  # 忽略可能的错误

            if not self.queued_jobs:
                return

            # Update policy engine with current cluster metadata
            cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}
            self.policy_engine.update_cluster_metadata(cluster_metadata)

            # NOTE: 移除全局 backpressure 检查
            # 直接尝试重新评估队列中的作业，PolicyEngine 会检查每个作业的可用集群
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

    def _process_task(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot], source_cluster_queue: Optional[str] = None):
        """Process a single task.

        Args:
            task_desc: The task to process
            cluster_snapshots: Current cluster resource snapshots
            source_cluster_queue: Name of the cluster queue this task came from, if any
        """
        # 并发保护：检查任务是否已在处理中
        if task_desc.is_processing:
            logger.warning(f"任务 {task_desc.task_id} 已在处理中，跳过重复执行")
            return

        # 标记任务为处理中
        task_desc.is_processing = True

        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            logger.info(f"处理任务 {task_desc.task_id}")

            # NOTE: 移除全局 backpressure 检查
            # PolicyEngine 会在 schedule() 中进行 per-cluster 资源检查
            # 如果目标集群资源紧张，会返回该集群名称让任务进入该集群的队列

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
                        # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                        self.task_queue.enqueue(task_desc, source_cluster_queue)
                        # 更新跟踪列表
                        if not self._is_duplicate_task_in_tracked_list(task_desc):
                            self.queued_tasks.append(task_desc)  # Track queued tasks
                        return

                # 检查目标集群是否健康可用
                if source_cluster_queue not in cluster_snapshots:
                    logger.error(f"目标集群 {source_cluster_queue} 不在线或无法连接，任务 {task_desc.task_id} 重新加入队列")
                    # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                    self.task_queue.enqueue(task_desc, source_cluster_queue)
                    # 更新跟踪列表
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
                    # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                    self.task_queue.enqueue(task_desc, source_cluster_queue)
                    # 更新跟踪列表
                    if not self._is_duplicate_task_in_tracked_list(task_desc):
                        self.queued_tasks.append(task_desc)  # Track queued tasks
                    return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过任务 {task_desc.task_id} 的执行")
                    return

                # 串行模式检查：确保集群可用
                from ray_multicluster_scheduler.common.config import settings, ExecutionMode
                if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
                    if TaskQueue.is_cluster_busy(source_cluster_queue):
                        logger.warning(f"集群 {source_cluster_queue} 在串行模式下繁忙，任务 {task_desc.task_id} 重新入队")
                        self.task_queue.enqueue(task_desc, source_cluster_queue)
                        if not self._is_duplicate_task_in_tracked_list(task_desc):
                            self.queued_tasks.append(task_desc)
                        return

                # 直接调度到指定集群
                future = self.dispatcher.dispatch_task(task_desc, source_cluster_queue)
                # 注册运行任务（用于串行模式检查）
                TaskQueue.register_running_task(task_desc.task_id, source_cluster_queue, "task")
                # 记录 task_id 到 future (ObjectRef) 和集群的映射
                self.task_to_future_mapping[task_desc.task_id] = future
                self.task_to_cluster_mapping[task_desc.task_id] = source_cluster_queue

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
                                    result = ray.get(future, timeout=TASK_COMPLETION_TIMEOUT)
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

                # 从跟踪列表中移除已处理的任务（集群队列路径）
                if task_desc in self.queued_tasks:
                    self.queued_tasks.remove(task_desc)

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
                        # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                        if task_desc.preferred_cluster:
                            self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                        else:
                            self.task_queue.enqueue(task_desc)
                        # 更新跟踪列表
                        if not self._is_duplicate_task_in_tracked_list(task_desc):
                            self.queued_tasks.append(task_desc)  # Track queued tasks
                        return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过任务 {task_desc.task_id} 的执行")
                    return

                # 实际调度任务到选定的集群
                future = self.dispatcher.dispatch_task(task_desc, decision.cluster_name)
                # 注册运行任务（用于串行模式检查）
                TaskQueue.register_running_task(task_desc.task_id, decision.cluster_name, "task")
                # 记录 task_id 到 future (ObjectRef) 和集群的映射
                self.task_to_future_mapping[task_desc.task_id] = future
                self.task_to_cluster_mapping[task_desc.task_id] = decision.cluster_name

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
                                    result = ray.get(future, timeout=TASK_COMPLETION_TIMEOUT)
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

                # 从跟踪列表中移除已处理的任务（全局队列路径）
                if task_desc in self.queued_tasks:
                    self.queued_tasks.remove(task_desc)

            else:
                # If no cluster is available, re-enqueue the task
                logger.warning(f"没有可用集群处理任务 {task_desc.task_id}，重新加入队列")
                # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                if task_desc.preferred_cluster:
                    self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue(task_desc)
                # 更新跟踪列表
                if not self._is_duplicate_task_in_tracked_list(task_desc):
                    self.queued_tasks.append(task_desc)  # Track queued tasks
                return

        except NoHealthyClusterError as e:
            logger.error(f"任务 {task_desc.task_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
            # 更新跟踪列表
            if not self._is_duplicate_task_in_tracked_list(task_desc):
                self.queued_tasks.append(task_desc)  # Track queued tasks
        except TaskSubmissionError as e:
            logger.error(f"任务 {task_desc.task_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
            # 更新跟踪列表
            if not self._is_duplicate_task_in_tracked_list(task_desc):
                self.queued_tasks.append(task_desc)  # Track queued tasks
        except Exception as e:
            logger.error(f"任务 {task_desc.task_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
            if task_desc.preferred_cluster:
                self.task_queue.enqueue(task_desc, task_desc.preferred_cluster)
            else:
                self.task_queue.enqueue(task_desc)
            # 更新跟踪列表
            if not self._is_duplicate_task_in_tracked_list(task_desc):
                self.queued_tasks.append(task_desc)  # Track queued tasks

        finally:
            # 无论任务执行成功或失败，都清除处理中标记
            task_desc.is_processing = False
            # 从 queued_tasks 跟踪列表中移除已处理的任务（如果在队列中）
            if task_desc in self.queued_tasks:
                self.queued_tasks.remove(task_desc)
            # 注销运行任务（用于串行模式检查）
            try:
                TaskQueue.unregister_running_task(task_desc.task_id)
            except Exception:
                pass  # 忽略注销错误

    def _process_job(self, job_desc: JobDescription, cluster_snapshots: Dict[str, ResourceSnapshot], source_cluster_queue: Optional[str] = None):
        """Process a single job.

        Args:
            job_desc: The job to process
            cluster_snapshots: Current cluster resource snapshots
            source_cluster_queue: Name of the cluster queue this job came from, if any
        """
        # 检查作业是否已在运行中（串行调度保护）
        if job_desc.actual_submission_id:
            running_tasks = TaskQueue.get_all_running_tasks()
            if job_desc.actual_submission_id in running_tasks:
                logger.warning(f"作业 {job_desc.job_id} (actual: {job_desc.actual_submission_id}) 已在运行中，跳过重复处理")
                return

        # 并发保护：检查作业是否已在处理中
        if getattr(job_desc, 'is_processing', False):
            logger.warning(f"作业 {job_desc.job_id} 已在处理中，跳过重复执行")
            return

        # 标记作业为处理中
        job_desc.is_processing = True

        try:
            # Ensure connections are initialized
            if not self._initialized:
                self._initialize_connections()

            logger.info(f"处理作业 {job_desc.job_id}")

            # NOTE: 移除全局 backpressure 检查
            # PolicyEngine 会在 schedule_job() 中进行 per-cluster 资源检查
            # 如果目标集群资源紧张，会返回该集群名称让作业进入该集群的队列

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
                        # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                        self.task_queue.enqueue_job(job_desc, source_cluster_queue)
                        # 更新跟踪列表
                        if not self._is_duplicate_job_in_tracked_list(job_desc):
                            self.queued_jobs.append(job_desc)  # Track queued jobs
                        return

                # 检查目标集群是否健康可用
                if source_cluster_queue not in cluster_snapshots:
                    logger.error(f"目标集群 {source_cluster_queue} 不在线或无法连接，作业 {job_desc.job_id} 重新加入队列")
                    # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                    self.task_queue.enqueue_job(job_desc, source_cluster_queue)
                    # 更新跟踪列表
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
                    # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                    self.task_queue.enqueue_job(job_desc, source_cluster_queue)
                    # 更新跟踪列表
                    if not self._is_duplicate_job_in_tracked_list(job_desc):
                        self.queued_jobs.append(job_desc)  # Track queued jobs
                    return

                # 检查是否还在运行（避免在关闭时继续执行）
                if not self.running:
                    logger.info(f"调度器已停止，跳过作业 {job_desc.job_id} 的执行")
                    return

                # 串行模式检查：确保集群可用
                from ray_multicluster_scheduler.common.config import settings, ExecutionMode
                if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
                    if TaskQueue.is_cluster_busy(source_cluster_queue):
                        logger.warning(f"集群 {source_cluster_queue} 在串行模式下繁忙，作业 {job_desc.job_id} 重新入队")
                        self.task_queue.enqueue_job(job_desc, source_cluster_queue)
                        if not self._is_duplicate_job_in_tracked_list(job_desc):
                            self.queued_jobs.append(job_desc)
                        return

                # 获取目标集群的配置，用于路径转换
                target_cluster_metadata = cluster_metadata[source_cluster_queue]

                # 执行路径转换
                converted_job_desc = self._convert_job_path(job_desc, target_cluster_metadata)

                # 直接调度到指定集群
                actual_submission_id = self.dispatcher.dispatch_job(converted_job_desc, source_cluster_queue)
                # 注册运行任务（用于串行模式检查）
                TaskQueue.register_running_task(actual_submission_id, source_cluster_queue, "job")
                # 更新作业的实际submission_id和状态
                job_desc.actual_submission_id = actual_submission_id
                job_desc.scheduling_status = "SUBMITTED"
                # 更新映射关系，以便后续查询作业状态
                self.job_cluster_mapping[actual_submission_id] = source_cluster_queue
                self.submission_to_job_mapping[actual_submission_id] = job_desc.job_id
                self.job_scheduling_mapping[job_desc.job_id] = job_desc
                # 添加反向映射：job_id -> actual_submission_id
                self.job_id_to_actual_submission_id[job_desc.job_id] = actual_submission_id
                logger.info(f"作业 {job_desc.job_id} 在集群 {source_cluster_queue} 提交完成，实际submission_id: {actual_submission_id}")

                # 更新 _submitted_job_ids 中的ID（修复排队作业ID不更新问题）
                try:
                    from ray_multicluster_scheduler.app.client_api import submit_job
                    submit_job.update_submitted_job_id(job_desc.job_id, actual_submission_id)
                except Exception as update_err:
                    logger.warning(f"更新提交作业ID失败: {update_err}")

                # 等待 Job 完成（只等待退出，不限制运行时间）
                logger.info(f"开始等待 Job {job_desc.job_id} 完成...")
                final_status = self._wait_for_job_completion(job_desc, source_cluster_queue,
                                                              timeout=JOB_COMPLETION_TIMEOUT,
                                                              wait_for_running=True)
                # 注销运行任务（用于串行模式检查）
                TaskQueue.unregister_running_task(actual_submission_id)
                job_desc.scheduling_status = final_status
                logger.info(f"Job {job_desc.job_id} 最终状态: {final_status}")
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
                        # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                        if job_desc.preferred_cluster:
                            self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                        else:
                            self.task_queue.enqueue_job(job_desc)
                        # 更新跟踪列表
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
                actual_submission_id = self.dispatcher.dispatch_job(converted_job_desc, decision.cluster_name)
                # 注册运行任务（用于串行模式检查）
                TaskQueue.register_running_task(actual_submission_id, decision.cluster_name, "job")
                # 更新作业的实际submission_id和状态
                job_desc.actual_submission_id = actual_submission_id
                job_desc.scheduling_status = "SUBMITTED"
                # 更新映射关系，以便后续查询作业状态
                self.job_cluster_mapping[actual_submission_id] = decision.cluster_name
                self.submission_to_job_mapping[actual_submission_id] = job_desc.job_id
                self.job_scheduling_mapping[job_desc.job_id] = job_desc
                # 添加反向映射：job_id -> actual_submission_id
                self.job_id_to_actual_submission_id[job_desc.job_id] = actual_submission_id
                logger.info(f"作业 {job_desc.job_id} 提交完成，实际submission_id: {actual_submission_id}")

                # 更新 _submitted_job_ids 中的ID（修复排队作业ID不更新问题）
                try:
                    from ray_multicluster_scheduler.app.client_api import submit_job
                    submit_job.update_submitted_job_id(job_desc.job_id, actual_submission_id)
                except Exception as update_err:
                    logger.warning(f"更新提交作业ID失败: {update_err}")

                # 等待 Job 完成（只等待退出，不限制运行时间）
                logger.info(f"开始等待 Job {job_desc.job_id} 完成...")
                final_status = self._wait_for_job_completion(job_desc, decision.cluster_name,
                                                              timeout=JOB_COMPLETION_TIMEOUT,
                                                              wait_for_running=True)
                # 注销运行任务（用于串行模式检查）
                TaskQueue.unregister_running_task(actual_submission_id)
                job_desc.scheduling_status = final_status
                logger.info(f"Job {job_desc.job_id} 最终状态: {final_status}")
            else:
                # If no cluster is available, re-enqueue the job
                logger.warning(f"没有可用集群处理作业 {job_desc.job_id}，重新加入队列")
                # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
                if job_desc.preferred_cluster:
                    self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
                else:
                    self.task_queue.enqueue_job(job_desc)
                # 更新跟踪列表
                if not self._is_duplicate_job_in_tracked_list(job_desc):
                    self.queued_jobs.append(job_desc)  # Track queued jobs
                return

        except NoHealthyClusterError as e:
            logger.error(f"作业 {job_desc.job_id} 失败，无健康集群: {e}")
            import traceback
            traceback.print_exc()
            # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)
            # 更新跟踪列表
            if not self._is_duplicate_job_in_tracked_list(job_desc):
                self.queued_jobs.append(job_desc)  # Track queued jobs
        except TaskSubmissionError as e:
            logger.error(f"作业 {job_desc.job_id} 提交失败: {e}")
            import traceback
            traceback.print_exc()
            # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)
            # 更新跟踪列表
            if not self._is_duplicate_job_in_tracked_list(job_desc):
                self.queued_jobs.append(job_desc)  # Track queued jobs
        except Exception as e:
            logger.error(f"作业 {job_desc.job_id} 意外失败: {e}")
            import traceback
            traceback.print_exc()
            # 重新加入task_queue以保持一致性（关键修复：确保任务不会丢失）
            if job_desc.preferred_cluster:
                self.task_queue.enqueue_job(job_desc, job_desc.preferred_cluster)
            else:
                self.task_queue.enqueue_job(job_desc)
            # 更新跟踪列表
            if not self._is_duplicate_job_in_tracked_list(job_desc):
                self.queued_jobs.append(job_desc)  # Track queued jobs
        finally:
            # 清除处理中标志
            job_desc.is_processing = False
            # 从 queued_jobs 跟踪列表中移除已处理的作业（如果在队列中）
            if job_desc in self.queued_jobs:
                self.queued_jobs.remove(job_desc)
            # 注销运行任务（用于串行模式检查）- 确保即使发生错误也能正确注销
            if job_desc.actual_submission_id:
                try:
                    TaskQueue.unregister_running_task(job_desc.actual_submission_id)
                except Exception:
                    pass  # 忽略注销错误

    def get_job_scheduling_status(self, job_id: str) -> Optional[JobDescription]:
        """获取作业的调度状态信息"""
        return self.job_scheduling_mapping.get(job_id)

    def get_actual_submission_id(self, job_id: str) -> Optional[str]:
        """根据job_id获取实际的submission_id"""
        job_desc = self.job_scheduling_mapping.get(job_id)
        return job_desc.actual_submission_id if job_desc else None

    def get_job_id_by_submission_id(self, submission_id: str) -> Optional[str]:
        """根据submission_id反向查找job_id"""
        return self.submission_to_job_mapping.get(submission_id)
