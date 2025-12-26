"""
Unified Scheduler Interface for Ray Multicluster Scheduler

This module provides simplified interfaces for initializing the scheduler environment
and submitting tasks/actors to the multicluster scheduler.
"""

import logging
import time
import threading
from typing import Callable, Dict, List, Any, Optional, Type
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.model.job_description import JobDescription

# Configure logging with default INFO level if not already configured
try:
    # Check if root logger has handlers
    if not logging.root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
except:
    pass

logger = logging.getLogger(__name__)


class UnifiedScheduler:
    """
    Unified interface for Ray multicluster scheduler.

    This class provides simplified methods for initializing the scheduler environment
    and submitting tasks/actors to the multicluster scheduler.
    """

    _instance = None
    _initialized = False
    # Store the config file path used for initialization
    _config_file_path = None

    # Health checker background thread
    _health_checker_thread = None
    _health_checker_stop_event = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(UnifiedScheduler, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the unified scheduler."""
        if not self._initialized:
            self.task_lifecycle_manager = None
            self._health_checker_stop_event = threading.Event()
            self._health_checker_thread = None
            self._initialized = True

    def initialize_environment(self, config_file_path: Optional[str] = None) -> TaskLifecycleManager:
        """
        Initialize the multicluster scheduler environment.

        This method sets up all necessary components for the scheduler to function,
        including cluster monitors, resource managers, policy engines, and task queues.

        Args:
            config_file_path (str, optional): Path to the cluster configuration YAML file.
                If not provided, the system will attempt to locate the configuration file
                in common locations or fall back to default configuration.

        Returns:
            TaskLifecycleManager: The initialized task lifecycle manager

        Raises:
            Exception: If there is an error during initialization, with full traceback information

        Example:
            >>> scheduler = UnifiedScheduler()
            >>> task_lifecycle_manager = scheduler.initialize_environment()
            >>> # With custom config file:
            >>> task_lifecycle_manager = scheduler.initialize_environment("/path/to/clusters.yaml")
        """
        try:
            # Store the config file path for later use in lazy initialization
            self.__class__._config_file_path = config_file_path

            # Initialize components
            cluster_monitor = ClusterMonitor(config_file_path=config_file_path)

            # Create task lifecycle manager
            self.task_lifecycle_manager = TaskLifecycleManager(
                cluster_monitor=cluster_monitor
            )

            # Initialize job client pool in connection manager
            self.task_lifecycle_manager.connection_manager.initialize_job_client_pool(cluster_monitor.config_manager)

            # Display cluster information and resource usage
            self._display_cluster_info(cluster_monitor)

            # Start the background health checker thread
            self._start_health_checker_thread(cluster_monitor)

            logger.info("🚀 调度器环境初始化成功完成")
            return self.task_lifecycle_manager
        except Exception as e:
            logger.error(f"Failed to initialize scheduler environment: {e}")
            import traceback
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback:\n{traceback_str}")
            raise Exception(f"Failed to initialize scheduler environment: {e}\nFull traceback:\n{traceback_str}")

    def _start_health_checker_thread(self, cluster_monitor):
        """Start the background health checker thread."""
        # Check if thread is already running
        if self._health_checker_thread and self._health_checker_thread.is_alive():
            logger.info("Health checker background thread is already running")
            return

        # Stop any existing health checker thread
        self._stop_health_checker_thread()

        # Reset the stop event
        self._health_checker_stop_event.clear()

        # Create and start the health checker thread
        self._health_checker_thread = threading.Thread(
            target=self._run_health_checker,
            args=(cluster_monitor,),
            daemon=False  # Make thread a non-daemon so it properly synchronizes with main process
        )
        self._health_checker_thread.start()
        logger.info("Health checker background thread started")

    def _run_health_checker(self, cluster_monitor):
        """Run the health checker in a background thread."""
        logger.info("Health checker started, updating cluster snapshots every 20 seconds")

        while not self._health_checker_stop_event.is_set():
            try:
                # Get cluster metadata from the cluster monitor
                cluster_metadata_list = []
                cluster_info = cluster_monitor.get_all_cluster_info()
                for name, info in cluster_info.items():
                    metadata = info['metadata']
                    cluster_metadata = ClusterMetadata(
                        name=metadata.name,
                        head_address=metadata.head_address,
                        dashboard=metadata.dashboard,
                        prefer=metadata.prefer,
                        weight=metadata.weight,
                        runtime_env=metadata.runtime_env,
                        tags=metadata.tags
                    )
                    cluster_metadata_list.append(cluster_metadata)

                # Create HealthChecker and update snapshots
                if cluster_metadata_list:  # Only create HealthChecker if there are clusters
                    # Use the cluster monitor's client pool for health checking
                    health_checker = HealthChecker(cluster_metadata_list, cluster_monitor.client_pool)

                    # Update cluster manager's health status with current health check results
                    health_checker.update_cluster_manager_health(cluster_monitor.cluster_manager)

                    # Also update cluster monitor snapshots
                    snapshots = health_checker.check_health()

                    # Update cluster monitor with new snapshots
                    for cluster_name, snapshot in snapshots.items():
                        cluster_monitor.update_resource_snapshot(cluster_name, snapshot)

                    logger.debug(f"Updated health snapshots for {len(snapshots)} clusters")

                # Wait for 20 seconds or until stop event is set
                for _ in range(20):  # 20 seconds with 1-second intervals
                    if self._health_checker_stop_event.is_set():
                        break
                    time.sleep(1)
            except Exception as e:
                logger.error(f"Error in health checker: {e}")
                # Wait for 10 seconds before retrying to avoid rapid error loops
                for _ in range(10):
                    if self._health_checker_stop_event.is_set():
                        break
                    time.sleep(1)

        logger.info("Health checker stopped")

    def cleanup(self):
        """Clean up resources and stop the health checker thread."""
        self._stop_health_checker_thread()

    def _stop_health_checker_thread(self):
        """Stop the background health checker thread."""
        if self._health_checker_thread and self._health_checker_thread.is_alive():
            logger.info("Stopping health checker background thread...")
            self._health_checker_stop_event.set()
            # Wait for thread to finish with a timeout
            self._health_checker_thread.join(timeout=10)  # Wait up to 10 seconds for thread to finish
            if self._health_checker_thread.is_alive():
                logger.warning("Health checker thread did not stop gracefully within timeout")
            else:
                logger.info("Health checker background thread stopped")
        else:
            logger.info("Health checker background thread is not running")

    def _display_cluster_info(self, cluster_monitor):
        """Display cluster information and resource usage."""
        try:
            # Get cluster information
            cluster_info = cluster_monitor.get_all_cluster_info()

            logger.info("📋 集群信息和资源使用情况:")
            logger.info("=" * 50)

            # Track connected clusters
            connected_clusters = []

            available_clusters = []
            preferred_clusters = []

            for name, info in cluster_info.items():
                metadata = info['metadata']
                snapshot = info['snapshot']

                if snapshot:
                    # Get resource information from new ResourceSnapshot fields
                    cpu_free = snapshot.cluster_cpu_total_cores - round(snapshot.cluster_cpu_usage_percent / 100.0 * snapshot.cluster_cpu_total_cores, 1) if snapshot.cluster_cpu_total_cores > 0 else 0
                    cpu_total = snapshot.cluster_cpu_total_cores
                    # Note: GPU information is not available in the new ResourceSnapshot structure
                    gpu_free = 0  # Placeholder since GPU info is not in new structure
                    gpu_total = 0  # Placeholder since GPU info is not in new structure
                    memory_free_gb = (snapshot.cluster_mem_total_mb - snapshot.cluster_mem_used_mb) / 1024.0 if snapshot.cluster_mem_total_mb > 0 else 0
                    memory_total_gb = snapshot.cluster_mem_total_mb / 1024.0
                    node_count = snapshot.node_count

                    # Calculate utilization from new fields
                    cpu_utilization = snapshot.cluster_cpu_usage_percent / 100.0 if snapshot.cluster_cpu_total_cores > 0 else 0
                    gpu_utilization = 0  # Placeholder since GPU info is not in new structure
                    memory_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                    # Get cluster health to display score
                    health = cluster_monitor.cluster_manager.health_status.get(name)
                    score = health.score if health else 0.0

                    # Display cluster info with emojis
                    logger.info(f"✅ 集群 [{name}]")
                    logger.info(f"   📍 地址: {metadata.head_address}")
                    logger.info(f"   💻 CPU: {cpu_free}/{cpu_total} 核心 (使用率: {cpu_utilization:.1%})")
                    logger.info(f"   🎮 GPU: {gpu_free}/{gpu_total} 卡 (使用率: {gpu_utilization:.1%})")
                    logger.info(f"   🧠 内存: {memory_free_gb:.1f}/{memory_total_gb:.1f} GB (使用率: {memory_utilization:.1%})")
                    logger.info(f"   🖥️  节点数: {node_count}")
                    logger.info(f"   ⭐ 偏好: {'是' if metadata.prefer else '否'} | 权重: {metadata.weight}")
                    logger.info(f"   📊 评分: {score:.2f}")
                    logger.info(f"   🏷️  标签: {', '.join(metadata.tags) if metadata.tags else '无'}")

                    # Add to lists
                    connected_clusters.append(name)
                    available_clusters.append((name, cpu_free, memory_free_gb))
                    if metadata.prefer:
                        preferred_clusters.append(name)
                else:
                    logger.info(f"❌ 集群 [{name}] 不可用")
                    logger.info(f"   📍 地址: {metadata.head_address}")
                    logger.info(f"   🚫 原因: 无法获取资源信息")

            logger.info("=" * 50)

            # Display connected clusters summary
            if connected_clusters:
                logger.info(f"🔗 当前可用的集群: {', '.join(connected_clusters)}")
            else:
                logger.info("🚫 当前没有可用的集群")

            # Display task submission information
            if available_clusters:
                # Sort by available resources (simple heuristic)
                available_clusters.sort(key=lambda x: (x[1], x[2]), reverse=True)  # Sort by CPU then memory
                best_cluster = available_clusters[0][0]

                # Check if there are preferred clusters
                if preferred_clusters:
                    logger.info(f"🎯 任务调度策略:")
                    logger.info(f"   🔧 指定集群优先: 如指定 preferred_cluster，将优先调度到指定集群")
                    logger.info(f"   ⭐ 偏好集群: {', '.join(preferred_clusters)} (系统偏好集群)")
                    logger.info(f"   📊 默认负载均衡: 资源最充足的集群是 [{best_cluster}]")
                else:
                    logger.info(f"🎯 任务调度策略:")
                    logger.info(f"   🔧 指定集群优先: 如指定 preferred_cluster，将优先调度到指定集群")
                    logger.info(f"   📊 默认负载均衡: 资源最充足的集群是 [{best_cluster}]")
            else:
                logger.info("⚠️  没有足够资源的集群可供任务提交")

        except Exception as e:
            logger.error(f"❌ 获取集群信息时出错: {e}")
            import traceback
            traceback.print_exc()

    def submit_task(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        resource_requirements: Dict[str, float] = None,
        tags: List[str] = None,
        name: str = "",
        preferred_cluster: Optional[str] = None
    ) -> Any:
        """
        Submit a task to the multicluster scheduler.

        This method provides a simplified interface for submitting tasks to the scheduler.
        The scheduler will automatically handle cluster selection, resource allocation,
        and task execution across available Ray clusters.

        Args:
            func (Callable): The function to execute remotely
            args (tuple, optional): Positional arguments for the function. Defaults to ().
            kwargs (dict, optional): Keyword arguments for the function. Defaults to None.
            resource_requirements (Dict[str, float], optional):
                Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1}).
                Defaults to None.
            tags (List[str], optional): List of tags to associate with the task. Defaults to None.
            name (str, optional): Optional name for the task. Defaults to "".
            preferred_cluster (str, optional): Preferred cluster name for task execution.
                If specified cluster is unavailable, scheduler will fallback to other clusters.

        Returns:
            Any: Task submission result - a tuple containing (task_id, result)

        Raises:
            RuntimeError: If the scheduler is not initialized or task submission fails
        """
        # Import here to avoid circular imports
        def _get_submit_task_function():
            from ray_multicluster_scheduler.app.client_api.submit_task import submit_task, initialize_scheduler as init_task_scheduler
            return submit_task, init_task_scheduler

        # 如果调度器未初始化，尝试惰性初始化
        if not self.task_lifecycle_manager:
            try:
                self.initialize_environment(config_file_path=self.__class__._config_file_path)
                # 同时初始化submit_task模块中的调度器
                _, init_task_scheduler = _get_submit_task_function()
                init_task_scheduler(self.task_lifecycle_manager)
            except Exception as e:
                logger.error(f"Failed to lazily initialize scheduler: {e}")
                import traceback
                traceback.print_exc()
                raise RuntimeError("Scheduler environment not initialized. Call initialize_environment() first.")

        try:
            logger.info(f"Submitting task: {name}")
            submit_task, _ = _get_submit_task_function()
            task_id, result = submit_task(
                func=func,
                args=args,
                kwargs=kwargs,
                resource_requirements=resource_requirements,
                tags=tags,
                name=name,
                preferred_cluster=preferred_cluster
            )
            # When a task is submitted, ensure the health checker is running
            if self.task_lifecycle_manager and not self._health_checker_thread:
                # Initialize health checker thread if not already running
                cluster_monitor = self.task_lifecycle_manager.cluster_monitor
                if cluster_monitor:
                    self._start_health_checker_thread(cluster_monitor)

            logger.info(f"Task {name} submitted successfully with task_id: {task_id}")
            return task_id, result
        except Exception as e:
            logger.error(f"Failed to submit task {name}: {e}")
            import traceback
            traceback.print_exc()
            raise

    def submit_job(
        self,
        entrypoint: str,
        runtime_env: Optional[Dict] = None,
        job_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        submission_id: Optional[str] = None,
        preferred_cluster: Optional[str] = None,
        resource_requirements: Optional[Dict[str, float]] = None,
        tags: Optional[List[str]] = None
    ) -> str:
        """
        Submit a job to the multicluster scheduler using JobSubmissionClient.

        This method provides a simplified interface for submitting jobs to the scheduler.
        The scheduler will automatically handle cluster selection, resource allocation,
        and job execution across available Ray clusters using JobSubmissionClient.

        Args:
            entrypoint (str): The command to run in the job (e.g., "python train.py")
            runtime_env (Dict, optional): Runtime environment for the job
            job_id (str, optional): Unique identifier for the job
            metadata (Dict, optional): Metadata to associate with the job
            submission_id (str, optional): Submission ID for tracking
            preferred_cluster (str, optional): Preferred cluster name for job execution
            resource_requirements (Dict[str, float], optional): Resource requirements for the job
            tags (List[str], optional): List of tags to associate with the job

        Returns:
            str: Job ID of the submitted job

        Raises:
            RuntimeError: If the scheduler is not initialized or job submission fails
        """
        # 如果调度器未初始化，尝试惰性初始化
        if not self.task_lifecycle_manager:
            try:
                self.initialize_environment(config_file_path=self.__class__._config_file_path)
            except Exception as e:
                logger.error(f"Failed to lazily initialize scheduler: {e}")
                import traceback
                traceback.print_exc()
                raise RuntimeError("Scheduler environment not initialized. Call initialize_environment() first.")

        try:
            logger.info(f"Submitting job: {job_id or 'auto-generated'}")

            # Create job description
            job_desc = JobDescription(
                job_id=job_id,
                entrypoint=entrypoint,
                runtime_env=runtime_env,
                metadata=metadata,
                submission_id=submission_id,
                preferred_cluster=preferred_cluster,
                resource_requirements=resource_requirements,
                tags=tags
            )

            # Submit job using the task lifecycle manager
            job_id_result = self.task_lifecycle_manager.submit_job(job_desc)

            # When a job is submitted, ensure the health checker is running
            if self.task_lifecycle_manager and not self._health_checker_thread:
                # Initialize health checker thread if not already running
                cluster_monitor = self.task_lifecycle_manager.cluster_monitor
                if cluster_monitor:
                    self._start_health_checker_thread(cluster_monitor)

            logger.info(f"Job {job_id or 'auto-generated'} submitted successfully with job_id: {job_id_result}")
            return job_id_result
        except Exception as e:
            logger.error(f"Failed to submit job {job_id or 'auto-generated'}: {e}")
            import traceback
            traceback.print_exc()
            raise

    def submit_actor(
        self,
        actor_class: Type,
        args: tuple = (),
        kwargs: dict = None,
        resource_requirements: Dict[str, float] = None,
        tags: List[str] = None,
        name: str = "",
        preferred_cluster: Optional[str] = None
    ) -> Any:
        """
        Submit an actor to the multicluster scheduler.

        This method provides a simplified interface for submitting actors to the scheduler.
        The scheduler will automatically handle cluster selection, resource allocation,
        and actor instantiation across available Ray clusters.

        Args:
            actor_class (Type): The actor class to instantiate remotely
            args (tuple, optional): Positional arguments for the actor constructor. Defaults to ().
            kwargs (dict, optional): Keyword arguments for the actor constructor. Defaults to None.
            resource_requirements (Dict[str, float], optional):
                Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1}).
                Defaults to None.
            tags (List[str], optional): List of tags to associate with the actor. Defaults to None.
            name (str, optional): Optional name for the actor. Defaults to "".
            preferred_cluster (str, optional): Preferred cluster name for actor execution.
                If specified cluster is unavailable, scheduler will fallback to other clusters.

        Returns:
            Any: Actor submission result - a tuple containing (actor_id, actor_instance)

        Raises:
            RuntimeError: If the scheduler is not initialized or actor submission fails
        """
        # Import here to avoid circular imports
        def _get_submit_actor_function():
            from ray_multicluster_scheduler.app.client_api.submit_actor import submit_actor, initialize_scheduler as init_actor_scheduler
            return submit_actor, init_actor_scheduler

        # 如果调度器未初始化，尝试惰性初始化
        if not self.task_lifecycle_manager:
            try:
                self.initialize_environment(config_file_path=self.__class__._config_file_path)
                # 同时初始化submit_actor模块中的调度器
                _, init_actor_scheduler = _get_submit_actor_function()
                init_actor_scheduler(self.task_lifecycle_manager)
            except Exception as e:
                logger.error(f"Failed to lazily initialize scheduler: {e}")
                import traceback
                traceback.print_exc()
                raise RuntimeError("Scheduler environment not initialized. Call initialize_environment() first.")

        try:
            logger.info(f"Submitting actor: {name}")
            submit_actor, _ = _get_submit_actor_function()
            actor_id, actor_instance = submit_actor(
                actor_class=actor_class,
                args=args,
                kwargs=kwargs,
                resource_requirements=resource_requirements,
                tags=tags,
                name=name,
                preferred_cluster=preferred_cluster
            )
            # When an actor is submitted, ensure the health checker is running
            if self.task_lifecycle_manager and not self._health_checker_thread:
                # Initialize health checker thread if not already running
                cluster_monitor = self.task_lifecycle_manager.cluster_monitor
                if cluster_monitor:
                    self._start_health_checker_thread(cluster_monitor)

            logger.info(f"Actor {name} submitted successfully with actor_id: {actor_id}")
            return actor_id, actor_instance
        except Exception as e:
            logger.error(f"Failed to submit actor {name}: {e}")
            import traceback
            traceback.print_exc()
            raise

    def list_clusters(self) -> List[str]:
        """
        List all available clusters in the scheduler.

        Returns:
            List[str]: List of cluster names that are available in the scheduler
        """
        if not self.task_lifecycle_manager:
            try:
                self.initialize_environment(config_file_path=self.__class__._config_file_path)
            except Exception as e:
                logger.error(f"Failed to initialize scheduler to list clusters: {e}")
                return []

        try:
            # Get cluster information from the cluster monitor
            cluster_info = self.task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
            return list(cluster_info.keys())
        except Exception as e:
            logger.error(f"Failed to list clusters: {e}")
            return []



# Global unified scheduler instance
_unified_scheduler = None


def get_unified_scheduler() -> UnifiedScheduler:
    """
    Get the global unified scheduler instance.

    Returns:
        UnifiedScheduler: The global unified scheduler instance
    """
    global _unified_scheduler
    if _unified_scheduler is None:
        _unified_scheduler = UnifiedScheduler()
    return _unified_scheduler


def initialize_scheduler_environment(config_file_path: Optional[str] = None) -> TaskLifecycleManager:
    """
    Initialize the multicluster scheduler environment.

    This is a convenience function that initializes the scheduler environment
    using the unified scheduler interface.

    Args:
        config_file_path (str, optional): Path to the cluster configuration YAML file.
            If not provided, the system will attempt to locate the configuration file
            in common locations or fall back to default configuration.

    Returns:
        TaskLifecycleManager: The initialized task lifecycle manager

    Raises:
        Exception: If there is an error during initialization, with full traceback information

    Example:
        >>> task_lifecycle_manager = initialize_scheduler_environment()
        >>> # With custom config file:
        >>> task_lifecycle_manager = initialize_scheduler_environment("/path/to/clusters.yaml")
    """
    try:
        scheduler = get_unified_scheduler()
        task_lifecycle_manager = scheduler.initialize_environment(config_file_path=config_file_path)

        # 同步初始化submit_task、submit_actor和submit_job模块中的调度器，确保它们使用相同的配置
        try:
            from ray_multicluster_scheduler.app.client_api.submit_task import initialize_scheduler as init_task_scheduler
            init_task_scheduler(task_lifecycle_manager)
        except Exception as e:
            logger.warning(f"Failed to initialize submit_task scheduler: {e}")

        try:
            from ray_multicluster_scheduler.app.client_api.submit_actor import initialize_scheduler as init_actor_scheduler
            init_actor_scheduler(task_lifecycle_manager)
        except Exception as e:
            logger.warning(f"Failed to initialize submit_actor scheduler: {e}")

        try:
            from ray_multicluster_scheduler.app.client_api.submit_job import initialize_scheduler as init_job_scheduler
            init_job_scheduler(task_lifecycle_manager)
        except Exception as e:
            logger.warning(f"Failed to initialize submit_job scheduler: {e}")

        return task_lifecycle_manager
    except Exception as e:
        logger.error(f"Failed to initialize scheduler environment: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"Traceback:\n{traceback_str}")
        raise Exception(f"Failed to initialize scheduler environment: {e}\nFull traceback:\n{traceback_str}")


def submit_task(
    func: Callable,
    args: tuple = (),
    kwargs: dict = None,
    resource_requirements: Dict[str, float] = None,
    tags: List[str] = None,
    name: str = "",
    preferred_cluster: Optional[str] = None
) -> Any:
    """
    Submit a task to the multicluster scheduler.

    This is a convenience function that submits a task to the scheduler
    using the unified scheduler interface.

    Args:
        func (Callable): The function to execute remotely
        args (tuple, optional): Positional arguments for the function. Defaults to ().
        kwargs (dict, optional): Keyword arguments for the function. Defaults to None.
        resource_requirements (Dict[str, float], optional):
            Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1}).
            Defaults to None.
        tags (List[str], optional): List of tags to associate with the task. Defaults to None.
        name (str, optional): Optional name for the task. Defaults to "".
        preferred_cluster (str, optional): Preferred cluster name for task execution.
            If specified cluster is unavailable, scheduler will fallback to other clusters.

    Returns:
        Any: Task submission result - a tuple containing (task_id, result)

    Raises:
        RuntimeError: If the scheduler is not initialized or task submission fails
    """
    try:
        scheduler = get_unified_scheduler()
        return scheduler.submit_task(
            func=func,
            args=args,
            kwargs=kwargs,
            resource_requirements=resource_requirements,
            tags=tags,
            name=name,
            preferred_cluster=preferred_cluster
        )
    except Exception as e:
        logger.error(f"Failed to submit task {name}: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"Traceback:\n{traceback_str}")
        raise Exception(f"Failed to submit task {name}: {e}\nFull traceback:\n{traceback_str}")


def submit_job(
    entrypoint: str,
    runtime_env: Optional[Dict] = None,
    job_id: Optional[str] = None,
    metadata: Optional[Dict] = None,
    submission_id: Optional[str] = None,
    preferred_cluster: Optional[str] = None,
    resource_requirements: Optional[Dict[str, float]] = None,
    tags: Optional[List[str]] = None
) -> str:
    """
    Submit a job to the multicluster scheduler using JobSubmissionClient.

    This is a convenience function that submits a job to the scheduler
    using the unified scheduler interface.

    Args:
        entrypoint (str): The command to run in the job (e.g., "python train.py")
        runtime_env (Dict, optional): Runtime environment for the job
        job_id (str, optional): Unique identifier for the job
        metadata (Dict, optional): Metadata to associate with the job
        submission_id (str, optional): Submission ID for tracking
        preferred_cluster (str, optional): Preferred cluster name for job execution
        resource_requirements (Dict[str, float], optional): Resource requirements for the job
        tags (List[str], optional): List of tags to associate with the job

    Returns:
        str: Job ID of the submitted job

    Raises:
        RuntimeError: If the scheduler is not initialized or job submission fails
    """
    try:
        scheduler = get_unified_scheduler()
        return scheduler.submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            job_id=job_id,
            metadata=metadata,
            submission_id=submission_id,
            preferred_cluster=preferred_cluster,
            resource_requirements=resource_requirements,
            tags=tags
        )
    except Exception as e:
        logger.error(f"Failed to submit job {job_id or 'auto-generated'}: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"Traceback:\n{traceback_str}")
        raise Exception(f"Failed to submit job {job_id or 'auto-generated'}: {e}\nFull traceback:\n{traceback_str}")


def submit_actor(
    actor_class: Type,
    args: tuple = (),
    kwargs: dict = None,
    resource_requirements: Dict[str, float] = None,
    tags: List[str] = None,
    name: str = "",
    preferred_cluster: Optional[str] = None
) -> Any:
    """
    Submit an actor to the multicluster scheduler.

    This is a convenience function that submits an actor to the scheduler
    using the unified scheduler interface.

    Args:
        actor_class (Type): The actor class to instantiate remotely
        args (tuple, optional): Positional arguments for the actor constructor. Defaults to ().
        kwargs (dict, optional): Keyword arguments for the actor constructor. Defaults to None.
        resource_requirements (Dict[str, float], optional):
            Dictionary of resource requirements (e.g., {"CPU": 2, "GPU": 1}).
            Defaults to None.
        tags (List[str], optional): List of tags to associate with the actor. Defaults to None.
        name (str, optional): Optional name for the actor. Defaults to "".
        preferred_cluster (str, optional): Preferred cluster name for actor execution.
            If specified cluster is unavailable, scheduler will fallback to other clusters.

    Returns:
        Any: Actor submission result - a tuple containing (actor_id, actor_instance)

    Raises:
        RuntimeError: If the scheduler is not initialized or actor submission fails
    """
    try:
        scheduler = get_unified_scheduler()
        return scheduler.submit_actor(
            actor_class=actor_class,
            args=args,
            kwargs=kwargs,
            resource_requirements=resource_requirements,
            tags=tags,
            name=name,
            preferred_cluster=preferred_cluster
        )
    except Exception as e:
        logger.error(f"Failed to submit actor {name}: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"Traceback:\n{traceback_str}")
        raise Exception(f"Failed to submit actor {name}: {e}\nFull traceback:\n{traceback_str}")