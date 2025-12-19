"""
Unified Scheduler Interface for Ray Multicluster Scheduler

This module provides simplified interfaces for initializing the scheduler environment
and submitting tasks/actors to the multicluster scheduler.
"""

import logging
import time
from typing import Callable, Dict, List, Any, Optional, Type
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor

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

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(UnifiedScheduler, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the unified scheduler."""
        if not self._initialized:
            self.task_lifecycle_manager = None
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

            # Display cluster information and resource usage
            self._display_cluster_info(cluster_monitor)

            logger.info("🚀 调度器环境初始化成功完成")
            return self.task_lifecycle_manager
        except Exception as e:
            logger.error(f"Failed to initialize scheduler environment: {e}")
            import traceback
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback:\n{traceback_str}")
            raise Exception(f"Failed to initialize scheduler environment: {e}\nFull traceback:\n{traceback_str}")

    def _display_cluster_info(self, cluster_monitor):
        """Display cluster information and resource usage."""
        try:
            # Refresh cluster resource snapshots to ensure we have current information
            cluster_monitor.refresh_resource_snapshots(force=True)

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

                if snapshot and snapshot.available_resources:
                    # Get resource information
                    cpu_free = snapshot.available_resources.get("CPU", 0)
                    cpu_total = snapshot.total_resources.get("CPU", 0)
                    gpu_free = snapshot.available_resources.get("GPU", 0)
                    gpu_total = snapshot.total_resources.get("GPU", 0)
                    memory_free = snapshot.available_resources.get("memory", 0)
                    memory_total = snapshot.total_resources.get("memory", 0)
                    node_count = snapshot.node_count

                    # Calculate utilization
                    cpu_utilization = (cpu_total - cpu_free) / cpu_total if cpu_total > 0 else 0
                    gpu_utilization = (gpu_total - gpu_free) / gpu_total if gpu_total > 0 else 0
                    memory_utilization = (memory_total - memory_free) / memory_total if memory_total > 0 else 0

                    # Display cluster info with emojis
                    logger.info(f"✅ 集群 [{name}]")
                    logger.info(f"   📍 地址: {metadata.head_address}")
                    logger.info(f"   💻 CPU: {cpu_free}/{cpu_total} 核心 (使用率: {cpu_utilization:.1%})")
                    logger.info(f"   🎮 GPU: {gpu_free}/{gpu_total} 卡 (使用率: {gpu_utilization:.1%})")
                    logger.info(f"   🧠 内存: {memory_free/1024/1024/1024:.1f}/{memory_total/1024/1024/1024:.1f} GB (使用率: {memory_utilization:.1%})")
                    logger.info(f"   🖥️  节点数: {node_count}")
                    logger.info(f"   ⭐ 偏好: {'是' if metadata.prefer else '否'} | 权重: {metadata.weight}")
                    logger.info(f"   🏷️  标签: {', '.join(metadata.tags) if metadata.tags else '无'}")

                    # Add to lists
                    connected_clusters.append(name)
                    available_clusters.append((name, cpu_free, memory_free))
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
        preferred_cluster: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None
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
            runtime_env (Dict[str, Any], optional): Runtime environment configuration.
                Defaults to None.

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
                preferred_cluster=preferred_cluster,
                runtime_env=runtime_env
            )
            logger.info(f"Task {name} submitted successfully with task_id: {task_id}")
            return task_id, result
        except Exception as e:
            logger.error(f"Failed to submit task {name}: {e}")
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
        preferred_cluster: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None
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
            runtime_env (Dict[str, Any], optional): Runtime environment configuration.
                Defaults to None.

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
                preferred_cluster=preferred_cluster,
                runtime_env=runtime_env
            )
            logger.info(f"Actor {name} submitted successfully with actor_id: {actor_id}")
            return actor_id, actor_instance
        except Exception as e:
            logger.error(f"Failed to submit actor {name}: {e}")
            import traceback
            traceback.print_exc()
            raise


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
        
        # 同步初始化submit_task和submit_actor模块中的调度器，确保它们使用相同的配置
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
    preferred_cluster: Optional[str] = None,
    runtime_env: Optional[Dict[str, Any]] = None
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
        runtime_env (Dict[str, Any], optional): Runtime environment configuration.
            Defaults to None.

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
            preferred_cluster=preferred_cluster,
            runtime_env=runtime_env
        )
    except Exception as e:
        logger.error(f"Failed to submit task {name}: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"Traceback:\n{traceback_str}")
        raise Exception(f"Failed to submit task {name}: {e}\nFull traceback:\n{traceback_str}")


def submit_actor(
    actor_class: Type,
    args: tuple = (),
    kwargs: dict = None,
    resource_requirements: Dict[str, float] = None,
    tags: List[str] = None,
    name: str = "",
    preferred_cluster: Optional[str] = None,
    runtime_env: Optional[Dict[str, Any]] = None
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
        runtime_env (Dict[str, Any], optional): Runtime environment configuration.
            Defaults to None.

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
            preferred_cluster=preferred_cluster,
            runtime_env=runtime_env
        )
    except Exception as e:
        logger.error(f"Failed to submit actor {name}: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"Traceback:\n{traceback_str}")
        raise Exception(f"Failed to submit actor {name}: {e}\nFull traceback:\n{traceback_str}")