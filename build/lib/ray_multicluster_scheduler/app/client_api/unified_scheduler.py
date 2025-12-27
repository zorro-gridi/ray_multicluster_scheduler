"""
Unified Scheduler Interface for Ray Multicluster Scheduler

This module provides simplified interfaces for initializing the scheduler environment
and submitting tasks/actors to the multicluster scheduler.
"""

import logging
import time
import threading
import ray
from typing import Callable, Dict, List, Any, Optional, Type
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.monitor.cluster_resource_monitor import ClusterResourceMonitor

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
    # Detached monitor actor reference
    _detached_monitor = None

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
            self._detached_monitor = None
            self._monitor_cluster_name = "centos"  # Default cluster for monitor
            self._initialized = True

    def initialize_environment(self, config_file_path: Optional[str] = None, monitor_cluster_name: str = "centos") -> TaskLifecycleManager:
        """
        Initialize the multicluster scheduler environment.

        This method sets up all necessary components for the scheduler to function,
        including cluster monitors, resource managers, policy engines, and task queues.

        Args:
            config_file_path (str, optional): Path to the cluster configuration YAML file.
                If not provided, the system will attempt to locate the configuration file
                in common locations or fall back to default configuration.
            monitor_cluster_name (str, optional): Name of the cluster to run the detached monitor on.
                Defaults to "centos". The cluster name will be validated against available clusters.

        Returns:
            TaskLifecycleManager: The initialized task lifecycle manager

        Raises:
            Exception: If there is an error during initialization, with full traceback information

        Example:
            >>> scheduler = UnifiedScheduler()
            >>> task_lifecycle_manager = scheduler.initialize_environment()
            >>> # With custom config file:
            >>> task_lifecycle_manager = scheduler.initialize_environment("/path/to/clusters.yaml")
            >>> # With custom monitor cluster:
            >>> task_lifecycle_manager = scheduler.initialize_environment(monitor_cluster_name="mac")
        """
        # Check if already initialized - return existing task_lifecycle_manager
        if self.task_lifecycle_manager is not None:
            logger.info("🔄 调度器环境已初始化，返回现有实例")
            return self.task_lifecycle_manager

        try:
            # Store the config file path for later use in lazy initialization
            self.__class__._config_file_path = config_file_path
            # Store the monitor cluster name
            self._monitor_cluster_name = monitor_cluster_name

            # Initialize components
            cluster_monitor = ClusterMonitor(config_file_path=config_file_path)

            # Create task lifecycle manager
            self.task_lifecycle_manager = TaskLifecycleManager(
                cluster_monitor=cluster_monitor
            )

            # Initialize job client pool in connection manager
            self.task_lifecycle_manager.connection_manager.initialize_job_client_pool(cluster_monitor.config_manager)

            # Start the detached cluster resource monitor as an actor
            self._start_detached_monitor(cluster_monitor, monitor_cluster_name)

            # Update cluster_monitor with detached_monitor reference
            cluster_monitor.detached_monitor = self._detached_monitor

            # Start the background health checker as a detached actor
            self._start_background_health_checker(cluster_monitor, monitor_cluster_name)

            # 等待首次快照收集完成（最多等待180秒，因为包含创建actor和收集多个集群的资源）
            # 如果复用了已存在的 actors，使用快速检查模式（已有历史快照）
            if hasattr(self, '_reused_existing_monitor') and self._reused_existing_monitor:
                logger.info("⚡ 复用了已存在的 actors，使用快速检查模式...")
                snapshots_ready = self._wait_for_initial_snapshots(timeout=180, quick_check=True)
            else:
                logger.info("⏳ 等待后台监控完成首次快照收集...")
                snapshots_ready = self._wait_for_initial_snapshots(timeout=180, quick_check=False)

            if snapshots_ready:
                logger.info("✅ 首次快照收集完成")
            else:
                logger.warning("⚠️  等待首次快照超时，将使用空快照初始化")

            # 初始化集群快照（从 detached_monitor 读取）
            cluster_monitor.initialize_snapshots_from_monitor()

            # 显示集群信息和资源使用情况（在快照准备好之后）
            self._display_cluster_info(cluster_monitor)

            logger.info("🚀 调度器环境初始化成功完成")
            return self.task_lifecycle_manager
        except Exception as e:
            logger.error(f"Failed to initialize scheduler environment: {e}")
            import traceback
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback:\n{traceback_str}")
            raise Exception(f"Failed to initialize scheduler environment: {e}\nFull traceback:\n{traceback_str}")

    def _start_detached_monitor(self, cluster_monitor, cluster_name: str = "centos"):
        """
        Start the detached cluster resource monitor actor on specified cluster.

        Args:
            cluster_monitor: The cluster monitor instance
            cluster_name: Name of the cluster to run the monitor on (default: "centos")
        """
        try:
            # Get cluster configurations
            cluster_configs_list = cluster_monitor.config_manager.get_cluster_configs()

            # Convert list to dict for easier lookup and for ClusterResourceMonitor
            cluster_configs = {config.name: config for config in cluster_configs_list}

            # Validate the specified cluster exists in configuration
            target_cluster = None
            target_address = None

            for name, config in cluster_configs.items():
                if name.lower() == cluster_name.lower():
                    target_cluster = config
                    target_address = f"ray://{config.head_address}"
                    break

            if not target_cluster:
                available_clusters = list(cluster_configs.keys())
                logger.warning(f"⚠️ Cluster '{cluster_name}' not found in configuration")
                logger.warning(f"   Available clusters: {available_clusters}")
                logger.warning("   Cannot start detached monitor")
                return

            # Check if Ray is initialized and on the correct cluster
            need_reconnect = False
            current_cluster = None

            if ray.is_initialized():
                try:
                    # Get current Ray address to determine which cluster we're on
                    current_address = ray.get_runtime_context().gcs_address
                    logger.info(f"Current Ray connection: {current_address}")

                    # Check if current connection is to the target cluster
                    if target_cluster.head_address not in current_address:
                        logger.info(f"Currently connected to different cluster, need to reconnect to {cluster_name}")
                        need_reconnect = True
                    else:
                        logger.info(f"Already connected to target cluster {cluster_name}")
                        current_cluster = cluster_name
                except Exception as e:
                    logger.warning(f"Could not determine current cluster: {e}")
                    need_reconnect = True
            else:
                logger.info(f"Ray not initialized, will connect to {cluster_name} cluster")
                need_reconnect = True

            # Reconnect to target cluster if needed
            if need_reconnect:
                # Shutdown current connection if exists
                if ray.is_initialized():
                    logger.info("Shutting down current Ray connection")
                    ray.shutdown()
                    time.sleep(1)  # Brief pause to allow shutdown to complete

                # Prepare runtime_env for ray.init
                # Use the cluster's configured runtime_env (conda, env_vars, etc.)
                init_runtime_env = target_cluster.runtime_env.copy() if target_cluster.runtime_env else {}

                logger.info(f"Connecting to {cluster_name} cluster at {target_address}")
                logger.info(f"Using runtime_env: {init_runtime_env}")

                # Initialize Ray connection to target cluster with runtime_env
                ray.init(
                    address=target_address,
                    runtime_env=init_runtime_env if init_runtime_env else None,
                    ignore_reinit_error=True
                )
                logger.info(f"✅ Connected to {cluster_name} cluster")

            # Try to get existing monitor actor first (check after ensuring correct cluster)
            # Use a fixed namespace for the monitor actor to avoid multiple instances
            MONITOR_NAMESPACE = "ray_multicluster_scheduler"

            # First check if there's a monitor in the fixed namespace
            existing_monitor = None
            self._reused_existing_monitor = False  # 标志：是否复用了已存在的 monitor
            try:
                existing_monitor = ray.get_actor("cluster_resource_monitor", namespace=MONITOR_NAMESPACE)
                self._detached_monitor = existing_monitor
                self._reused_existing_monitor = True  # 标记为复用
                logger.info("✅ Found existing detached cluster resource monitor actor in fixed namespace")
                logger.info("   将在后续的 _wait_for_initial_snapshots 中验证是否正常工作")
                logger.info("   Skipping creation - using existing monitor")
                return
            except ValueError:
                # Actor doesn't exist in fixed namespace, check other namespaces
                logger.info("No monitor found in fixed namespace, checking all namespaces...")

            # Check all namespaces for any existing monitor
            try:
                named_actors = ray.util.list_named_actors(all_namespaces=True)
                for actor_info in named_actors:
                    if actor_info['name'] == 'cluster_resource_monitor':
                        namespace = actor_info.get('namespace', 'default')
                        if isinstance(namespace, bytes):
                            namespace = namespace.decode('utf-8')

                        logger.warning(f"⚠️ 发现已存在的monitor在非namespace中: {namespace}")
                        logger.warning("⚠️ 这可能是旧版本创建的，建议运行reset_scheduler_environment()清理")

                        # Try to use the existing monitor
                        try:
                            existing_monitor = ray.get_actor(actor_info['name'], namespace=namespace)
                            self._detached_monitor = existing_monitor
                            self._reused_existing_monitor = True  # 标记为复用
                            logger.info(f"✅ 使用已存在的monitor (namespace: {namespace})")
                            logger.info("   将在后续的 _wait_for_initial_snapshots 中验证是否正常工作")
                            return
                        except Exception as e:
                            logger.warning(f"⚠️ 无法使用已存在的monitor: {e}")
                            # Continue to check other monitors
                            continue
            except Exception as e:
                logger.warning(f"⚠️ 扫描已存在monitor时出错: {e}")
                import traceback
                logger.warning(traceback.format_exc())

            # Create the detached monitor actor on the current (target) cluster
            # Note: runtime_env is already set in ray.init, so actor inherits it
            # Use a fixed namespace to avoid multiple instances with random namespaces
            logger.info(f"   Creating detached monitor actor on {cluster_name} cluster")
            self._detached_monitor = ClusterResourceMonitor.options(
                namespace=MONITOR_NAMESPACE,
                name="cluster_resource_monitor",
                lifetime="detached"
            ).remote(cluster_configs)

            logger.info("✅ Detached cluster resource monitor started as a detached actor")
            logger.info(f"   Actor name: cluster_resource_monitor")
            logger.info(f"   Running on: {cluster_name} cluster ({target_address})")
            logger.info(f"   Configured for {len(cluster_configs)} clusters: {list(cluster_configs.keys())}")

            # 优化：创建后不需要立即验证，等待后续的 _wait_for_initial_snapshots 检查
            logger.info("   ℹ️  Monitor 已创建，将在稍后的初始化步骤中验证")

        except Exception as e:
            logger.error(f"Failed to start detached monitor: {e}")
            import traceback
            traceback.print_exc()

    def _start_background_health_checker(self, cluster_monitor, cluster_name: str = "centos"):
        """
        启动后台健康检查 detached actor

        Args:
            cluster_monitor: The cluster monitor instance
            cluster_name: Name of the cluster to run the health checker on (default: "centos")
        """
        try:
            from ray_multicluster_scheduler.scheduler.health.background_health_checker import BackgroundHealthChecker

            MONITOR_NAMESPACE = "ray_multicluster_scheduler"

            # 检查是否已存在 BackgroundHealthChecker
            try:
                existing_checker = ray.get_actor("background_health_checker", namespace=MONITOR_NAMESPACE)
                logger.info("✅ 找到已存在的 BackgroundHealthChecker actor")
                # 优化：直接使用已存在的，不需要验证（避免阻塞）
                logger.info("   跳过创建 - 使用已存在的 health checker")
                return
            except ValueError:
                logger.info("未找到已存在的 BackgroundHealthChecker，将创建新的...")

            # 准备集群元数据
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

            # 准备 client_pool 配置（传递字典而不是对象）
            client_pool_config = {config.name: config for config in cluster_monitor.config_manager.get_cluster_configs()}

            # 创建 BackgroundHealthChecker actor
            logger.info(f"   在 {cluster_name} 集群上创建 BackgroundHealthChecker actor...")
            background_checker = BackgroundHealthChecker.options(
                namespace=MONITOR_NAMESPACE,
                name="background_health_checker",
                lifetime="detached"
            ).remote(
                cluster_metadata_list,
                client_pool_config,
                self._detached_monitor,
                refresh_interval=20
            )

            logger.info("✅ BackgroundHealthChecker actor 创建成功")
            logger.info(f"   Actor name: background_health_checker")
            logger.info(f"   Running on: {cluster_name} cluster")
            logger.info(f"   监控 {len(cluster_metadata_list)} 个集群")

            # 启动监控循环（异步调用，不等待返回）
            background_checker.start_monitoring.remote()
            logger.info("✅ 后台监控循环已启动（异步）")

        except Exception as e:
            logger.error(f"启动 BackgroundHealthChecker 失败: {e}")
            import traceback
            traceback.print_exc()

    def _wait_for_initial_snapshots(self, timeout: int = 180, quick_check: bool = False) -> bool:
        """
        等待 ClusterResourceMonitor 有快照数据

        Args:
            timeout: 超时时间（秒），默认180秒（3分钟）
            quick_check: 如果为True，快速检查，有快照就立即返回，适用于复用已存在的actor

        Returns:
            bool: 是否成功获取到快照
        """
        if not self._detached_monitor:
            logger.warning("⚠️  detached_monitor 不可用，无法等待快照")
            return False

        start_time = time.time()
        check_interval = 2  # 每2秒检查一次，减少日志输出
        last_log_time = start_time
        log_interval = 10  # 每10秒输出一次进度

        # 快速检查模式：立即检查一次，有快照就返回
        if quick_check:
            try:
                logger.info("⚡ 快速检查快照是否已存在...")
                snapshots = ray.get(self._detached_monitor.get_latest_snapshots.remote(), timeout=5.0)
                if snapshots and len(snapshots) > 0:
                    logger.info(f"   ✅ 发现已存在的 {len(snapshots)} 个集群快照，无需等待")
                    for cluster_name in snapshots.keys():
                        logger.info(f"      - {cluster_name}")
                    return True
                else:
                    logger.info("   ⚠️  快照为空，将等待后台收集...")
            except Exception as e:
                logger.warning(f"   ⚠️  快速检查失败: {e}，将进入正常等待流程")

        while time.time() - start_time < timeout:
            try:
                snapshots = ray.get(self._detached_monitor.get_latest_snapshots.remote())
                if snapshots and len(snapshots) > 0:
                    elapsed = time.time() - start_time
                    logger.info(f"   ✅ 获取到 {len(snapshots)} 个集群的快照 (耗时: {elapsed:.1f}秒)")
                    return True

                # 每10秒输出一次进度
                current_time = time.time()
                if current_time - last_log_time >= log_interval:
                    elapsed = current_time - start_time
                    logger.info(f"   🕒 等待中... ({elapsed:.0f}/{timeout}s) - 后台正在收集集群资源快照")
                    last_log_time = current_time

                time.sleep(check_interval)
            except Exception as e:
                logger.debug(f"   检查快照时出错: {e}")
                time.sleep(check_interval)

        logger.warning(f"   ⚠️  等待 {timeout} 秒后仍未获取到快照")
        logger.warning(f"   提示: 请检查集群连接是否正常，BackgroundHealthChecker 是否正常运行")
        return False

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
                    # Pass the detached monitor to enable reading from global snapshots
                    health_checker = HealthChecker(
                        cluster_metadata_list,
                        cluster_monitor.client_pool,
                        detached_monitor=self._detached_monitor
                    )

                    # Update cluster manager's health status with current health check results
                    health_checker.update_cluster_manager_health(cluster_monitor.cluster_manager)

                    # Also update cluster monitor snapshots
                    snapshots = health_checker.check_health()

                    # Update cluster monitor with new snapshots
                    for cluster_name, snapshot in snapshots.items():
                        cluster_monitor.update_resource_snapshot(cluster_name, snapshot)

                    # 推送快照到全局 ClusterResourceMonitor
                    if self._detached_monitor and snapshots:
                        try:
                            ray.get(self._detached_monitor.update_snapshots.remote(snapshots))
                            logger.debug(f"✅ 推送 {len(snapshots)} 个快照到 ClusterResourceMonitor")
                        except Exception as e:
                            logger.warning(f"⚠️  推送快照到 ClusterResourceMonitor 失败: {e}")

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

    def reset(self):
        """
        完全重置调度器状态，包括：
        - 停止健康检查线程
        - 停止task lifecycle manager
        - 删除已有的ClusterResourceMonitor（如果存在）
        - 清空所有本地引用
        - 关闭Ray连接

        使用场景：当需要完全重新初始化调度器时调用此方法
        """
        logger.info("🔄 开始重置调度器...")

        try:
            # 1. 停止健康检查线程
            self._stop_health_checker_thread()

            # 2. 停止task lifecycle manager
            if self.task_lifecycle_manager is not None:
                try:
                    self.task_lifecycle_manager.stop()
                    logger.info("   ✅ Task lifecycle manager已停止")
                except Exception as e:
                    logger.warning(f"   ⚠️ 停止task lifecycle manager时出错: {e}")

            # 3. 尝试删除已有的ClusterResourceMonitor detached actor
            # 无论当前状态如何，都应该尝试连接并删除monitor
            try:
                # 需要连接到monitor所在的集群才能删除
                monitor_cluster = self._monitor_cluster_name if hasattr(self, '_monitor_cluster_name') else "centos"
                logger.info(f"   🔗 准备连接到{monitor_cluster}集群以删除monitor...")

                # 检查是否需要重新连接到monitor集群
                need_connect = False
                if not ray.is_initialized():
                    logger.info("   Ray未初始化，需要连接到monitor集群")
                    need_connect = True
                else:
                    # 检查当前连接是否在monitor集群上
                    try:
                        from ray_multicluster_scheduler.control_plane.config import ConfigManager
                        config_manager = ConfigManager(self.__class__._config_file_path)
                        cluster_configs = {config.name: config for config in config_manager.get_cluster_configs()}
                        if monitor_cluster in cluster_configs:
                            target_address = cluster_configs[monitor_cluster].head_address
                            current_address = ray.get_runtime_context().gcs_address
                            if target_address not in current_address:
                                logger.info(f"   当前连接的集群不是{monitor_cluster}，需要重新连接")
                                need_connect = True
                            else:
                                logger.info(f"   已连接到{monitor_cluster}集群")
                    except Exception as check_error:
                        logger.warning(f"   检查当前连接时出错: {check_error}，将尝试重新连接")
                        need_connect = True

                # 如果需要，连接到monitor集群
                if need_connect:
                    try:
                        from ray_multicluster_scheduler.control_plane.config import ConfigManager
                        config_manager = ConfigManager(self.__class__._config_file_path)
                        cluster_configs = {config.name: config for config in config_manager.get_cluster_configs()}
                        if monitor_cluster in cluster_configs:
                            target_cluster = cluster_configs[monitor_cluster]
                            target_address = f"ray://{target_cluster.head_address}"

                            if ray.is_initialized():
                                logger.info("   关闭当前Ray连接...")
                                ray.shutdown()
                                time.sleep(0.5)

                            init_runtime_env = target_cluster.runtime_env.copy() if target_cluster.runtime_env else {}
                            logger.info(f"   正在连接到{monitor_cluster}集群: {target_address}")
                            ray.init(
                                address=target_address,
                                runtime_env=init_runtime_env if init_runtime_env else None,
                                ignore_reinit_error=True
                            )
                            logger.info(f"   ✅ 已连接到{monitor_cluster}集群以删除monitor")
                        else:
                            logger.error(f"   ❌ 配置中未找到集群 '{monitor_cluster}'")
                            raise Exception(f"集群 '{monitor_cluster}' 不存在于配置中")
                    except Exception as e:
                        logger.error(f"   ❌ 连接到monitor集群失败: {e}")
                        import traceback
                        logger.error(f"   异常堆栈:\n{traceback.format_exc()}")
                        raise  # 抛出异常，因为无法连接就无法删除

                # 尝试获取并删除所有名为cluster_resource_monitor的actor
                deleted_count = 0
                MONITOR_NAMESPACE = "ray_multicluster_scheduler"

                # 首先尝试删除固定namespace的monitor
                try:
                    logger.info(f"   🔍 尝试删除固定namespace的monitor (namespace: {MONITOR_NAMESPACE})")
                    actor = ray.get_actor("cluster_resource_monitor", namespace=MONITOR_NAMESPACE)
                    ray.kill(actor, no_restart=True)  # no_restart=True 对于 detached actor 很重要
                    deleted_count += 1
                    logger.info(f"   ✅ 已删除ClusterResourceMonitor (namespace: {MONITOR_NAMESPACE})")
                except ValueError:
                    logger.info(f"   ℹ️ 在{MONITOR_NAMESPACE} namespace中没有找到monitor")
                except Exception as e:
                    logger.error(f"   ❌ 删除固定 namespace monitor 失败: {e}")
                    import traceback
                    logger.error(f"   异常堆栈:\n{traceback.format_exc()}")

                # 删除 BackgroundHealthChecker
                try:
                    logger.info(f"   🔍 尝试删除 BackgroundHealthChecker (namespace: {MONITOR_NAMESPACE})")
                    actor = ray.get_actor("background_health_checker", namespace=MONITOR_NAMESPACE)
                    ray.kill(actor, no_restart=True)
                    deleted_count += 1
                    logger.info(f"   ✅ 已删除 BackgroundHealthChecker")
                except ValueError:
                    logger.info(f"   ℹ️  在 {MONITOR_NAMESPACE} namespace 中没有找到 BackgroundHealthChecker")
                except Exception as e:
                    logger.error(f"   ❌ 删除 BackgroundHealthChecker 失败: {e}")
                    import traceback
                    logger.error(f"   异常堆栈:\n{traceback.format_exc()}")

                # 然后遍历所有namespace，清理旧的随机namespace的monitor
                try:
                    logger.info("   🔍 扫描其他namespace中的monitor...")
                    # 使用ray.util.list_named_actors()列出所有命名actor
                    named_actors = ray.util.list_named_actors(all_namespaces=True)

                    for actor_info in named_actors:
                        if actor_info['name'] == 'cluster_resource_monitor':
                            try:
                                # 获取namespace，注意可能是字节串
                                namespace = actor_info.get('namespace', 'default')
                                # 如果是字节串，转换为字符串
                                if isinstance(namespace, bytes):
                                    namespace = namespace.decode('utf-8')

                                # 跳过已经删除的固定namespace
                                if namespace == MONITOR_NAMESPACE:
                                    continue

                                logger.info(f"   🔍 尝试删除旧monitor: name={actor_info['name']}, namespace={namespace}")
                                actor = ray.get_actor(actor_info['name'], namespace=namespace)
                                ray.kill(actor, no_restart=True)  # no_restart=True 对于 detached actor 很重要
                                deleted_count += 1
                                logger.info(f"   ✅ 已删除旧ClusterResourceMonitor (namespace: {namespace})")
                            except Exception as e:
                                logger.error(f"   ❌ 删除monitor失败 (namespace: {namespace}): {e}")
                                import traceback
                                logger.error(f"   异常堆栈:\n{traceback.format_exc()}")
                except Exception as e:
                    logger.error(f"   ❌ 扫描其他namespace时出错: {e}")
                    import traceback
                    logger.error(f"   异常堆栈:\n{traceback.format_exc()}")

                if deleted_count > 0:
                    logger.info(f"   ✅ 共删除了 {deleted_count} 个ClusterResourceMonitor实例")
                else:
                    logger.info("   ℹ️ 没有找到需要删除的ClusterResourceMonitor")

                # 验证删除结果：检查是否还有剩余的monitor
                try:
                    logger.info("   🔍 验证删除结果...")
                    remaining_monitors = []
                    all_actors = ray.util.list_named_actors(all_namespaces=True)
                    for actor_info in all_actors:
                        if actor_info['name'] == 'cluster_resource_monitor':
                            namespace = actor_info.get('namespace', 'default')
                            if isinstance(namespace, bytes):
                                namespace = namespace.decode('utf-8')
                            remaining_monitors.append(namespace)
                            logger.warning(f"   ⚠️ 仍然存在monitor: namespace={namespace}")

                    if remaining_monitors:
                        logger.error(f"   ❌ 删除验证失败！仍有 {len(remaining_monitors)} 个 monitor 存在")
                        logger.error(f"   剩余的namespace: {remaining_monitors}")
                    else:
                        logger.info("   ✅ 验证通过：所有ClusterResourceMonitor已被删除")
                except Exception as verify_error:
                    logger.warning(f"   ⚠️ 验证删除结果时出错: {verify_error}")

            except Exception as e:
                logger.error(f"   ❌ 删除ClusterResourceMonitor时出错: {e}")
                import traceback
                logger.error(f"   异常堆栈:\n{traceback.format_exc()}")
            # 4. 清空本地引用
            self._detached_monitor = None
            self.task_lifecycle_manager = None
            self._health_checker_thread = None

            # 5. 关闭Ray连接
            if ray.is_initialized():
                ray.shutdown()
                logger.info("   ✅ Ray连接已关闭")

            logger.info("✅ 调度器重置完成")

        except Exception as e:
            logger.error(f"❌ 重置调度器时出错: {e}")
            import traceback
            traceback.print_exc()

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
            # 注意：不再启动前台健康检查线程
            # BackgroundHealthChecker (detached actor) 已在后台全局收集快照
            # submit_task 执行完成后应立即退出，不需要前台监控线程

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

            # 注意：不再启动前台健康检查线程
            # BackgroundHealthChecker (detached actor) 已在后台全局收集快照
            # submit_job 通过 JobSubmissionClient 独立管理，不需要前台监控线程

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
            # 注意：不再启动前台健康检查线程
            # BackgroundHealthChecker (detached actor) 已在后台全局收集快照
            # submit_actor 执行完成后应立即退出，不需要前台监控线程

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


def initialize_scheduler_environment(config_file_path: Optional[str] = None, monitor_cluster_name: str = "centos") -> TaskLifecycleManager:
    """
    Initialize the multicluster scheduler environment.

    This is a convenience function that initializes the scheduler environment
    using the unified scheduler interface.

    Args:
        config_file_path (str, optional): Path to the cluster configuration YAML file.
            If not provided, the system will attempt to locate the configuration file
            in common locations or fall back to default configuration.
        monitor_cluster_name (str, optional): Name of the cluster to run the detached monitor on.
            Defaults to "centos". The cluster name will be validated against available clusters.

    Returns:
        TaskLifecycleManager: The initialized task lifecycle manager

    Raises:
        Exception: If there is an error during initialization, with full traceback information

    Example:
        >>> task_lifecycle_manager = initialize_scheduler_environment()
        >>> # With custom config file:
        >>> task_lifecycle_manager = initialize_scheduler_environment("/path/to/clusters.yaml")
        >>> # With custom monitor cluster:
        >>> task_lifecycle_manager = initialize_scheduler_environment(monitor_cluster_name="mac")
    """
    try:
        scheduler = get_unified_scheduler()
        task_lifecycle_manager = scheduler.initialize_environment(config_file_path=config_file_path, monitor_cluster_name=monitor_cluster_name)

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


def reset_scheduler_environment():
    """
    重置调度器环境，删除所有已有资源。

    这个函数会：
    - 停止所有后台线程
    - 停止task lifecycle manager
    - 删除所有ClusterResourceMonitor detached actor
    - 清空本地状态
    - 关闭Ray连接

    使用场景：
    - 当需要完全重新初始化调度器时
    - 当发现有多个ClusterResourceMonitor实例时
    - 当调度器出现异常需要清理时

    Example:
        >>> from ray_multicluster_scheduler.app.client_api import reset_scheduler_environment
        >>> reset_scheduler_environment()  # 重置所有状态
        >>> # 然后可以重新初始化
        >>> task_manager = initialize_scheduler_environment()
    """
    try:
        scheduler = get_unified_scheduler()
        scheduler.reset()
    except Exception as e:
        logger.error(f"Failed to reset scheduler environment: {e}")
        import traceback
        traceback.print_exc()
        raise


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