import ray
import logging
from typing import Dict, Optional, Callable, Any
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.common.circuit_breaker import ClusterCircuitBreakerManager
from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError

logger = logging.getLogger(__name__)


class Dispatcher:
    """Handles dispatching tasks to Ray clusters based on scheduling decisions."""

    def __init__(self, policy_engine: PolicyEngine, connection_manager: ConnectionLifecycleManager,
                 circuit_breaker_manager: ClusterCircuitBreakerManager = None):
        self.policy_engine = policy_engine
        self.connection_manager = connection_manager
        self.circuit_breaker_manager = circuit_breaker_manager or ClusterCircuitBreakerManager()
        # 为每个集群维护独立的Ray客户端实例
        self.cluster_clients = {}

    def dispatch_task(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> Any:
        """Schedule and submit a task to a Ray cluster."""
        # Make scheduling decision
        try:
            decision = self.policy_engine.schedule(task_desc, cluster_snapshots)
        except Exception as e:
            logger.error(f"Failed to make scheduling decision for task {task_desc.task_id}: {e}")
            raise NoHealthyClusterError(f"Could not schedule task {task_desc.task_id}: {e}")

        if not decision or not decision.cluster_name:
            raise NoHealthyClusterError(f"No suitable cluster found for task {task_desc.task_id}")

        # Submit task to the cluster through circuit breaker
        def submit_task_to_cluster():
            logger.info(f"Submitting task {task_desc.task_id} to cluster {decision.cluster_name}")

            # 获取指定集群的连接
            cluster_client = self._get_cluster_client(decision.cluster_name)

            # 准备runtime_env配置，根据集群设置相应的home_dir环境变量
            final_runtime_env = self._prepare_runtime_env_for_cluster(task_desc, decision.cluster_name)

            if task_desc.is_actor:
                # Submit as actor
                actor_class = task_desc.func_or_class
                # 如果提供了runtime_env，则使用它
                if final_runtime_env:
                    actor = actor_class.options(
                        name=task_desc.name,
                        runtime_env=final_runtime_env
                    ).remote(*task_desc.args, **task_desc.kwargs)
                else:
                    actor = actor_class.options(name=task_desc.name).remote(*task_desc.args, **task_desc.kwargs)
                return actor
            else:
                # Submit as remote function
                remote_func = task_desc.func_or_class
                # 先将普通函数转换为远程函数
                if final_runtime_env:
                    remote_func = ray.remote(remote_func).options(
                        name=task_desc.name,
                        runtime_env=final_runtime_env
                    )
                else:
                    remote_func = ray.remote(remote_func).options(name=task_desc.name)

                result = remote_func.remote(*task_desc.args, **task_desc.kwargs)
                return result

        try:
            future = self.circuit_breaker_manager.call_cluster(
                decision.cluster_name,
                submit_task_to_cluster
            )
            logger.info(f"Successfully submitted task {task_desc.task_id} to cluster {decision.cluster_name}")
            return future

        except Exception as e:
            logger.error(f"Failed to submit task {task_desc.task_id} to cluster {decision.cluster_name}: {e}")
            import traceback
            traceback.print_exc()
            # 标记集群连接为断开状态
            self.connection_manager.mark_cluster_disconnected(decision.cluster_name)
            raise TaskSubmissionError(f"Failed to submit task {task_desc.task_id} to cluster {decision.cluster_name}: {e}")

    def _get_cluster_client(self, cluster_name: str) -> Any:
        """获取指定集群的客户端连接，支持多集群并发"""
        try:
            # 从连接管理器获取连接
            connection = self.connection_manager.get_connection(cluster_name)
            if connection:
                logger.debug(f"Using existing connection for cluster {cluster_name}")
                return connection

            # 如果没有现有连接，建立新连接
            logger.info(f"Establishing new connection for cluster {cluster_name}")
            cluster_metadata = self.connection_manager.cluster_metadata.get(cluster_name)
            if not cluster_metadata:
                raise TaskSubmissionError(f"Cluster metadata not found for cluster {cluster_name}")

            # 构建集群特定的runtime_env配置
            runtime_env = self._build_runtime_env_for_cluster(cluster_metadata)

            # 连接到目标集群
            ray_client_address = f"ray://{cluster_metadata.head_address}"
            logger.info(f"Connecting to cluster {cluster_name} at {ray_client_address} with runtime_env: {runtime_env}")

            # 使用ignore_reinit_error=True避免重复初始化错误
            ray.init(
                address=ray_client_address,
                runtime_env=runtime_env,
                ignore_reinit_error=True
            )

            # 存储连接信息并标记为已连接
            self.cluster_clients[cluster_name] = ray_client_address
            self.connection_manager.mark_cluster_connected(cluster_name)
            logger.info(f"Successfully connected to cluster {cluster_name}")
            return ray_client_address

        except Exception as e:
            logger.error(f"Failed to get client for cluster {cluster_name}: {e}")
            # 标记集群连接为断开状态
            self.connection_manager.mark_cluster_disconnected(cluster_name)
            raise TaskSubmissionError(f"Could not get client for cluster {cluster_name}: {e}")

    def _build_runtime_env_for_cluster(self, cluster_metadata: ClusterMetadata) -> Optional[Dict]:
        """Build runtime environment configuration for a specific cluster."""
        # 从集群配置文件中获取runtime_env配置
        if not cluster_metadata.runtime_env:
            logger.info(f"No runtime_env configuration found for cluster {cluster_metadata.name}")
            return None

        # 直接使用集群配置中的runtime_env
        runtime_env = cluster_metadata.runtime_env.copy()
        logger.info(f"Configured runtime_env for cluster {cluster_metadata.name}: {runtime_env}")

        return runtime_env

    def _prepare_runtime_env_for_cluster(self, task_desc: TaskDescription, cluster_name: str) -> Optional[Dict]:
        """Prepare runtime environment - now simply returns None as runtime_env is configured at connection time."""
        # Since runtime_env is now configured at the cluster connection level,
        # we don't need to prepare it per task submission.
        # The cluster's default runtime_env will be used.
        logger.debug(f"Using cluster default runtime_env for task {task_desc.task_id} on cluster {cluster_name}")
        return None