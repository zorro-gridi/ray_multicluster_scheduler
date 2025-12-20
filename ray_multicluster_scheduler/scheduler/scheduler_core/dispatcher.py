import ray
import logging
from typing import Dict, Optional, Callable
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

    def dispatch_task(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> ray.ObjectRef:
        """Schedule and submit a task to a Ray cluster."""
        # Make scheduling decision
        try:
            decision = self.policy_engine.schedule(task_desc, cluster_snapshots)
        except Exception as e:
            logger.error(f"Failed to make scheduling decision for task {task_desc.task_id}: {e}")
            raise NoHealthyClusterError(f"Could not schedule task {task_desc.task_id}: {e}")

        if not decision.cluster_name:
            raise NoHealthyClusterError(f"No suitable cluster found for task {task_desc.task_id}")

        # Submit task to the cluster through circuit breaker
        def submit_task_to_cluster():
            logger.info(f"Submitting task {task_desc.task_id} to cluster {decision.cluster_name}")

            # 在提交任务前，确保连接到正确的集群
            self._connect_to_cluster(decision.cluster_name)

            # Submit the task - connection should already be established
            # We don't need to call ray.init() again since we're already connected

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
                # 如果提供了runtime_env，则使用它
                if final_runtime_env:
                    result = remote_func.options(
                        name=task_desc.name,
                        runtime_env=final_runtime_env
                    ).remote(*task_desc.args, **task_desc.kwargs)
                else:
                    result = remote_func.options(name=task_desc.name).remote(*task_desc.args, **task_desc.kwargs)
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
            raise TaskSubmissionError(f"Failed to submit task {task_desc.task_id} to cluster {decision.cluster_name}: {e}")

    def _connect_to_cluster(self, cluster_name: str):
        """Connect to the specified cluster with runtime environment configuration."""
        try:
            # 获取集群元数据
            cluster_metadata = self.connection_manager.cluster_metadata.get(cluster_name)
            if not cluster_metadata:
                raise TaskSubmissionError(f"Cluster metadata not found for cluster {cluster_name}")

            # 连接到指定集群
            ray_client_address = f"ray://{cluster_metadata.head_address}"
            logger.info(f"Connecting to cluster {cluster_name} at {ray_client_address}")

            # 断开当前连接（如果有的话）
            try:
                ray.shutdown()
            except:
                pass

            # 构建集群特定的runtime_env配置
            runtime_env = self._build_runtime_env_for_cluster(cluster_metadata)

            # 连接到目标集群，使用集群特定的runtime_env配置
            ray.init(
                address=ray_client_address,
                runtime_env=runtime_env,
                ignore_reinit_error=True
            )

            logger.info(f"Successfully connected to cluster {cluster_name} with runtime_env: {runtime_env}")
        except Exception as e:
            logger.error(f"Failed to connect to cluster {cluster_name}: {e}")
            raise TaskSubmissionError(f"Could not connect to cluster {cluster_name}: {e}")

    def _build_runtime_env_for_cluster(self, cluster_metadata: ClusterMetadata) -> Optional[Dict]:
        """Build runtime environment configuration for a specific cluster."""
        # 从集群配置文件中获取runtime_env配置
        # 注意：这里我们假设ConfigManager已经正确解析了配置文件中的runtime_env部分
        # 并将相关信息存储在ClusterMetadata对象中

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
