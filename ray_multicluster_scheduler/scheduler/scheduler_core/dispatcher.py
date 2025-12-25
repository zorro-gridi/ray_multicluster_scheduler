import ray
import logging
from typing import Dict, Optional, Callable, Any
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.common.circuit_breaker import ClusterCircuitBreakerManager
from ray_multicluster_scheduler.common.exception import NoHealthyClusterError, TaskSubmissionError


logger = logging.getLogger(__name__)


class Dispatcher:
    """Handles dispatching tasks to Ray clusters based on scheduling decisions."""

    def __init__(self, connection_manager: ConnectionLifecycleManager,
                 circuit_breaker_manager: ClusterCircuitBreakerManager = None):
        self.connection_manager = connection_manager
        self.circuit_breaker_manager = circuit_breaker_manager or ClusterCircuitBreakerManager()
        # 为每个集群维护独立的Ray客户端实例
        self.cluster_clients = {}

    def dispatch_task(self, task_desc: TaskDescription, target_cluster: str = None) -> Any:
        """Submit a task to a specified Ray cluster."""
        # If no target cluster is provided, we cannot proceed
        if not target_cluster:
            raise NoHealthyClusterError(f"No target cluster specified for task {task_desc.task_id}")

        # Submit task to the cluster through circuit breaker
        def submit_task_to_cluster():
            logger.info(f"Submitting task {task_desc.task_id} to cluster {target_cluster}")

            # 确保连接到正确的集群
            client = self._get_cluster_client(target_cluster)

            # 准备runtime_env配置，根据集群设置相应的home_dir环境变量
            final_runtime_env = self._prepare_runtime_env_for_cluster_target(task_desc, target_cluster)

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
                target_cluster,
                submit_task_to_cluster
            )
            logger.info(f"Successfully submitted task {task_desc.task_id} to cluster {target_cluster}")
            return future

        except Exception as e:
            logger.error(f"Failed to submit task {task_desc.task_id} to cluster {target_cluster}: {e}")
            import traceback
            traceback.print_exc()
            # 标记集群连接为断开状态
            self.connection_manager.mark_cluster_disconnected(target_cluster)
            raise TaskSubmissionError(f"Failed to submit task {task_desc.task_id} to cluster {target_cluster}: {e}")

    def dispatch_job(self, job_desc: JobDescription, target_cluster: str = None) -> str:
        """Submit a job to a specified Ray cluster using JobSubmissionClient."""
        # If no target cluster is provided, we cannot proceed
        if not target_cluster:
            raise NoHealthyClusterError(f"No target cluster specified for job {job_desc.job_id}")

        # Submit job to the cluster through circuit breaker
        def submit_job_to_cluster():
            logger.info(f"Submitting job {job_desc.job_id} to cluster {target_cluster}")

            # Get JobSubmissionClient for the target cluster
            job_client = self._get_job_client(target_cluster)

            # Prepare runtime_env for the target cluster, combining job and cluster configs
            final_runtime_env = self._prepare_runtime_env_for_cluster_target(job_desc, target_cluster)

            # Submit the job using JobSubmissionClient
            job_id = job_client.submit_job(
                entrypoint=job_desc.entrypoint,
                runtime_env=final_runtime_env,
                metadata=job_desc.metadata,
                job_id=job_desc.job_id
            )

            return job_id

        try:
            job_id = self.circuit_breaker_manager.call_cluster(
                target_cluster,
                submit_job_to_cluster
            )
            logger.info(f"Successfully submitted job {job_desc.job_id} to cluster {target_cluster}, job_id: {job_id}")
            return job_id

        except Exception as e:
            logger.error(f"Failed to submit job {job_desc.job_id} to cluster {target_cluster}: {e}")
            import traceback
            traceback.print_exc()
            # 标记集群连接为断开状态
            self.connection_manager.mark_cluster_disconnected(target_cluster)
            raise TaskSubmissionError(f"Failed to submit job {job_desc.job_id} to cluster {target_cluster}: {e}")

    def _get_cluster_client(self, cluster_name: str) -> Any:
        """获取指定集群的客户端连接，支持多集群并发"""
        try:
            # 使用连接管理器确保连接到正确的集群
            success = self.connection_manager.ensure_cluster_connection(cluster_name)
            if not success:
                raise TaskSubmissionError(f"Failed to establish connection to cluster {cluster_name}")

            # 存储连接信息并标记为已连接
            cluster_metadata = self.connection_manager.cluster_metadata.get(cluster_name)
            if not cluster_metadata:
                raise TaskSubmissionError(f"Cluster metadata not found for cluster {cluster_name}")

            ray_client_address = f"ray://{cluster_metadata.head_address}"
            self.cluster_clients[cluster_name] = ray_client_address
            self.connection_manager.mark_cluster_connected(cluster_name)
            logger.info(f"Successfully connected to cluster {cluster_name}")
            return ray_client_address

        except Exception as e:
            logger.error(f"Failed to get client for cluster {cluster_name}: {e}")
            # 标记集群连接为断开状态
            self.connection_manager.mark_cluster_disconnected(cluster_name)
            raise TaskSubmissionError(f"Could not get client for cluster {cluster_name}: {e}")

    def _get_job_client(self, cluster_name: str) -> Any:
        """获取指定集群的JobSubmissionClient连接"""
        try:
            # Get JobSubmissionClient from connection manager
            job_client = self.connection_manager.get_job_client(cluster_name)
            if not job_client:
                raise TaskSubmissionError(f"Failed to get JobSubmissionClient for cluster {cluster_name}")

            logger.info(f"Successfully obtained JobSubmissionClient for cluster {cluster_name}")
            return job_client

        except Exception as e:
            logger.error(f"Failed to get JobSubmissionClient for cluster {cluster_name}: {e}")
            # 标记集群连接为断开状态
            self.connection_manager.mark_cluster_disconnected(cluster_name)
            raise TaskSubmissionError(f"Could not get JobSubmissionClient for cluster {cluster_name}: {e}")

    def _prepare_runtime_env_for_cluster_target(self, target_desc: Any, cluster_name: str) -> Optional[Dict]:
        """Prepare runtime environment for any target (task or job) based on target cluster configuration."""
        # Get the cluster metadata
        cluster_metadata = self.connection_manager.cluster_metadata.get(cluster_name)
        if not cluster_metadata or not hasattr(cluster_metadata, 'runtime_env'):
            logger.warning(f"No runtime_env configuration found for cluster {cluster_name}")
            # Return the target's runtime_env if no cluster config exists
            # Check if it's a task description
            if hasattr(target_desc, 'runtime_env'):
                return target_desc.runtime_env
            else:
                return None

        cluster_runtime_env = cluster_metadata.runtime_env

        # Get the target's runtime_env
        target_runtime_env = None
        if hasattr(target_desc, 'runtime_env'):
            target_runtime_env = target_desc.runtime_env

        # If target doesn't have a runtime_env, use the cluster's default
        if not target_runtime_env:
            logger.info(f"Using cluster default runtime_env for target {target_desc.job_id if hasattr(target_desc, 'job_id') else target_desc.task_id} on cluster {cluster_name}")
            return cluster_runtime_env

        # If both target and cluster have runtime_env, merge them
        # Cluster config takes precedence for specific keys like conda
        final_runtime_env = target_runtime_env.copy()

        # Override with cluster-specific settings
        if cluster_runtime_env:
            for key, value in cluster_runtime_env.items():
                final_runtime_env[key] = value

        logger.info(f"Merged runtime_env for target {target_desc.job_id if hasattr(target_desc, 'job_id') else target_desc.task_id} on cluster {cluster_name}: {final_runtime_env}")
        return final_runtime_env