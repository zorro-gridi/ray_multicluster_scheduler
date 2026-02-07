# 异常堆栈
2025-12-29 12:50:34,008 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - ERROR - Failed to create JobSubmissionClient for cluster centos: Failed to connect to Ray at address: http://192.168.5.7:8265.

Traceback (most recent call last):
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/scheduler/scheduler_core/dispatcher.py", line 97, in dispatch_job
    job_id = self.circuit_breaker_manager.call_cluster(
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/common/circuit_breaker.py", line 138, in call_cluster
    return circuit_breaker.call(func, *args, **kwargs)
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/common/circuit_breaker.py", line 66, in call
    raise e
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/common/circuit_breaker.py", line 61, in call
    result = func(*args, **kwargs)
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/scheduler/scheduler_core/dispatcher.py", line 81, in submit_job_to_cluster
    job_client = self._get_job_client(target_cluster)
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/scheduler/scheduler_core/dispatcher.py", line 152, in _get_job_client
    raise TaskSubmissionError(f"Could not get JobSubmissionClient for cluster {cluster_name}: {e}")
ray_multicluster_scheduler.common.exception.TaskSubmissionError: Could not get JobSubmissionClient for cluster centos: Failed to get JobSubmissionClient for cluster centos
2025-12-29 12:50:34,009 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - ERROR - 作业 job_f3c4420d 提交失败: Failed to submit job job_f3c4420d to cluster centos: Could not get JobSubmissionClient for cluster centos: Failed to get JobSubmissionClient for cluster centos
Traceback (most recent call last):
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/scheduler/scheduler_core/dispatcher.py", line 143, in _get_job_client
    raise TaskSubmissionError(f"Failed to get JobSubmissionClient for cluster {cluster_name}")
ray_multicluster_scheduler.common.exception.TaskSubmissionError: Failed to get JobSubmissionClient for cluster centos

# 相关接口
    def _create_job_client(self, cluster_metadata: ClusterMetadata) -> Optional[JobSubmissionClient]:
        """Create a JobSubmissionClient for a specific cluster based on its metadata."""
        try:
            # JobSubmissionClient使用HTTP地址，优先使用dashboard地址
            # 从dashboard地址提取host和port，如果dashboard地址为空则从head_address构建
            job_client_address = None

            if hasattr(cluster_metadata, 'dashboard') and cluster_metadata.dashboard:
                # 如果dashboard地址以http://或https://开头，则直接使用
                if cluster_metadata.dashboard.startswith('http'):
                    job_client_address = cluster_metadata.dashboard
                else:
                    job_client_address = f"http://{cluster_metadata.dashboard}"

            # 如果dashboard地址不可用或连接失败，尝试使用head_address构建8265端口
            if not job_client_address or not self._test_connection(cluster_metadata, job_client_address):
                head_addr = cluster_metadata.head_address
                if ":" in head_addr:
                    host, port = head_addr.rsplit(":", 1)
                    # 尝试使用8265端口，这是Ray Job Submission的默认端口
                    job_client_address = f"http://{host}:8265"
                else:
                    job_client_address = f"http://{head_addr}:8265"

            # 创建JobSubmissionClient，JobSubmissionClient不接受runtime_env参数
            # runtime_env在提交作业时指定
            job_client = JobSubmissionClient(
                address=job_client_address,
                create_cluster_if_needed=True,
            )

            logger.info(f"Created JobSubmissionClient for cluster {cluster_metadata.name} with address {job_client_address}")
            return job_client
        except Exception as e:
            logger.error(f"Failed to create JobSubmissionClient for cluster {cluster_metadata.name}: {e}")
            return None

# 调试需求
为什么没有按照系统集群的配置 dashboard: http://192.168.5.7:31591 进行初始化？