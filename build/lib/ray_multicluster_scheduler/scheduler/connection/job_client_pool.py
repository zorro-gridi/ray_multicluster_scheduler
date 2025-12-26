"""
Job client pool for managing JobSubmissionClient connections to different clusters.
"""

from re import T
import time
from typing import Dict, Optional, Any
from ray.job_submission import JobSubmissionClient
from ray_multicluster_scheduler.common.model import ClusterMetadata
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.control_plane.config import ConfigManager

logger = get_logger(__name__)

class JobClientPool:
    """Manages a pool of JobSubmissionClient connections to different clusters."""

    def __init__(self, config_manager: ConfigManager):
        self.clients: Dict[str, JobSubmissionClient] = {}
        self.active_clients: Dict[str, bool] = {}
        # 存储连接时间戳，用于检测连接是否过期
        self.client_timestamps: Dict[str, float] = {}
        self.config_manager = config_manager

    def add_cluster(self, cluster_metadata: ClusterMetadata):
        """Add a cluster to the JobSubmissionClient pool."""
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

            self.clients[cluster_metadata.name] = job_client
            self.active_clients[cluster_metadata.name] = True
            self.client_timestamps[cluster_metadata.name] = time.time()

            logger.info(f"Added cluster {cluster_metadata.name} to job client pool with address {job_client_address}")
        except Exception as e:
            logger.error(f"Failed to add cluster {cluster_metadata.name} to job client pool: {e}")
            raise

    def _test_connection(self, cluster_metadata: ClusterMetadata, address: str) -> bool:
        """Test if the address is accessible for JobSubmissionClient"""
        try:
            import requests
            # 尝试连接到Ray dashboard的jobs API端点
            response = requests.get(f"{address}/api/jobs/", timeout=5)
            # 如果返回200或401（认证）等，说明端口是通的
            return response.status_code in [200, 401, 403, 405]
        except:
            # 如果requests不可用或连接失败，返回False
            return False

    def get_client(self, cluster_name: str) -> Optional[JobSubmissionClient]:
        """Get a JobSubmissionClient for a specific cluster."""
        if cluster_name in self.clients and self.active_clients.get(cluster_name, False):
            # 更新最后使用时间
            self.client_timestamps[cluster_name] = time.time()
            return self.clients[cluster_name]
        return None

    def submit_job(self, cluster_name: str, **job_kwargs) -> str:
        """Submit a job to the specified cluster."""
        if cluster_name not in self.clients:
            raise ValueError(f"Unknown cluster: {cluster_name}")

        job_client = self.clients[cluster_name]
        job_id = job_client.submit_job(**job_kwargs)

        logger.info(f"Submitted job to cluster {cluster_name}, job_id: {job_id}")
        return job_id

    def get_job_status(self, cluster_name: str, job_id: str):
        """Get the status of a job."""
        if cluster_name not in self.clients:
            raise ValueError(f"Unknown cluster: {cluster_name}")

        job_client = self.clients[cluster_name]
        return job_client.get_job_status(job_id)

    def stop_job(self, cluster_name: str, job_id: str):
        """Stop a job."""
        if cluster_name not in self.clients:
            raise ValueError(f"Unknown cluster: {cluster_name}")

        job_client = self.clients[cluster_name]
        job_client.stop_job(job_id)

        logger.info(f"Stopped job {job_id} on cluster {cluster_name}")

    def list_jobs(self, cluster_name: str):
        """List all jobs on a cluster."""
        if cluster_name not in self.clients:
            raise ValueError(f"Unknown cluster: {cluster_name}")

        job_client = self.clients[cluster_name]
        return job_client.list_jobs()

    def remove_cluster(self, cluster_name: str):
        """Remove a cluster from the job client pool."""
        if cluster_name in self.clients:
            del self.clients[cluster_name]
            if cluster_name in self.active_clients:
                del self.active_clients[cluster_name]
            if cluster_name in self.client_timestamps:
                del self.client_timestamps[cluster_name]
            logger.info(f"Removed cluster {cluster_name} from job client pool")