"""
Job Tracker for managing scheduler-submitted jobs.

This module tracks all jobs submitted by the scheduler to enable
precise cleanup during shutdown without affecting pre-existing jobs.
"""

import threading
from typing import Dict, Set
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class JobTracker:
    """
    Tracks all jobs submitted by the scheduler for precise lifecycle management.

    This ensures that during Ctrl+C shutdown, only jobs submitted by this
    scheduler instance are stopped, not pre-existing jobs on the cluster.
    """

    def __init__(self):
        """Initialize the job tracker."""
        # {cluster_name: set(job_ids)}
        self.tracked_jobs: Dict[str, Set[str]] = {}
        self._lock = threading.Lock()

    def register_job(self, cluster_name: str, job_id: str):
        """
        Register a job submitted by the scheduler.

        Args:
            cluster_name: The cluster where the job was submitted
            job_id: The unique job identifier
        """
        with self._lock:
            if cluster_name not in self.tracked_jobs:
                self.tracked_jobs[cluster_name] = set()
            self.tracked_jobs[cluster_name].add(job_id)
            logger.debug(f"Registered job {job_id} on cluster {cluster_name}")

    def unregister_job(self, cluster_name: str, job_id: str):
        """
        Unregister a job (completed or stopped).

        Args:
            cluster_name: The cluster where the job was running
            job_id: The unique job identifier
        """
        with self._lock:
            if cluster_name in self.tracked_jobs:
                self.tracked_jobs[cluster_name].discard(job_id)
                logger.debug(f"Unregistered job {job_id} from cluster {cluster_name}")

    def get_tracked_jobs(self, cluster_name: str = None) -> Dict[str, Set[str]]:
        """
        Get all tracked jobs.

        Args:
            cluster_name: Optional cluster name to filter by

        Returns:
            Dictionary mapping cluster names to sets of job IDs
        """
        with self._lock:
            if cluster_name:
                return {cluster_name: self.tracked_jobs.get(cluster_name, set()).copy()}
            # Return a deep copy to avoid external modification
            return {
                name: job_ids.copy()
                for name, job_ids in self.tracked_jobs.items()
            }

    def get_total_count(self) -> int:
        """
        Get the total number of tracked jobs across all clusters.

        Returns:
            Total count of tracked jobs
        """
        with self._lock:
            return sum(len(jobs) for jobs in self.tracked_jobs.values())

    def clear_all(self):
        """Clear all tracked jobs."""
        with self._lock:
            self.tracked_jobs.clear()
            logger.debug("Cleared all tracked jobs")
