"""
Cluster manager that handles cluster config_metaurations and connections.
"""

import ray
import time
import logging
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from ray_multicluster_scheduler.common.model import ClusterMetadata, ResourceSnapshot, ClusterHealth
from ray_multicluster_scheduler.common.logging import get_logger


logger = get_logger(__name__)


class ClusterManager:
    """Manages Ray cluster config_metaurations and connections."""

    def __init__(self, cluster_metadata: Optional[List[ClusterMetadata]] = None):
        """Initialize the cluster manager."""
        self.clusters: Dict[str, ClusterMetadata] = {}
        if cluster_metadata:
            for config_meta in cluster_metadata:
                self.clusters[config_meta.name] = config_meta
        self.health_status: Dict[str, ClusterHealth] = {}
        self.active_connections: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self.metrics = {
            "total_checks": 0,
            "successful_checks": 0,
            "failed_checks": 0,
            "last_refresh": None
        }
        # Initialize health status for all clusters
        for name in self.clusters:
            self.health_status[name] = ClusterHealth()

    def add_cluster(self, config_meta: ClusterMetadata):
        """Add a cluster config_metauration."""
        self.clusters[config_meta.name] = config_meta
        self.health_status[config_meta.name] = ClusterHealth()
        logger.info(f"Added cluster config_metauration: {config_meta.name}")

    def remove_cluster(self, name: str):
        """Remove a cluster config_metauration."""
        if name in self.clusters:
            del self.clusters[name]
            del self.health_status[name]
            logger.info(f"Removed cluster config_metauration: {name}")

    def get_cluster_config_meta(self, name: str) -> Optional[ClusterMetadata]:
        """Get cluster config_metauration by name."""
        return self.clusters.get(name)

    def get_all_cluster_metadata(self) -> Dict[str, ClusterMetadata]:
        """Get all cluster config_metaurations."""
        return self.clusters.copy()

    def get_cluster_home_dir(self, cluster_name: str) -> Optional[str]:
        """Get the home directory for a specific cluster."""
        config_meta = self.get_cluster_config_meta(cluster_name)
        if config_meta and config_meta.runtime_env:
            env_vars = config_meta.runtime_env.get('env_vars', {})
            return env_vars.get('home_dir')
        return None


    def select_best_cluster(self, required_resources: Dict[str, float] = None) -> Optional[str]:
        """Select the best cluster based on health scores and resource requirements."""
        if required_resources is None:
            required_resources = {}

        # Filter healthy clusters
        healthy_clusters = {
            name: self.health_status[name]
            for name in self.clusters
            if self.health_status[name].available
        }

        # Filter clusters with sufficient resources
        sufficient_clusters = {}
        for name, health in healthy_clusters.items():
            resources = health.resources
            available_cpu = resources.get("cpu_free", 0)
            available_gpu = resources.get("gpu_free", 0)
            required_cpu = required_resources.get("CPU", 0)
            required_gpu = required_resources.get("GPU", 0)

            if available_cpu >= required_cpu and available_gpu >= required_gpu:
                sufficient_clusters[name] = health

        # Check if we have any sufficient clusters
        if not sufficient_clusters:
            logger.warning("没有找到满足资源需求的集群")
            return None

        # Select the best cluster based on score
        best_cluster = max(sufficient_clusters.items(), key=lambda x: x[1].score)[0]
        best_score = sufficient_clusters[best_cluster].score

        # Log selection details
        if len(sufficient_clusters) > 1:
            # Log details for load balancing
            for name, health in sufficient_clusters.items():
                resources = health.resources
                cpu_free = resources.get("cpu_free", 0)
                cpu_total = resources.get("cpu_total", 0)
                gpu_free = resources.get("gpu_free", 0)
                gpu_total = resources.get("gpu_total", 0)
                cpu_utilization = resources.get("cpu_utilization", 0)

                logger.info(f"集群 [{name}] 评分={health.score:.1f}, "
                           f"CPU负载={cpu_utilization:.1%} ({cpu_free:.2f}/{cpu_total:.2f}), "
                           f"GPU={gpu_free}/{gpu_total}")

        logger.info(f"选择最佳集群 [{best_cluster}] 进行负载均衡: "
                   f"评分={best_score:.1f}")

        return best_cluster

    def get_best_cluster(self, requirements: Optional[Dict] = None) -> Optional[str]:
        """Get the best cluster based on current health and requirements.

        This is a wrapper method for select_best_cluster to maintain API compatibility.

        Args:
            requirements: Dictionary containing resource requirements and tags

        Returns:
            Name of the best cluster or None if no suitable cluster found
        """
        if requirements is None:
            requirements = {}

        # Extract resource requirements
        required_resources = requirements.get("resources", {})

        return self.select_best_cluster(required_resources)