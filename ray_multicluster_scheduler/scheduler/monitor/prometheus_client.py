"""
Prometheus client for fetching cluster resource metrics using object-oriented design.
"""
import requests
import time
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

from ray_multicluster_scheduler.common.model import ResourceSnapshot
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)

try:
    from prometheus_api_client import PrometheusConnect
    PROMETHEUS_SDK_AVAILABLE = True
except ImportError:
    PROMETHEUS_SDK_AVAILABLE = False
    PrometheusConnect = None


class PrometheusQuerier(ABC):
    """Abstract base class for Prometheus queriers."""

    @abstractmethod
    def query(self, query: str) -> List[Dict]:
        """Send Prometheus query and return results."""
        pass


class HTTPPrometheusQuerier(PrometheusQuerier):
    """Prometheus querier using HTTP API."""

    def __init__(self, url: str):
        self.url = url if "/api/v1/query" in url else f"{url}/api/v1/query"

    def query(self, query: str) -> List[Dict]:
        """Send Prometheus instant query via HTTP API."""
        try:
            r = requests.get(self.url, params={"query": query}, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data["status"] != "success":
                raise RuntimeError("Prometheus query failed")
            return data["data"]["result"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Prometheus HTTP query request failed: {e}")
            return []
        except KeyError as e:
            logger.error(f"Prometheus HTTP query response format error: {e}")
            return []


class SDKPrometheusQuerier(PrometheusQuerier):
    """Prometheus querier using Python SDK."""

    def __init__(self, url: str):
        self.url = url

    def query(self, query: str) -> List[Dict]:
        """Send Prometheus instant query via SDK."""
        if not PROMETHEUS_SDK_AVAILABLE:
            logger.error("prometheus_api_client not available")
            return []

        try:
            prom = PrometheusConnect(url=self.url, disable_ssl=True)
            result = prom.custom_query(query=query)
            # Convert result to match the expected format
            formatted_result = []
            for item in result:
                formatted_result.append({
                    "metric": item.get("metric", {}),
                    "value": [None, item["value"][1] if "value" in item else "0"]
                })
            return formatted_result
        except Exception as e:
            logger.error(f"Prometheus SDK query request failed: {e}")
            return []


class PrometheusClient:
    """Main Prometheus client that manages different queriers for different clusters."""

    def __init__(self):
        self.queriers: Dict[str, PrometheusQuerier] = {}
        self._initialize_queriers()

    def _initialize_queriers(self):
        """Initialize queriers for known clusters."""
        cluster_configs = {
            "mac": {
                "type": "http",
                "url": "http://192.168.5.7:9092",  # mac cluster Prometheus
            },
            "centos": {
                "type": "sdk",
                "url": "http://192.168.5.7:30090",  # centos cluster Prometheus via NodePort
            }
        }

        for cluster_name, config in cluster_configs.items():
            if config["type"] == "sdk":
                self.queriers[cluster_name] = SDKPrometheusQuerier(config["url"])
            else:  # http
                self.queriers[cluster_name] = HTTPPrometheusQuerier(config["url"])

    def get_querier(self, cluster_name: str) -> PrometheusQuerier:
        """Get appropriate querier for the cluster."""
        return self.queriers.get(cluster_name)

    def query(self, query: str, cluster_name: str) -> List[Dict]:
        """Query Prometheus for a specific cluster."""
        querier = self.get_querier(cluster_name)
        if querier:
            return querier.query(query)
        else:
            # Fallback to HTTP querier with default URL
            default_querier = HTTPPrometheusQuerier("http://192.168.5.7:9092/api/v1/query")
            return default_querier.query(query)


# Global instance
_prometheus_client = PrometheusClient()


def get_centos_cluster_snapshot() -> Dict[str, Dict]:
    """
    Returns CentOS cluster resource snapshot object from Prometheus metrics.
    """
    cluster_name = "centos"
    snapshot = {}

    try:
        # Determine the querier based on cluster name
        querier = _prometheus_client.get_querier(cluster_name)
        if not querier:
            logger.warning(f"No querier found for cluster {cluster_name}, using default")
            querier = HTTPPrometheusQuerier("http://192.168.5.7:9092/api/v1/query")

        # Use centos-specific queries with node-level metrics
        cpu_query = '100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))'
        cpu_result = _prometheus_client.query(cpu_query, cluster_name)
        cpu_percent = float(cpu_result[0]['value'][1]) if cpu_result else 0.0

        cpu_used_query = 'sum(rate(node_cpu_seconds_total{mode!="idle"}[5m]))'
        cpu_used_result = _prometheus_client.query(cpu_used_query, cluster_name)
        cpu_used_cores = float(cpu_used_result[0]['value'][1]) if cpu_used_result else 0.0

        cpu_total_query = 'count(node_cpu_seconds_total{mode="idle"})'
        cpu_total_result = _prometheus_client.query(cpu_total_query, cluster_name)
        cpu_total_cores = float(cpu_total_result[0]['value'][1]) if cpu_total_result else 0.0

        # Memory statistics
        mem_total_query = 'sum(node_memory_MemTotal_bytes)'
        mem_avail_query = 'sum(node_memory_MemAvailable_bytes)'

        mem_total_result = _prometheus_client.query(mem_total_query, cluster_name)
        mem_avail_result = _prometheus_client.query(mem_avail_query, cluster_name)

        mem_total_bytes = float(mem_total_result[0]['value'][1]) if mem_total_result else 0
        mem_avail_bytes = float(mem_avail_result[0]['value'][1]) if mem_avail_result else 0
        mem_used_bytes = mem_total_bytes - mem_avail_bytes

        mem_total_gib = mem_total_bytes / (1024 ** 3)
        mem_avail_gib = mem_avail_bytes / (1024 ** 3)
        mem_used_gib = mem_used_bytes / (1024 ** 3)

        # For centos, we'll use node count from a different query if available
        nodes_query = 'count(count(node_cpu_seconds_total) by (instance))'
        nodes_result = _prometheus_client.query(nodes_query, cluster_name)
        node_count = int(float(nodes_result[0]['value'][1])) if nodes_result else 0

        # GPU query for centos (if available)
        gpu_query = 'count(node_gpu_product_name)'  # This is a placeholder, actual query may vary
        gpu_result = _prometheus_client.query(gpu_query, cluster_name)
        gpu_total = float(gpu_result[0]['value'][1]) if gpu_result else 0

        # 确保CPU使用量不超过总数，避免负值问题
        adjusted_cpu_used_cores = min(cpu_used_cores, cpu_total_cores) if cpu_total_cores > 0 else 0
        cpu_usage_percent = (adjusted_cpu_used_cores / cpu_total_cores * 100) if cpu_total_cores > 0 else 0

        snapshot[cluster_name] = {
            "node_count": node_count,
            "cluster_cpu_total_cores": cpu_total_cores,
            "cluster_cpu_used_cores": adjusted_cpu_used_cores,
            "cluster_cpu_usage_percent": round(cpu_usage_percent, 2),  # 保留两位小数
            "cluster_mem_total_mb": mem_total_gib * 1024,  # Convert GiB to MB
            "cluster_mem_used_mb": mem_used_gib * 1024,  # Convert GiB to MB
            "cluster_mem_total_gib": mem_total_gib,  # 内存总量，单位GiB
            "cluster_mem_used_gib": mem_used_gib,  # 内存使用量，单位GiB
            "cluster_mem_usage_percent": round((mem_used_gib / mem_total_gib * 100) if mem_total_gib > 0 else 0, 2),  # 保留两位小数
            "gpu_total": gpu_total
        }
    except Exception as e:
        logger.error(f"Error getting snapshot for cluster {cluster_name}: {e}")
        # Return default values if there's an error
        snapshot[cluster_name] = {
            "node_count": 0,
            "cluster_cpu_total_cores": 0.0,
            "cluster_cpu_used_cores": 0.0,
            "cluster_cpu_usage_percent": 0.0,
            "cluster_mem_total_mb": 0.0,
            "cluster_mem_used_mb": 0.0,
            "cluster_mem_total_gib": 0.0,  # 内存总量，单位GiB
            "cluster_mem_used_gib": 0.0,  # 内存使用量，单位GiB
            "cluster_mem_usage_percent": 0.0,
            "gpu_total": 0.0
        }

    return snapshot


def get_mac_cluster_snapshot() -> Dict[str, Dict]:
    """
    Returns Mac cluster resource snapshot object from Prometheus metrics.
    """
    cluster_name = "mac"
    snapshot = {}

    try:
        # Determine the querier based on cluster name
        querier = _prometheus_client.get_querier(cluster_name)
        if not querier:
            logger.warning(f"No querier found for cluster {cluster_name}, using default")
            querier = HTTPPrometheusQuerier("http://192.168.5.7:9092/api/v1/query")

        # Use mac-specific queries with ray metrics
        # Node count
        nodes_query = f'count(ray_node_cpu_count{{ray_cluster="{cluster_name}"}})'
        nodes = _prometheus_client.query(nodes_query, cluster_name)
        node_count = int(float(nodes[0]["value"][1])) if nodes else 0

        # CPU cores total
        cpu_query = f'sum(ray_node_cpu_count{{ray_cluster="{cluster_name}"}})'
        cpu = _prometheus_client.query(cpu_query, cluster_name)
        cpu_total = float(cpu[0]["value"][1]) if cpu else 0

        # CPU utilization percentage (value is already in percentage format)
        cpu_util_query = f'avg(ray_node_cpu_utilization{{ray_cluster="{cluster_name}"}})'
        cpu_util = _prometheus_client.query(cpu_util_query, cluster_name)
        cpu_util_percent = round(float(cpu_util[0]["value"][1]), 2) if cpu_util else 0

        # CPU remaining
        cpu_rest = round((1 - cpu_util_percent / 100) * cpu_total, 2)

        # Memory total in GiB
        mem_total_query = f'sum(ray_node_mem_total{{ray_cluster="{cluster_name}"}})'
        mem_total = _prometheus_client.query(mem_total_query, cluster_name)
        mem_total_gib = round(float(mem_total[0]["value"][1]) / (1024**3), 2) if mem_total else 0

        # Memory used in GiB
        mem_used_query = f'sum(ray_node_mem_used{{ray_cluster="{cluster_name}"}})'
        mem_used = _prometheus_client.query(mem_used_query, cluster_name)
        mem_used_gib = round(float(mem_used[0]["value"][1]) / (1024**3), 2) if mem_used else 0

        # GPU total (if metrics exist)
        gpu_query = f'sum(ray_node_gpus{{ray_cluster="{cluster_name}"}})'
        gpu_metric = _prometheus_client.query(gpu_query, cluster_name)
        gpu_total = float(gpu_metric[0]["value"][1]) if gpu_metric else 0

        # 确保CPU使用量不超过总数，避免负值问题
        adjusted_cpu_used = min(round(cpu_total * cpu_util_percent / 100, 2), cpu_total) if cpu_total > 0 else 0
        # 重新计算使用率以確保准确性
        actual_cpu_util_percent = (adjusted_cpu_used / cpu_total * 100) if cpu_total > 0 else 0

        # Memory values in MB (for compatibility)
        mem_total_mb = mem_total_gib * 1024.0
        mem_used_mb = mem_used_gib * 1024.0
        mem_usage_percent = (mem_used_gib / mem_total_gib * 100) if mem_total_gib > 0 else 0

        snapshot[cluster_name] = {
            "node_count": node_count,
            "cluster_cpu_total_cores": cpu_total,
            "cluster_cpu_used_cores": adjusted_cpu_used,
            "cluster_cpu_usage_percent": round(actual_cpu_util_percent, 2),  # 保留两位小数
            "cluster_mem_total_mb": mem_total_mb,
            "cluster_mem_used_mb": mem_used_mb,
            "cluster_mem_total_gib": mem_total_gib,  # 内存总量，单位GiB
            "cluster_mem_used_gib": mem_used_gib,  # 内存使用量，单位GiB
            "cluster_mem_usage_percent": round(mem_usage_percent, 2),  # 保留两位小数
            "gpu_total": gpu_total
        }
    except Exception as e:
        logger.error(f"Error getting snapshot for cluster {cluster_name}: {e}")
        # Return default values if there's an error
        snapshot[cluster_name] = {
            "node_count": 0,
            "cluster_cpu_total_cores": 0.0,
            "cluster_cpu_used_cores": 0.0,
            "cluster_cpu_usage_percent": 0.0,
            "cluster_mem_total_mb": 0.0,
            "cluster_mem_used_mb": 0.0,
            "cluster_mem_total_gib": 0.0,  # 内存总量，单位GiB
            "cluster_mem_used_gib": 0.0,  # 内存使用量，单位GiB
            "cluster_mem_usage_percent": 0.0,
            "gpu_total": 0.0
        }

    return snapshot


def get_ray_cluster_snapshot(cluster_names: List[str]) -> Dict[str, Dict]:
    """
    Returns Ray cluster resource snapshot object from Prometheus metrics.
    Format:
    {
        "mac": {"nodes": 2, "cpu_total": 8, "cpu_util_percent": 75.5,
                   "mem_total_bytes": 16e9, "mem_used_bytes": 8e9, "gpu_total": 1},
    }
    """
    snapshot = {}

    for cluster_name in cluster_names:
        if cluster_name == "centos":
            centos_snapshot = get_centos_cluster_snapshot()
            snapshot.update(centos_snapshot)
        elif cluster_name == "mac":
            mac_snapshot = get_mac_cluster_snapshot()
            snapshot.update(mac_snapshot)
        else:
            logger.warning(f"Unsupported cluster type: {cluster_name}")
            snapshot[cluster_name] = {
                "node_count": 0,
                "cluster_cpu_total_cores": 0.0,
                "cluster_cpu_used_cores": 0.0,
                "cluster_cpu_usage_percent": 0.0,
                "cluster_mem_total_mb": 0.0,
                "cluster_mem_used_mb": 0.0,
                "cluster_mem_total_gib": 0.0,  # 内存总量，单位GiB
                "cluster_mem_used_gib": 0.0,  # 内存使用量，单位GiB
                "cluster_mem_usage_percent": 0.0,
                "gpu_total": 0.0
            }

    return snapshot


def get_cluster_resource_snapshot(cluster_name: str) -> ResourceSnapshot:
    """
    Get cluster resource snapshot in ResourceSnapshot format from Prometheus metrics.
    """
    try:
        # Get cluster snapshot data
        cluster_snapshots = get_ray_cluster_snapshot([cluster_name])
        if cluster_name not in cluster_snapshots:
            logger.warning(f"Cluster {cluster_name} not found in Prometheus metrics")
            return ResourceSnapshot(
                cluster_name=cluster_name,
                cluster_cpu_usage_percent=0.0,
                cluster_mem_usage_percent=0.0,
                cluster_cpu_used_cores=0.0,
                cluster_cpu_total_cores=0.0,
                cluster_mem_used_mb=0.0,
                cluster_mem_total_mb=0.0,
                node_count=0,
                timestamp=time.time(),
                node_stats=None
            )

        cluster_data = cluster_snapshots[cluster_name]

        # Get values directly from standardized keys
        cpu_total_cores = cluster_data.get("cluster_cpu_total_cores", 0.0)
        cpu_used_cores = cluster_data.get("cluster_cpu_used_cores", 0.0)
        cpu_util_percent = cluster_data.get("cluster_cpu_usage_percent", 0.0)

        # Memory values in MB
        mem_total_mb = cluster_data.get("cluster_mem_total_mb", 0.0)
        mem_used_mb = cluster_data.get("cluster_mem_used_mb", 0.0)
        mem_util_percent = cluster_data.get("cluster_mem_usage_percent", 0.0)

        # Memory values in GiB
        mem_total_gib = cluster_data.get("cluster_mem_total_gib", 0.0)
        mem_used_gib = cluster_data.get("cluster_mem_used_gib", 0.0)

        return ResourceSnapshot(
            cluster_name=cluster_name,
            cluster_cpu_usage_percent=cpu_util_percent,
            cluster_mem_usage_percent=mem_util_percent,
            cluster_cpu_used_cores=cpu_used_cores,
            cluster_cpu_total_cores=cpu_total_cores,
            cluster_mem_used_mb=mem_used_mb,
            cluster_mem_total_mb=mem_total_mb,
            cluster_mem_used_gib=mem_used_gib,
            cluster_mem_total_gib=mem_total_gib,
            node_count=cluster_data.get("nodes", 0),
            timestamp=time.time(),
            node_stats=None
        )
    except Exception as e:
        logger.error(f"Error creating ResourceSnapshot for cluster {cluster_name}: {e}")
        # Return default ResourceSnapshot in case of error
        return ResourceSnapshot(
            cluster_name=cluster_name,
            cluster_cpu_usage_percent=0.0,
            cluster_mem_usage_percent=0.0,
            cluster_cpu_used_cores=0.0,
            cluster_cpu_total_cores=0.0,
            cluster_mem_used_mb=0.0,
            cluster_mem_total_mb=0.0,
            node_count=0,
            timestamp=time.time(),
            node_stats=None
        )


def get_cluster_health_status(cluster_name: str) -> bool:
    """
    Check if cluster is available by querying Prometheus.
    """
    try:
        # Query for cluster up status
        if cluster_name == "centos":
            # For centos, use node exporter up status
            up_query = 'up'
        else:
            # For mac, use ray cluster up status
            up_query = f'up{{ray_cluster="{cluster_name}"}}'

        results = _prometheus_client.query(up_query, cluster_name)

        # Check if any result has value "1" (indicating cluster is up)
        for result in results:
            if result.get("value", [None, None])[1] == "1":
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking health status for cluster {cluster_name}: {e}")
        return False


if __name__ == '__main__':
    print(get_cluster_health_status('mac'))
    print(get_cluster_resource_snapshot('mac'))