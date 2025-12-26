import ray
import psutil
import time
import os
import logging
from typing import Dict, List, Optional

from ray.experimental.state.api import list_nodes
from ray.util.placement_group import placement_group
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool

# Get logger for this module
logger = logging.getLogger(__name__)


# =========================================================
# Node Monitor Actor
# =========================================================
@ray.remote(num_cpus=0)
class NodeMonitor:
    """
    Node 级资源监控（Ray Dashboard 口径）
    """

    def __init__(self):
        ctx = ray.get_runtime_context()
        self.node_id = ctx.get_node_id()
        self.node_ip = ray.util.get_node_ip_address()

        self._last_ts = time.time()
        self._last_cpu_ns = self._read_cgroup_cpu_ns()

    # -----------------------------
    # Helpers
    # -----------------------------
    def _read_first(self, paths):
        for p in paths:
            if os.path.exists(p):
                return p
        return None

    # -----------------------------
    # CPU
    # -----------------------------
    def _get_cpu_limit(self) -> float:
        try:
            # cgroup v2
            if os.path.exists("/sys/fs/cgroup/cpu.max"):
                quota, period = open("/sys/fs/cgroup/cpu.max").read().split()
                if quota != "max":
                    return int(quota) / int(period)

            # cgroup v1
            quota = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
            period = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
            if os.path.exists(quota) and os.path.exists(period):
                q = int(open(quota).read())
                p = int(open(period).read())
                if q > 0:
                    return q / p
        except Exception:
            pass

        return float(psutil.cpu_count(logical=True))

    def _read_cgroup_cpu_ns(self) -> int:
        try:
            if os.path.exists("/sys/fs/cgroup/cpuacct/cpuacct.usage"):
                return int(open("/sys/fs/cgroup/cpuacct/cpuacct.usage").read())

            if os.path.exists("/sys/fs/cgroup/cpu.stat"):
                for line in open("/sys/fs/cgroup/cpu.stat"):
                    if line.startswith("usage_usec"):
                        return int(line.split()[1]) * 1000
        except Exception:
            pass
        return 0

    # -----------------------------
    # Memory
    # -----------------------------
    def _get_mem_limit_bytes(self) -> int:
        try:
            for p in [
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                "/sys/fs/cgroup/memory.max",
            ]:
                if os.path.exists(p):
                    v = open(p).read().strip()
                    if v.isdigit():
                        v = int(v)
                        if 0 < v < (1 << 60):
                            return v
        except Exception:
            pass
        return psutil.virtual_memory().total

    def _get_mem_used_bytes(self) -> int:
        try:
            for p in [
                "/sys/fs/cgroup/memory/memory.usage_in_bytes",
                "/sys/fs/cgroup/memory.current",
            ]:
                if os.path.exists(p):
                    return int(open(p).read())
        except Exception:
            pass
        return psutil.virtual_memory().used

    # =====================================================
    # Public APIs
    # =====================================================
    def get_node_usage(self) -> Dict:
        now = time.time()
        cur_cpu_ns = self._read_cgroup_cpu_ns()
        dt = now - self._last_ts

        cpu_limit = self._get_cpu_limit()
        cpu_pct = 0.0
        if dt > 0 and cur_cpu_ns > self._last_cpu_ns:
            delta = cur_cpu_ns - self._last_cpu_ns
            cpu_pct = delta / (dt * cpu_limit * 1e9) * 100

        mem_limit = self._get_mem_limit_bytes()
        mem_used = self._get_mem_used_bytes()
        mem_pct = mem_used / mem_limit * 100 if mem_limit > 0 else 0.0

        self._last_ts = now
        self._last_cpu_ns = cur_cpu_ns

        return {
            "node_id": self.node_id,
            "node_ip": self.node_ip,
            "cpu_limit_cores": round(cpu_limit, 2),
            "cpu_used_cores": round(cpu_pct / 100 * cpu_limit, 2),
            "cpu_usage_percent": round(min(cpu_pct, 100.0), 2),
            "mem_limit_mb": round(mem_limit / 1024 / 1024, 2),
            "mem_used_mb": round(mem_used / 1024 / 1024, 2),
            "mem_usage_percent": round(min(mem_pct, 100.0), 2),
        }

    def get_worker_usage(self) -> List[Dict]:
        results = []
        node_cpu = psutil.cpu_count(logical=True)

        for p in psutil.process_iter(attrs=["pid", "cmdline", "memory_info"]):
            try:
                cmd = " ".join(p.info.get("cmdline") or [])
                if "ray::" in cmd:
                    raw_cpu = p.cpu_percent(interval=0.0)
                    results.append({
                        "pid": p.pid,
                        "cpu_usage_percent": round(raw_cpu / node_cpu, 2),
                        "memory_rss_mb": round(
                            p.info["memory_info"].rss / 1024 / 1024, 2
                        ),
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return results


# =========================================================
# Cluster Aggregation
# =========================================================
def is_k8s() -> bool:
    return os.path.exists("/var/run/secrets/kubernetes.io")


def aggregate_cluster_usage(nodes: List[Dict]) -> Dict:
    if is_k8s():
        cpu_used = sum(n["cpu_used_cores"] for n in nodes)
        cpu_total = sum(n["cpu_limit_cores"] for n in nodes)
        mem_used = sum(n["mem_used_mb"] for n in nodes)
        mem_total = sum(n["mem_limit_mb"] for n in nodes)
    else:
        cpu_total = psutil.cpu_count(logical=True)
        cpu_used = psutil.cpu_percent(interval=0.5) / 100 * cpu_total
        mem = psutil.virtual_memory()
        mem_used = mem.used / 1024 / 1024
        mem_total = mem.total / 1024 / 1024

    return {
        "cluster_cpu_usage_percent": round(cpu_used / cpu_total * 100, 2),
        "cluster_mem_usage_percent": round(mem_used / mem_total * 100, 2),
        "cluster_cpu_used_cores": round(cpu_used, 2),
        "cluster_cpu_total_cores": round(cpu_total, 2),
        "cluster_mem_used_mb": round(mem_used, 2),
        "cluster_mem_total_mb": round(mem_total, 2),
    }


def get_cluster_level_stats(cluster_name: str = None, ray_client_pool :RayClientPool = None) -> Dict:
    """
    获取集群级别资源统计信息
    :param cluster_name: 集群名称，如果为None则使用当前连接的集群
    :param ray_client_pool: Ray客户端连接池实例，如果提供则使用连接池中的连接
    :return: 集群级别的统计信息
    """
    success = False
    # 如果提供了连接池实例和集群名称，通过连接管理器建立连接
    if ray_client_pool and cluster_name:
        # 使用连接池确保连接到指定集群
        success = ray_client_pool.ensure_cluster_connection(cluster_name)

    if cluster_name and not success:
        success = RayClientPool.establish_ray_connection(cluster_name)
        if not success:
            logger.error(f"Failed to establish connection to cluster {cluster_name}")
            raise RuntimeError(f"Failed to establish connection to cluster {cluster_name}") from None

    if not ray.is_initialized():
        raise RuntimeError("Ray未初始化，请提供集群名称以建立连接")

    try:
        # NOTE: ray.nodes 接口替换 list_nodes() 接口
        alive = [n for n in ray.nodes() if n.get("Alive", False)]
        logger.debug(f"Found {len(alive)} alive nodes in the cluster")

        if len(alive) == 0:
            logger.warning("No alive nodes found in the cluster")
            # Return default values when no nodes are available
            return {
                "cluster_cpu_usage_percent": 0.0,
                "cluster_mem_usage_percent": 0.0,
                "cluster_cpu_used_cores": 0.0,
                "cluster_cpu_total_cores": 0.0,
                "cluster_mem_used_mb": 0.0,
                "cluster_mem_total_mb": 0.0,
            }

        # 尝试创建placement group，如果失败则返回默认值
        try:
            pg = placement_group([{"CPU": 0.001}] * len(alive), strategy="SPREAD")
            ray.get(pg.ready(), timeout=30)  # 设置30秒超时

            monitors = [
                NodeMonitor.options(
                    placement_group=pg,
                    placement_group_bundle_index=i,
                ).remote()
                for i in range(len(alive))
            ]

            node_stats = ray.get([m.get_node_usage.remote() for m in monitors], timeout=30)  # 设置30秒超时
            logger.debug(f"Node stats collected: {len(node_stats)} nodes")

            cluster_stats = aggregate_cluster_usage(node_stats)
            logger.debug(f"Cluster stats: {cluster_stats}")
        except ray.exceptions.RaySystemError as e:
            logger.warning(f"Ray system error during resource collection: {e}")
            # 如果是Ray系统错误，返回默认值而不是抛出异常
            cluster_stats = {
                "cluster_cpu_usage_percent": 0.0,
                "cluster_mem_usage_percent": 0.0,
                "cluster_cpu_used_cores": 0.0,
                "cluster_cpu_total_cores": 0.0,
                "cluster_mem_used_mb": 0.0,
                "cluster_mem_total_mb": 0.0,
            }
        except Exception as e:
            # 检查是否是连接相关的错误
            if "Channel closed" in str(e) or "connection" in str(e).lower():
                logger.warning(f"Connection Closed due to Main thread finished during resource collection: {e}")
                # 对于连接错误，返回默认值而不是抛出异常
                cluster_stats = {
                    "cluster_cpu_usage_percent": 0.0,
                    "cluster_mem_usage_percent": 0.0,
                    "cluster_cpu_used_cores": 0.0,
                    "cluster_cpu_total_cores": 0.0,
                    "cluster_mem_used_mb": 0.0,
                    "cluster_mem_total_mb": 0.0,
                }
            else:
                logger.error(f"Unexpected error in get_cluster_level_stats: {e}")
                import traceback
                traceback.print_exc()
                # Return default values in case of error
                cluster_stats = {
                    "cluster_cpu_usage_percent": 0.0,
                    "cluster_mem_usage_percent": 0.0,
                    "cluster_cpu_used_cores": 0.0,
                    "cluster_cpu_total_cores": 0.0,
                    "cluster_mem_used_mb": 0.0,
                    "cluster_mem_total_mb": 0.0,
                }

    except Exception as e:
        logger.error(f"Error in get_cluster_level_stats: {e}")
        import traceback
        traceback.print_exc()
        # Return default values in case of error
        cluster_stats = {
            "cluster_cpu_usage_percent": 0.0,
            "cluster_mem_usage_percent": 0.0,
            "cluster_cpu_used_cores": 0.0,
            "cluster_cpu_total_cores": 0.0,
            "cluster_mem_used_mb": 0.0,
            "cluster_mem_total_mb": 0.0,
        }

    return cluster_stats


def get_node_level_stats(cluster_name: str = None, ray_client_pool :RayClientPool = None) -> List[Dict]:
    """
    获取节点级别资源统计信息
    :param cluster_name: 集群名称，如果为None则使用当前连接的集群
    :param ray_client_pool: Ray客户端连接池实例，如果提供则使用连接池中的连接
    :return: 节点级别的统计信息列表
    """
    success = False
    # 如果提供了连接池实例和集群名称，通过连接管理器建立连接
    if ray_client_pool and cluster_name:
        # 使用连接池确保连接到指定集群
        success = ray_client_pool.ensure_cluster_connection(cluster_name)

    if cluster_name and not success:
        success = RayClientPool.establish_ray_connection(cluster_name)
        if not success:
            logger.error(f"Failed to establish connection to cluster {cluster_name}")
            raise RuntimeError(f"Failed to establish connection to cluster {cluster_name}") from None

    if not ray.is_initialized():
        raise RuntimeError("Ray未初始化，请提供集群名称以建立连接")

    result = []
    try:
        # NOTE: ray.nodes 接口替换 list_nodes() 接口
        alive = [n for n in ray.nodes() if n.get("Alive", False)]
        logger.debug(f"Found {len(alive)} alive nodes for node-level stats")

        if len(alive) == 0:
            logger.warning("No alive nodes found for node-level stats")
        else:
            # 尝试创建placement group，如果失败则跳过统计
            try:
                pg = placement_group([{"CPU": 0.001}] * len(alive), strategy="SPREAD")
                ray.get(pg.ready(), timeout=30)  # 设置30秒超时

                monitors = [
                    NodeMonitor.options(
                        placement_group=pg,
                        placement_group_bundle_index=i,
                    ).remote()
                    for i in range(len(alive))
                ]

                node_stats = ray.get([m.get_node_usage.remote() for m in monitors], timeout=30)  # 设置30秒超时
                logger.debug(f"Node stats collected: {len(node_stats)} nodes")

                result = node_stats
            except ray.exceptions.RaySystemError as e:
                logger.warning(f"Ray system error during node-level resource collection: {e}")
                result = []  # 返回空列表而不是抛出异常
            except Exception as e:
                # 检查是否是连接相关的错误
                if "Channel closed" in str(e) or "connection" in str(e).lower():
                    logger.warning(f"Connection error during node-level resource collection: {e}")
                    result = []  # 返回空列表而不是抛出异常
                else:
                    logger.error(f"Unexpected error in get_node_level_stats: {e}")
                    import traceback
                    traceback.print_exc()
                    result = []  # 返回空列表而不是抛出异常
    except Exception as e:
        logger.error(f"Error in get_node_level_stats: {e}")
        import traceback
        traceback.print_exc()

    return result


def get_worker_level_stats(cluster_name: str = None, ray_client_pool :RayClientPool = None) -> List[List[Dict]]:
    """
    获取工作进程级别资源统计信息
    :param cluster_name: 集群名称，如果为None则使用当前连接的集群
    :param ray_client_pool: Ray客户端连接池实例，如果提供则使用连接池中的连接
    :return: 工作进程级别的统计信息列表（按节点分组）
    """
    success = False
    # 如果提供了连接池实例和集群名称，通过连接管理器建立连接
    if ray_client_pool and cluster_name:
        # 使用连接池确保连接到指定集群
        success = ray_client_pool.ensure_cluster_connection(cluster_name)

    if cluster_name and not success:
        success = RayClientPool.establish_ray_connection(cluster_name)
        if not success:
            logger.error(f"Failed to establish connection to cluster {cluster_name}")
            raise RuntimeError(f"Failed to establish connection to cluster {cluster_name}") from None

    if not ray.is_initialized():
        raise RuntimeError("Ray未初始化，请提供集群名称以建立连接")

    result = []
    try:
        alive = [n for n in ray.nodes() if n.get("Alive", False)]
        logger.debug(f"Found {len(alive)} alive nodes for worker-level stats")

        if len(alive) == 0:
            logger.warning("No alive nodes found for worker-level stats")
        else:
            # 尝试创建placement group，如果失败则跳过统计
            try:
                pg = placement_group([{"CPU": 0.001}] * len(alive), strategy="SPREAD")
                ray.get(pg.ready(), timeout=30)  # 设置30秒超时

                monitors = [
                    NodeMonitor.options(
                        placement_group=pg,
                        placement_group_bundle_index=i,
                    ).remote()
                    for i in range(len(alive))
                ]

                worker_stats = ray.get([m.get_worker_usage.remote() for m in monitors], timeout=30)  # 设置30秒超时
                logger.debug(f"Worker stats collected: {len(worker_stats)} nodes")

                result = worker_stats
            except ray.exceptions.RaySystemError as e:
                logger.warning(f"Ray system error during worker-level resource collection: {e}")
                result = []  # 返回空列表而不是抛出异常
            except Exception as e:
                # 检查是否是连接相关的错误
                if "Channel closed" in str(e) or "connection" in str(e).lower():
                    logger.warning(f"Connection error during worker-level resource collection: {e}")
                    result = []  # 返回空列表而不是抛出异常
                else:
                    logger.error(f"Unexpected error in get_worker_level_stats: {e}")
                    import traceback
                    traceback.print_exc()
                    result = []  # 返回空列表而不是抛出异常
    except Exception as e:
        logger.error(f"Error in get_worker_level_stats: {e}")
        import traceback
        traceback.print_exc()

    return result
