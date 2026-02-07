"""
Core data models for the ray multicluster scheduler.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import uuid
from datetime import datetime


@dataclass
class TaskDescription:
    """Description of a task to be scheduled."""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    func_or_class: Any = None
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    resource_requirements: Dict[str, float] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    is_actor: bool = False
    preferred_cluster: Optional[str] = None  # 新增：指定首选集群名称
    is_top_level_task: bool = True  # 是否为顶级任务，顶级任务受40秒限制，子任务不受限制
    runtime_env: Optional[Dict[str, Any]] = None  # 新增：运行时环境配置
    is_processing: bool = False  # 新增：标记任务是否正在处理，防止并发重复执行


@dataclass
class ClusterMetadata:
    """Metadata for a Ray cluster."""
    name: str
    head_address: str
    dashboard: str
    prefer: bool = False
    weight: float = 1.0
    runtime_env: Optional[Dict[str, Any]] = None  # 新增：运行时环境配置，包含conda和home_dir等信息
    tags: List[str] = field(default_factory=list)


@dataclass
class ResourceSnapshot:
    """Snapshot of cluster resources."""
    cluster_name: str
    # 集群级别资源统计
    cluster_cpu_usage_percent: float = 0.0  # CPU使用率百分比，保留两位小数
    cluster_mem_usage_percent: float = 0.0  # 内存使用率百分比，保留两位小数
    cluster_cpu_used_cores: float = 0.0
    cluster_cpu_total_cores: float = 0.0
    cluster_mem_used_mb: float = 0.0  # 内存使用量，单位MB
    cluster_mem_total_mb: float = 0.0  # 内存总量，单位MB
    cluster_mem_used_gib: float = 0.0  # 内存使用量，单位GiB
    cluster_mem_total_gib: float = 0.0  # 内存总量，单位GiB
    # 节点数量
    node_count: int = 0
    # 时间戳
    timestamp: float = 0.0
    # 节点级别资源统计 (按节点分组的详细信息)
    node_stats: Optional[List[Dict]] = None  # 包含每个节点的详细统计信息，如node_id, node_ip, cpu_limit_cores等


@dataclass
class SchedulingDecision:
    """Result of a scheduling decision."""
    task_id: str
    cluster_name: str
    reason: str = ""


@dataclass
class ClusterHealth:
    """Health status information for a cluster."""
    score: float = 0.0
    resources: Dict[str, Any] = field(default_factory=dict)
    available: bool = True
    last_checked: datetime = field(default_factory=datetime.now)
    error_message: str = ""

    def update(self, score: float, resources: Dict[str, Any], available: bool, error_message: str = ""):
        """Update cluster health status."""
        self.score = score
        self.resources = resources
        self.available = available
        self.last_checked = datetime.now()
        self.error_message = error_message