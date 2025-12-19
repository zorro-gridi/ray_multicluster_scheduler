"""
Core data models for the ray multicluster scheduler.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import uuid


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
    runtime_env: Optional[Dict[str, Any]] = None  # 新增：运行时环境配置


@dataclass
class ClusterMetadata:
    """Metadata for a Ray cluster."""
    name: str
    head_address: str
    dashboard: str
    prefer: bool = False
    weight: float = 1.0
    home_dir: Optional[str] = None
    conda: Optional[str] = None  # 新增：集群默认使用的conda虚拟环境名
    tags: List[str] = field(default_factory=list)


@dataclass
class ResourceSnapshot:
    """Snapshot of cluster resources."""
    cluster_name: str
    available_resources: Dict[str, float]
    total_resources: Dict[str, float]
    node_count: int
    timestamp: float


@dataclass
class SchedulingDecision:
    """Result of a scheduling decision."""
    task_id: str
    cluster_name: str
    reason: str = ""