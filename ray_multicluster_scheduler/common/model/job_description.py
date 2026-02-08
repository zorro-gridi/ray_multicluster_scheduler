"""
Job description model for the ray multicluster scheduler.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Any, List
import uuid
from . import TaskDescription  # 用于转换为任务描述以复用调度逻辑

@dataclass
class JobDescription:
    """
    Description of a job to be submitted to the scheduler.
    """
    job_id: str
    entrypoint: str
    runtime_env: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    submission_id: Optional[str] = None
    preferred_cluster: Optional[str] = None
    resource_requirements: Optional[Dict[str, float]] = None
    tags: Optional[List[str]] = None
    # 新增字段：调度状态跟踪
    scheduling_status: str = "PENDING"  # PENDING, QUEUED, SUBMITTED, RUNNING, COMPLETED, FAILED
    actual_submission_id: Optional[str] = None  # 实际提交到Ray集群的submission_id
    is_processing: bool = False  # 处理中标志，用于防止重复处理

    def __post_init__(self):
        if not self.job_id:
            self.job_id = f"job_{uuid.uuid4().hex[:20]}"
        if not self.submission_id:
            self.submission_id = f"sub_{uuid.uuid4().hex[:20]}"
        if self.tags is None:
            self.tags = []

    def as_task_description(self) -> 'TaskDescription':
        """将Job描述转换为Task描述以复用调度策略"""
        return TaskDescription(
            task_id=self.job_id,
            func_or_class=None,  # Job不包含函数引用
            args=(),
            kwargs={},
            resource_requirements=self.resource_requirements or {},
            tags=self.tags,
            preferred_cluster=self.preferred_cluster,
            is_actor=False,
            is_top_level_task=True,  # Job作为顶级任务，受40秒限制
            runtime_env=self.runtime_env  # 添加这行来传递runtime_env
        )