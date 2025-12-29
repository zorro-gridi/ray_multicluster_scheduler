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
            runtime_env=self.runtime_env  # 添加这行来传递runtime_env
        )