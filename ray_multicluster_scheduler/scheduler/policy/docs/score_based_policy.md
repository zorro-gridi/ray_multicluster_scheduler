# ScoreBasedPolicy

## 概述

基础评分策略，根据集群资源可用性进行评分和选择。计算每个集群的可用 CPU 和内存资源，选择资源最充足的集群进行任务调度。采用绝对资源值进行评分，而非归一化后的比率。

## 类定义

### ScoreBasedPolicy

#### `__init__(self)`

初始化基础评分策略。

**参数:**
- 无

**返回:**
- 无

#### `evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision`

评估集群基于资源可用性并返回最佳集群。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象，包含任务ID和资源需求 |
| `cluster_snapshots` | `Dict[str, ResourceSnapshot]` | 集群资源快照字典，key为集群名称 |

**返回:**
- `SchedulingDecision`: 调度决策对象，包含选择的集群名称和原因

**评分公式:**

```
score = cpu_available + memory_available_gib
```

其中：
- `cpu_available = cluster_cpu_total_cores - cluster_cpu_used_cores`
- `memory_available_gib = (cluster_mem_total_mb - cluster_mem_used_mb) / 1024.0`

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.policy.score_based_policy import ScoreBasedPolicy
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot

# 创建策略实例
policy = ScoreBasedPolicy()

# 准备集群快照
snapshots = {
    "centos": ResourceSnapshot(
        cluster_cpu_total_cores=16,
        cluster_cpu_used_cores=8,
        cluster_mem_total_mb=32768,
        cluster_mem_used_mb=16384
    ),
    "macos": ResourceSnapshot(
        cluster_cpu_total_cores=10,
        cluster_cpu_used_cores=5,
        cluster_mem_total_mb=16384,
        cluster_mem_used_mb=8192
    )
}

# 创建任务描述
task = TaskDescription(task_id="task_001", resource_requirements={"CPU": 2})

# 评估并选择集群
decision = policy.evaluate(task, snapshots)
print(f"选择集群: {decision.cluster_name}")
print(f"原因: {decision.reason}")
```

## 与 EnhancedScoreBasedPolicy 的区别

| 特性 | ScoreBasedPolicy | EnhancedScoreBasedPolicy |
|-----|-----------------|------------------------|
| 权重支持 | 不支持 | 支持 `cluster.weight` 配置 |
| 偏好集群 | 不支持 | 支持 `cluster.prefer` 加成 |
| 负载均衡因子 | 不支持 | 支持资源利用率因子 |
| GPU资源评分 | 不支持 | 支持GPU资源加权 |
| 内存资源权重 | 1:1 权重 | 0.1 倍权重 |

Last Updated: 2026-02-09
