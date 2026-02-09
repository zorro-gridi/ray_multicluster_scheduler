# TagAffinityPolicy

## 概述

标签亲和策略，根据任务标签与集群标签的匹配关系进行调度决策。当任务指定了标签时，策略会寻找包含相同标签的集群，并将任务调度到匹配的集群。这对于需要特定架构或环境的任务非常有用。

## 类定义

### TagAffinityPolicy

#### `__init__(self, cluster_metadata: Dict[str, ClusterMetadata])`

初始化标签亲和策略。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_metadata` | `Dict[str, ClusterMetadata]` | 集群元数据字典，key为集群名称，包含集群标签信息 |

**返回:**
- 无

#### `evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision`

根据标签亲和性评估集群并返回最佳匹配集群。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象，包含任务标签 |
| `cluster_snapshots` | `Dict[str, ResourceSnapshot]` | 集群资源快照字典 |

**返回:**
- `SchedulingDecision`: 调度决策对象

**匹配规则:**
- 检查任务标签是否与任一集群标签匹配
- 选择第一个匹配的集群作为目标集群
- 如果没有匹配的集群，返回空的集群选择

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.policy.tag_affinity_policy import TagAffinityPolicy
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata

# 创建策略实例，需要传入集群元数据
cluster_metadata = {
    "centos": ClusterMetadata(
        name="centos",
        tags=["linux", "x86_64", "cpu"]
    ),
    "macos": ClusterMetadata(
        name="macos",
        tags=["macos", "arm64", "gpu"]
    )
}
policy = TagAffinityPolicy(cluster_metadata)

# 场景1: 任务有匹配标签
task_with_tags = TaskDescription(
    task_id="task_001",
    tags=["linux", "x86_64"]
)
decision = policy.evaluate(task_with_tags, {})
print(f"匹配结果: {decision.cluster_name}")  # 输出: centos

# 场景2: 任务无标签
task_without_tags = TaskDescription(task_id="task_002")
decision = policy.evaluate(task_without_tags, {})
print(f"匹配结果: {decision.cluster_name}")  # 输出: "" (空字符串)
print(f"原因: {decision.reason}")  # 输出: "Task has no tags for affinity matching"

# 场景3: 任务标签无匹配
task_no_match = TaskDescription(
    task_id="task_003",
    tags=["windows"]
)
decision = policy.evaluate(task_no_match, {})
print(f"匹配结果: {decision.cluster_name}")  # 输出: "" (空字符串)
print(f"原因: {decision.reason}")  # 输出: "No clusters found matching task tags ['windows']"
```

## 典型使用场景

1. **架构特定任务**: 指定 `["x86_64"]` 标签调度到 Linux 集群，指定 `["arm64"]` 标签调度到 macOS 集群
2. **GPU任务**: 指定 `["gpu"]` 标签调度到配备 GPU 的集群
3. **环境隔离**: 指定 `["cpu"]` 或 `["memory"]` 标签调度到对应优化集群

## 优先级说明

在 `PolicyEngine` 中，标签亲和策略的优先级高于评分策略：
- 如果标签亲和策略返回了特定集群，该决策会被优先采用
- 如果没有标签匹配，则回退到评分策略

Last Updated: 2026-02-09
