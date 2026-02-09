# WeightedPreferencePolicy

## 概述

加权偏好策略，综合考虑集群权重配置和资源可用性进行调度决策。每个集群可以通过配置文件设置权重值，调度时将资源可用性与权重相乘作为最终评分。这允许管理员灵活控制任务在不同集群间的分布比例。

## 类定义

### WeightedPreferencePolicy

#### `__init__(self, cluster_metadata: Dict[str, ClusterMetadata])`

初始化加权偏好策略。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_metadata` | `Dict[str, ClusterMetadata]` | 集群元数据字典，key为集群名称，包含权重配置 |

**返回:**
- 无

#### `evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision`

根据权重和资源可用性评估集群。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |
| `cluster_snapshots` | `Dict[str, ResourceSnapshot]` | 集群资源快照字典 |

**返回:**
- `SchedulingDecision`: 调度决策对象

**评分公式:**

```
resource_score = cpu_available + memory_available_gib
weighted_score = resource_score * cluster_weight
```

其中：
- `cpu_available = cpu_total - cpu_used`
- `memory_available_gib = (mem_total - mem_used) / 1024.0`

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.policy.weighted_preference_policy import WeightedPreferencePolicy
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata

# 创建策略实例
cluster_metadata = {
    "centos": ClusterMetadata(
        name="centos",
        weight=2.0,  # 更高权重
        prefer=False,
        tags=["linux"]
    ),
    "macos": ClusterMetadata(
        name="macos",
        weight=1.0,  # 默认权重
        prefer=False,
        tags=["macos"]
    )
}
policy = WeightedPreferencePolicy(cluster_metadata)

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

# 评估并选择集群
task = TaskDescription(task_id="task_001")
decision = policy.evaluate(task, snapshots)
print(f"选择集群: {decision.cluster_name}")
print(f"原因: {decision.reason}")
```

## 与 EnhancedScoreBasedPolicy 的对比

| 特性 | WeightedPreferencePolicy | EnhancedScoreBasedPolicy |
|-----|-------------------------|------------------------|
| 权重应用方式 | 线性乘法 | 多因子综合计算 |
| 负载均衡因子 | 不支持 | 支持 |
| 偏好加成 | 不支持 | 1.2x 偏好加成 |
| GPU资源 | 不支持 | 支持GPU加权 |
| 复杂度 | 简单 | 复杂 |

## 配置示例 (clusters.yaml)

```yaml
clusters:
  - name: centos
    head_address: 192.168.5.7:32546
    weight: 2.0      # 权重设置为2.0
    prefer: false
    tags: [linux, x86_64]

  - name: macos
    head_address: 192.168.5.8:32546
    weight: 1.0      # 默认权重
    prefer: false
    tags: [macos, arm64]
```

Last Updated: 2026-02-09
