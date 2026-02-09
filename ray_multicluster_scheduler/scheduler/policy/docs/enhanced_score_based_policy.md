# EnhancedScoreBasedPolicy

## 概述

增强版评分策略，基于集群的真实资源评分机制实现负载均衡。相比基础评分策略，增加了权重支持、偏好集群加成、负载均衡因子和GPU资源加权等高级特性。

## 类定义

### EnhancedScoreBasedPolicy

#### `__init__(self)`

初始化增强版评分策略。

**参数:**
- 无

**返回:**
- 无

#### `evaluate(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot], cluster_metadata: Dict[str, ClusterMetadata] = None) -> SchedulingDecision`

使用增强评分机制评估集群并选择最优集群。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象，包含任务ID和资源需求 |
| `cluster_snapshots` | `Dict[str, ResourceSnapshot]` | 集群资源快照字典 |
| `cluster_metadata` | `Dict[str, ClusterMetadata]` | 集群元数据字典，包含权重和偏好配置 |

**返回:**
- `SchedulingDecision`: 调度决策对象

**评分公式:**

```
base_score = cpu_free * weight
memory_bonus = memory_available_gib * 0.1 * weight
gpu_bonus = gpu_free * 5
preference_bonus = 1.2 if prefer else 1.0
cpu_utilization = (cpu_total - cpu_free) / cpu_total
load_balance_factor = 1.0 - cpu_utilization

score = (base_score + memory_bonus + gpu_bonus) * preference_bonus * load_balance_factor
```

其中：
- `cpu_free = cpu_total - cpu_used`
- `memory_available_gib = mem_free / 1024.0`

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.policy.enhanced_score_based_policy import EnhancedScoreBasedPolicy
from ray_multicluster_scheduler.common.model import TaskDescription, ResourceSnapshot, ClusterMetadata

# 创建策略实例
policy = EnhancedScoreBasedPolicy()

# 准备集群元数据（包含权重和偏好配置）
cluster_metadata = {
    "centos": ClusterMetadata(
        name="centos",
        weight=1.0,
        prefer=False,
        tags=["linux", "x86_64"]
    ),
    "macos": ClusterMetadata(
        name="macos",
        weight=1.5,  # 更高权重
        prefer=True,  # 偏好集群
        tags=["macos", "arm64"]
    )
}

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
        cluster_cpu_used_cores=2,
        cluster_mem_total_mb=16384,
        cluster_mem_used_mb=4096
    )
}

# 创建任务描述
task = TaskDescription(task_id="task_002")

# 评估并选择集群
decision = policy.evaluate(task, snapshots, cluster_metadata)
print(f"选择集群: {decision.cluster_name}")
print(f"原因: {decision.reason}")
```

## 评分因子说明

| 因子 | 计算方式 | 权重影响 |
|-----|---------|---------|
| 基础评分 | `cpu_free * weight` | 直接乘以集群权重 |
| 内存加成 | `memory_gib * 0.1 * weight` | 权重影响较小 |
| GPU加成 | `gpu_free * 5` | 固定高权重，GPU更宝贵 |
| 偏好加成 | `1.2 if prefer else 1.0` | 偏好集群获得 1.2x 加成 |
| 负载均衡因子 | `1.0 - cpu_utilization` | 利用率越低，因子越高 |

## 过滤规则

评分小于0的集群会被过滤掉，表示该集群没有可用CPU资源。

Last Updated: 2026-02-09
