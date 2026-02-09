# PolicyEngine

## 概述

策略引擎是多集群调度器的核心调度决策组件，负责整合和协调多个调度策略，根据任务描述和集群状态做出最优的调度决策。引擎实现了多级调度规则：

1. **首选集群规则**: 优先使用用户指定的集群
2. **资源阈值规则**: 检查集群资源使用率是否超过70%阈值
3. **40秒规则**: 防止同一集群在40秒内接收多个顶级任务
4. **标签亲和规则**: 匹配任务标签与集群标签
5. **负载均衡规则**: 基于评分策略选择资源最充足的集群
6. **轮询回退规则**: 当所有策略都无法决策时，使用轮询选择

## 类定义

### PolicyEngine

#### `__init__(self, cluster_monitor: ClusterMonitor)`

初始化策略引擎。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_monitor` | `ClusterMonitor` | 集群监控器，用于获取集群状态和资源信息 |

**异常:**
- `ValueError`: 当 `cluster_monitor` 为 `None` 时抛出

#### `update_cluster_metadata(self, cluster_metadata: Dict[str, ClusterMetadata]) -> None`

更新集群元数据供策略使用。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_metadata` | `Dict[str, ClusterMetadata]` | 集群元数据字典 |

**返回:**
- `None`

#### `add_policy(self, policy) -> None`

添加自定义策略到引擎。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `policy` | 策略对象 | 实现 `evaluate` 方法的策略实例 |

**返回:**
- `None`

#### `remove_policy(self, policy) -> None`

从引擎中移除策略。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `policy` | 策略对象 | 要移除的策略实例 |

**返回:**
- `None`

#### `schedule(self, task_desc: TaskDescription) -> SchedulingDecision`

任务调度入口，评估所有策略并做出调度决策。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |

**返回:**
- `SchedulingDecision`: 调度决策对象，包含目标集群名称和原因

#### `schedule_job(self, job_desc: JobDescription) -> SchedulingDecision`

作业调度入口，与任务调度使用相同策略。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `job_desc` | `JobDescription` | 作业描述对象 |

**返回:**
- `SchedulingDecision`: 调度决策对象

**说明:**
- 将作业转换为任务描述后复用任务调度逻辑

#### `_make_scheduling_decision(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision`

内部调度决策方法，处理所有调度规则。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |
| `cluster_snapshots` | `Dict[str, ResourceSnapshot]` | 集群资源快照字典 |

**返回:**
- `SchedulingDecision`: 调度决策对象

#### `_combine_decisions(self, task_desc: TaskDescription, policy_decisions: List[SchedulingDecision]) -> SchedulingDecision`

合并多个策略的决策结果。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |
| `policy_decisions` | `List[SchedulingDecision]` | 各策略的决策列表 |

**返回:**
- `SchedulingDecision`: 合并后的决策对象

**合并规则:**
1. 优先采用标签亲和策略的决策
2. 其次采用评分策略的决策
3. 如果都没有特定决策，返回空的集群选择

## 属性

| 属性名 | 类型 | 说明 |
|-------|------|------|
| `cluster_monitor` | `ClusterMonitor` | 集群监控器 |
| `policies` | `list` | 注册的策略列表 |
| `score_policy` | `EnhancedScoreBasedPolicy` | 增强评分策略实例 |
| `tag_policy` | `TagAffinityPolicy` | 标签亲和策略实例 |
| `cluster_submission_history` | `ClusterSubmissionHistory` | 集群提交历史管理器 |
| `_cluster_metadata` | `Dict[str, ClusterMetadata]` | 集群元数据缓存 |
| `_round_robin_counter` | `int` | 轮询计数器，用于回退选择 |
| `RESOURCE_THRESHOLD` | `float` | 资源使用率阈值，默认为 0.7 (70%) |

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.common.model import TaskDescription

# 初始化集群监控器
cluster_monitor = ClusterMonitor()

# 初始化策略引擎
engine = PolicyEngine(cluster_monitor)

# 方式1: 直接调度任务
task = TaskDescription(
    task_id="task_001",
    preferred_cluster="centos",  # 可选，指定首选集群
    resource_requirements={"CPU": 2},
    tags=["linux"]  # 可选，指定标签
)
decision = engine.schedule(task)
print(f"调度决策: {decision.cluster_name}")
print(f"原因: {decision.reason}")

# 方式2: 自定义策略
from ray_multicluster_scheduler.scheduler.policy.weighted_preference_policy import WeightedPreferencePolicy

# 获取最新集群元数据并更新
cluster_metadata = cluster_monitor.get_all_cluster_metadata()
engine.update_cluster_metadata(cluster_metadata)

# 添加自定义策略
custom_policy = WeightedPreferencePolicy(cluster_metadata)
engine.add_policy(custom_policy)

# 移除策略
engine.remove_policy(custom_policy)
```

## 调度决策流程图

```
                    开始调度
                        |
                        v
         ┌── 是否有首选集群? ──┐
         │                   │
        Yes                  No
         │                   │
         v                   v
    首选集群是否健康?      所有集群是否都超过阈值?
         │                   │
        Yes                  Yes
         │                   │
         v                   v
    40秒规则检查        进入队列等待
         │
    通过 / 不通过
         │
         v
    资源阈值检查
         │
    通过 / 不通过
         |
         v
    返回首选集群
                        |
                        v
              ┌─ 收集所有策略决策 ─┐
              │                   │
              v                   v
         标签亲和策略        评分策略
              │                   │
              v                   v
         返回匹配集群        返回最佳集群
              │                   │
              └───────┬───────────┘
                      |
                      v
              合并策略决策
                      |
                      v
         ┌─ 是否有决策? ──┐
         │               │
        Yes              No
         │               │
         v               v
    40秒规则检查      轮询回退选择
         │
         v
    返回决策或进入队列
```

## 关键调度规则

### 资源阈值
- CPU、GPU、内存默认 70% 阈值
- 当所有集群超过阈值时，任务进入队列等待

### 40秒规则
- 同一集群 40 秒内只能接收一个顶级任务
- 子任务不受此限制
- 串行执行模式下会额外检查任务队列繁忙状态

### 策略优先级
1. 首选集群（如指定且可用）
2. 标签亲和匹配（架构特定）
3. 基于资源可用性的负载均衡
4. 轮询回退

Last Updated: 2026-02-09
