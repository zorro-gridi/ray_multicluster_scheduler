# ClusterSubmissionHistory

## 概述

集群任务提交历史记录管理器，用于实现"40秒规则"：同一集群在40秒内只能接收一个顶级任务，防止快速提交导致的资源竞争。该模块采用线程安全的 `RLock` 实现，支持串行执行模式下的任务队列繁忙检测。

## 类定义

### ClusterSubmissionHistory

#### `__init__(self)`

初始化集群提交历史记录管理器。

**参数:**
- 无

#### `record_submission(self, cluster_name: str) -> None`

记录集群任务提交时间。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
- `None`

#### `get_last_submission_time(self, cluster_name: str) -> Optional[float]`

获取集群最近一次任务提交的时间。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
- `Optional[float]`: 时间戳，如果集群从未提交过任务则返回 `None`

#### `is_cluster_available(self, cluster_name: str) -> bool`

检查集群是否可用（上次任务提交时间是否超过等待时间）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
- `bool`: `True` 表示可用，`False` 表示不可用

#### `is_cluster_available_and_record(self, cluster_name: str, is_top_level_task: bool = True) -> bool`

原子操作：检查集群是否可用，如果可用则立即记录提交时间。

此方法解决了竞态条件问题：
1. 检查和记录在同一个锁内完成
2. 防止多个任务同时通过检查
3. 确保40秒规则的严格执行

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |
| `is_top_level_task` | `bool` | 是否为顶级任务，顶级任务受40秒限制，子任务不受限制 |

**返回:**
- `bool`: `True` 表示集群可用且已记录提交时间，`False` 表示集群不可用

**异常:**
- 在串行模式下，如果集群繁忙（有任务正在执行）则返回 `False`

#### `get_available_clusters(self, cluster_names: list) -> list`

获取可用集群列表（排除40秒内已提交任务的集群）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_names` | `list` | 待检查的集群名称列表 |

**返回:**
- `list`: 可用集群名称列表

#### `get_remaining_wait_time(self, cluster_name: str) -> float`

获取集群还需要等待的时间（如果小于0表示可以提交）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
- `float`: 还需要等待的秒数，小于0表示可以立即提交

## 属性

| 属性名 | 类型 | 说明 |
|-------|------|------|
| `_last_submission_times` | `Dict[str, float]` | 存储每个集群最近一次任务提交的时间戳 |
| `SUBMISSION_WAIT_TIME` | `float` | 默认等待时间，默认值为 40.0 秒 |
| `_lock` | `threading.RLock` | 递归锁，保护并发访问 |

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import ClusterSubmissionHistory

# 初始化
history = ClusterSubmissionHistory()

# 检查集群是否可用并记录
is_available = history.is_cluster_available_and_record("centos", is_top_level_task=True)
if is_available:
    print("集群可用，任务已记录")
else:
    remaining = history.get_remaining_wait_time("centos")
    print(f"集群不可用，还需等待 {remaining:.2f} 秒")

# 获取可用集群列表
available = history.get_available_clusters(["centos", "macos"])
print(f"可用集群: {available}")
```

## 注意事项

- 子任务不受40秒限制，使用特殊键 `{cluster_name}_subtask` 记录
- 在串行执行模式下，会额外检查 `TaskQueue.is_cluster_busy()` 判断集群是否繁忙
- 所有方法都是线程安全的，使用 `RLock` 保护

Last Updated: 2026-02-09
