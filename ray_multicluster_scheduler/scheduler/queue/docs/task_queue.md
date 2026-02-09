# TaskQueue

## 概述

TaskQueue 是多集群调度器的核心队列管理模块，负责管理和调度 Task 和 Job 两种类型的任务。模块实现了以下核心特性：

1. **双队列模型**: 全局队列 + 每集群专属队列，支持任务的智能路由
2. **双重去重机制**: 基于 ID 去重 + 基于内容去重，防止重复提交
3. **线程安全**: 使用 `threading.Lock` 和 `threading.Condition` 保证并发安全
4. **运行任务跟踪**: 类级别状态管理，跟踪所有集群上运行中的任务

## 类定义

### TaskQueue

#### `__init__(self, max_size: int = 100)`

初始化任务队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `max_size` | `int` | 全局队列最大容量，默认为 100 |

#### `enqueue(self, task_desc: TaskDescription, cluster_name: str = None) -> bool`

将任务添加到合适的队列。如果指定了 cluster_name，添加到该集群的队列；否则添加到全局队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `bool`: 成功入队返回 True，队列满时返回 False

**说明:**
- 自动进行 ID 去重和内容去重检查

#### `enqueue_global(self, task_desc: TaskDescription) -> bool`

将任务添加到全局队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |

**返回:**
| 返回值 | 说明 |
|-------|------|
| `True` | 成功入队 |
| `False` | 队列已满 |

#### `enqueue_cluster(self, task_desc: TaskDescription, cluster_name: str) -> bool`

将任务添加到指定集群的队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_desc` | `TaskDescription` | 任务描述对象 |
| `cluster_name` | `str` | 目标集群名称 |

**返回:**
- `bool`: 成功入队返回 True

#### `enqueue_job(self, job_desc: JobDescription, cluster_name: str = None) -> bool`

将作业添加到合适的作业队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `job_desc` | `JobDescription` | 作业描述对象 |
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `bool`: 成功入队返回 True，队列满时返回 False

#### `enqueue_global_job(self, job_desc: JobDescription) -> bool`

将作业添加到全局作业队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `job_desc` | `JobDescription` | 作业描述对象 |

**返回:**
| 返回值 | 说明 |
|-------|------|
| `True` | 成功入队 |
| `False` | 队列已满 |

#### `enqueue_cluster_job(self, job_desc: JobDescription, cluster_name: str) -> bool`

将作业添加到指定集群的作业队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `job_desc` | `JobDescription` | 作业描述对象 |
| `cluster_name` | `str` | 目标集群名称 |

**返回:**
- `bool`: 成功入队返回 True

#### `dequeue(self, cluster_name: str = None) -> Optional[TaskDescription]`

从合适的队列中取出任务。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
| 返回值 | 说明 |
|-------|------|
| `TaskDescription` | 取出的任务对象 |
| `None` | 队列为空 |

**说明:**
- 如果指定了 cluster_name，优先从该集群队列取，没有则从全局队列回退
- 如果未指定，从全局队列取

#### `dequeue_global(self) -> Optional[TaskDescription]`

从全局队列中取出任务。如果队列为空，会阻塞等待最多 1 秒。

**返回:**
| 返回值 | 说明 |
|-------|------|
| `TaskDescription` | 取出的任务对象 |
| `None` | 等待超时，队列仍为空 |

#### `dequeue_from_cluster(self, cluster_name: str) -> Optional[TaskDescription]`

从指定集群的队列中取出任务。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
| 返回值 | 说明 |
|-------|------|
| `TaskDescription` | 取出的任务对象 |
| `None` | 队列为空 |

#### `dequeue_job(self, cluster_name: str = None) -> Optional[JobDescription]`

从合适的作业队列中取出作业。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
| 返回值 | 说明 |
|-------|------|
| `JobDescription` | 取出的作业对象 |
| `None` | 队列为空 |

#### `dequeue_global_job(self) -> Optional[JobDescription]`

从全局作业队列中取出作业。如果队列为空，会阻塞等待最多 1 秒。

**返回:**
| 返回值 | 说明 |
|-------|------|
| `JobDescription` | 取出的作业对象 |
| `None` | 等待超时，队列仍为空 |

#### `dequeue_from_cluster_job(self, cluster_name: str) -> Optional[JobDescription]`

从指定集群的作业队列中取出作业。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
| 返回值 | 说明 |
|-------|------|
| `JobDescription` | 取出的作业对象 |
| `None` | 队列为空 |

#### `size(self, cluster_name: str = None) -> int`

获取任务队列的当前大小。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `int`: 队列中的任务数量

#### `job_size(self, cluster_name: str = None) -> int`

获取作业队列的当前大小。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `int`: 队列中的作业数量

#### `total_size(self) -> int`

获取所有任务队列的总大小（全局 + 所有集群）。

**返回:**
- `int`: 所有队列中的任务总数

#### `total_job_size(self) -> int`

获取所有作业队列的总大小（全局 + 所有集群）。

**返回:**
- `int`: 所有队列中的作业总数

#### `total_combined_size(self) -> int`

获取所有队列的总大小（任务队列 + 作业队列）。

**返回:**
- `int`: 所有队列中任务和作业的总数

#### `is_empty(self, cluster_name: str = None) -> bool`

检查指定的 task 队列是否为空。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `bool`: 队列为空返回 True

#### `is_full(self, cluster_name: str = None) -> bool`

检查指定的 task 队列是否已满。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `bool`: 队列已满返回 True

#### `is_job_empty(self, cluster_name: str = None) -> bool`

检查指定的作业队列是否为空。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称 |

**返回:**
- `bool`: 队列为空返回 True

#### `clear(self, cluster_name: str = None)`

清空指定的 task 队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称，不指定则清空全局队列 |

#### `clear_jobs(self, cluster_name: str = None)`

清空指定的作业队列。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 可选的集群名称，不指定则清空全局作业队列 |

#### `get_cluster_queue_names(self) -> list`

获取所有有任务的集群队列名称列表。

**返回:**
- `list`: 有任务的集群名称列表

#### `get_cluster_job_queue_names(self) -> list`

获取所有有作业的集群作业队列名称列表。

**返回:**
- `list`: 有作业的集群名称列表

#### `register_running_task(cls, task_id: str, cluster_name: str, task_type: str = "task") -> None`

注册一个运行中的任务/作业（类方法）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_id` | `str` | 任务或作业 ID |
| `cluster_name` | `str` | 运行任务的集群名称 |
| `task_type` | `str` | 类型标识，"task" 或 "job" |

**说明:**
- 类级别方法，操作全局 `_running_tasks` 字典

#### `unregister_running_task(cls, task_id: str) -> bool`

注销一个运行中的任务/作业（类方法）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `task_id` | `str` | 任务或作业 ID |

**返回:**
- `bool`: 成功注销返回 True，不存在则返回 False

#### `get_cluster_running_task_count(cls, cluster_name: str) -> int`

获取指定集群上运行中的任务/作业数量（类方法）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
- `int`: 运行中的任务/作业数量

#### `is_cluster_busy(cls, cluster_name: str) -> bool`

检查集群是否有运行中的任务/作业（类方法）。

**参数:**
| 参数名 | 类型 | 说明 |
|-------|------|------|
| `cluster_name` | `str` | 集群名称 |

**返回:**
- `bool`: 有运行任务返回 True

#### `get_all_running_tasks(cls) -> Dict[str, Dict]`

获取所有运行中的任务（类方法，用于调试/监控）。

**返回:**
| 返回值 | 说明 |
|-------|------|
| `Dict[str, Dict]` | 任务ID -> 任务信息的字典副本 |

## 属性

| 属性名 | 类型 | 说明 |
|-------|------|------|
| `max_size` | `int` | 全局队列最大容量 |
| `global_queue` | `deque` | 全局任务队列 |
| `cluster_queues` | `defaultdict` | 集群专属任务队列 |
| `global_job_queue` | `deque` | 全局作业队列 |
| `cluster_job_queues` | `defaultdict` | 集群专属作业队列 |
| `lock` | `threading.Lock` | 互斥锁，保护队列操作 |
| `condition` | `threading.Condition` | 条件变量，支持等待/通知 |
| `global_task_ids` | `set` | 全局队列中的任务 ID 集合 |
| `cluster_task_ids` | `defaultdict` | 各集群队列中的任务 ID 集合 |
| `global_job_ids` | `set` | 全局作业队列中的作业 ID 集合 |
| `cluster_job_ids` | `defaultdict` | 各集群作业队列中的作业 ID 集合 |
| `_running_tasks` | `Dict` | 类属性，运行中任务跟踪字典 |
| `_running_lock` | `threading.Lock` | 类属性，保护运行任务字典 |

## 使用示例

```python
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.model.job_description import JobDescription

# 初始化队列
queue = TaskQueue(max_size=100)

# 创建任务描述
task1 = TaskDescription(
    task_id="task_001",
    func_or_class="my_module.my_function",
    args=(1, 2),
    kwargs={"key": "value"},
    resource_requirements={"CPU": 2, "GPU": 0},
    tags=["linux"],
    preferred_cluster="centos"
)

task2 = TaskDescription(
    task_id="task_002",
    func_or_class="my_module.another_function",
    args=(),
    kwargs={},
    resource_requirements={"CPU": 1},
    tags=["linux", "x86_64"]
)

# 方式1: 添加到全局队列
queue.enqueue_global(task1)

# 方式2: 添加到指定集群队列
queue.enqueue_cluster(task2, "centos")

# 方式3: 智能入队（自动选择队列）
queue.enqueue(task2)  # 无集群名称 -> 全局队列
queue.enqueue(task1, cluster_name="centos")  # 有集群名称 -> 集群队列

# 创建作业描述
job = JobDescription(
    job_id="job_001",
    entrypoint="python script.py",
    runtime_env={"conda": "my_env"},
    metadata={"owner": "user1"},
    preferred_cluster=None
)

# 添加作业到队列
queue.enqueue_job(job)  # 全局作业队列
queue.enqueue_cluster_job(job, "centos")  # 集群作业队列

# 获取队列大小
print(f"全局任务队列: {queue.size()}")
print(f"centos 集群任务队列: {queue.size('centos')}")
print(f"全局作业队列: {queue.job_size()}")
print(f"所有队列总大小: {queue.total_combined_size()}")

# 获取有任务的集群列表
cluster_names = queue.get_cluster_queue_names()
print(f"有任务的集群: {cluster_names}")

# 出队任务
task = queue.dequeue()  # 从全局队列取
task = queue.dequeue("centos")  # 优先从 centos 取，没有则从全局回退

# 从指定集群出队
task_from_cluster = queue.dequeue_from_cluster("centos")

# 检查队列状态
print(f"全局队列是否为空: {queue.is_empty()}")
print(f"全局队列是否已满: {queue.is_full()}")

# 运行任务跟踪
TaskQueue.register_running_task("task_001", "centos", "task")
count = TaskQueue.get_cluster_running_task_count("centos")
print(f"centos 运行任务数: {count}")
print(f"centos 是否繁忙: {TaskQueue.is_cluster_busy('centos')}")
print(f"所有运行任务: {TaskQueue.get_all_running_tasks()}")

# 任务完成后注销
TaskQueue.unregister_running_task("task_001")

# 清空队列
queue.clear("centos")  # 清空指定集群队列
queue.clear()  # 清空全局队列
queue.clear_jobs("centos")  # 清空指定集群作业队列
queue.clear_jobs()  # 清空全局作业队列
```

## 队列架构图

```
                    TaskQueue
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
  全局队列        集群队列 A      集群队列 B
  (deque)        (deque)        (deque)
        │               │               │
        │               │               │
        ▼               ▼               ▼
  全局作业队列    集群作业队列 A  集群作业队列 B
  (deque)        (deque)        (deque)
```

## 去重机制

### ID 去重
- 基于 `task_id` / `job_id` 进行快速去重
- 使用 `set` 数据结构实现 O(1) 时间复杂度

### 内容去重
- 基于函数/类名、参数、资源需求、标签、首选集群进行深度比较
- 遍历队列检查，O(n) 时间复杂度

```python
def _tasks_have_same_content(self, task1: TaskDescription, task2: TaskDescription) -> bool:
    """比较两个任务的内容是否相同"""
    return (task1.func_or_class == task2.func_or_class and
            task1.args == task2.args and
            task1.kwargs == task2.kwargs and
            task1.resource_requirements == task2.resource_requirements and
            task1.tags == task2.tags and
            task1.preferred_cluster == task2.preferred_cluster)
```

## 线程安全机制

```python
# 使用条件变量实现线程安全
self.lock = threading.Lock()
self.condition = threading.Condition(self.lock)

# 入队时通知等待的线程
with self.condition:
    self.global_queue.append(task_desc)
    self.condition.notify()  # 通知等待的消费者

# 出队时等待新任务
with self.condition:
    while len(self.global_queue) == 0:
        if not self.condition.wait(timeout=1.0):
            return None
```

Last Updated: 2026-02-09
