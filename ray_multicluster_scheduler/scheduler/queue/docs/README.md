# Queue Module Documentation

## 概述

Queue 模块提供多集群调度器的任务队列管理功能，支持 Task 和 Job 两种类型的队列操作。

## 文档列表

| 文档 | 说明 |
|-----|------|
| [task_queue.md](./task_queue.md) | TaskQueue 类的完整接口文档 |

## 模块结构

```
queue/
├── __init__.py
├── task # 核心队列_queue.py      实现
└── docs/
    ├── README.md       # 本索引文件
    └── task_queue.md   # 详细接口文档
```

## 快速入门

```python
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

# 初始化队列
queue = TaskQueue(max_size=100)

# 入队操作
from ray_multicluster_scheduler.common.model import TaskDescription

task = TaskDescription(
    task_id="task_001",
    func_or_class="my_module.my_function",
    args=(1, 2),
    kwargs={},
    resource_requirements={"CPU": 2}
)

# 添加到全局队列
queue.enqueue_global(task)

# 添加到指定集群队列
queue.enqueue_cluster(task, "centos")

# 出队操作
task = queue.dequeue()
```

## 核心功能

- **双队列模型**: 全局队列 + 集群专属队列
- **双重去重**: ID 去重 + 内容去重
- **线程安全**: Lock + Condition 保护
- **运行任务跟踪**: 类级别状态管理

Last Updated: 2026-02-09
