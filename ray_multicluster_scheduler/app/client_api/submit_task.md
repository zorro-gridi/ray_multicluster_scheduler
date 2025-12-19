# Submit Task API 文档

## 概述
Submit Task API 为向 Ray 多集群调度器提交任务提供了一个简单的接口。该 API 抽象了集群选择、任务排队和结果收集的复杂性，使开发人员能够专注于应用程序逻辑。

## 模块结构
```
ray_multicluster_scheduler/
└── app/
    └── client_api/
        └── submit_task.py
```

## 核心组件

### 函数

#### `initialize_scheduler(task_lifecycle_manager)`
使用任务生命周期管理器初始化调度器。

**参数:**
- `task_lifecycle_manager` (TaskLifecycleManager): 用于调度任务的任务生命周期管理器实例。

**返回值:**
- 无

**异常:**
- 无

#### `submit_task(func, args=(), kwargs=None, resource_requirements=None, tags=None, name="")`
向调度器提交任务，以便在可用的 Ray 集群上执行。

**参数:**
- `func` (Callable): 要远程执行的函数。
- `args` (tuple, 可选): 函数的位置参数。默认为 ()。
- `kwargs` (dict, 可选): 函数的关键字参数。默认为 None。
- `resource_requirements` (Dict[str, float], 可选): 资源需求字典（例如，{"CPU": 2, "GPU": 1}）。默认为 None。
- `tags` (List[str], 可选): 与任务关联的标签列表。默认为 None。
- `name` (str, 可选): 任务的可选名称。默认为 ""。

**返回值:**
- 无：在当前实现中，函数返回 None。未来版本可能会返回用于检索结果的 future 或 promise。

**异常:**
- `RuntimeError`: 如果调度器未初始化或任务提交失败。

## 使用示例

### 基本任务提交
```python
from ray_multicluster_scheduler.app.client_api.submit_task import initialize_scheduler, submit_task

# 初始化调度器（通常在应用程序中只做一次）
initialize_scheduler(task_lifecycle_manager)

# 定义一个简单的执行函数
@ray.remote
def add_numbers(a, b):
    return a + b

# 提交任务
submit_task(add_numbers, args=(5, 3))
```

### 带资源需求的任务
```python
# 提交带有特定资源需求的任务
submit_task(
    process_data,
    args=(data,),
    resource_requirements={"CPU": 2, "GPU": 1},
    tags=["ml", "training"],
    name="data_processing_task"
)
```

## 错误处理

API 可能会引发以下异常：

1. `RuntimeError`: 在以下情况下引发：
   - 在调用 `submit_task` 之前未初始化调度器
   - 任务未能加入任务队列

## 实现详情

### 全局状态
模块维护一个全局的 `_task_lifecycle_manager` 实例，该实例在所有 API 调用中共享。

### 任务描述创建
当调用 `submit_task` 时，它会创建一个具有以下属性的 `TaskDescription` 对象：
- `name`: 提供的名称或空字符串
- `func_or_class`: 要执行的函数
- `args`: 函数的位置参数
- `kwargs`: 函数的关键字参数
- `resource_requirements`: 任务的资源需求
- `tags`: 与任务关联的标签
- `is_actor`: 对于任务提交始终为 False

### 任务提交流程
1. 验证调度器是否已初始化
2. 从提供的参数创建 `TaskDescription` 对象
3. 将任务提交到任务生命周期管理器的队列
4. 返回 None（未来版本可能会返回用于结果检索的 future）

## 最佳实践

1. **初始化**: 确保在提交任务之前初始化调度器。
2. **错误处理**: 始终将任务提交包装在 try-except 块中以处理潜在错误。
3. **资源规范**: 指定资源需求以确保任务被调度到具有足够资源的集群上。
4. **标记**: 使用标签帮助调度器做出更好的集群放置决策。
5. **命名**: 为任务提供有意义的名称以帮助调试和监控。

## 限制

1. **结果检索**: 当前实现不提供检索任务结果的机制。
2. **异步执行**: 所有任务提交都是异步的，但没有直接的方法等待完成。
3. **错误传播**: 来自任务执行的异常目前不会传播回调用方。

## 未来增强功能

1. **结果 Futures**: 返回可用于检索任务结果的 futures 或 promises。
2. **同步执行**: 添加对阻塞直到任务完成的支持。
3. **增强错误处理**: 改进来自任务执行的错误传播。
4. **批量提交**: 添加对一次提交多个任务的支持。