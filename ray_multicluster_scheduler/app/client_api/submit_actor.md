# Submit Actor API 文档

## 概述
Submit Actor API 为向 Ray 多集群调度器提交 Actor 提供了一个简单的接口。该 API 抽象了集群选择、Actor 排队和实例管理的复杂性，使开发人员能够专注于应用程序逻辑。

## 模块结构
```
ray_multicluster_scheduler/
└── app/
    └── client_api/
        └── submit_actor.py
```

## 核心组件

### 函数

#### `initialize_scheduler(task_lifecycle_manager)`
使用任务生命周期管理器初始化调度器。

**参数:**
- `task_lifecycle_manager` (TaskLifecycleManager): 用于调度 Actor 的任务生命周期管理器实例。

**返回值:**
- 无

**异常:**
- 无

#### `submit_actor(actor_class, args=(), kwargs=None, resource_requirements=None, tags=None, name="")`
向调度器提交 Actor，以便在可用的 Ray 集群上实例化。

**参数:**
- `actor_class` (Type): 要远程实例化的 Actor 类。
- `args` (tuple, 可选): Actor 构造函数的位置参数。默认为 ()。
- `kwargs` (dict, 可选): Actor 构造函数的关键字参数。默认为 None。
- `resource_requirements` (Dict[str, float], 可选): 资源需求字典（例如，{"CPU": 2, "GPU": 1}）。默认为 None。
- `tags` (List[str], 可选): 与 Actor 关联的标签列表。默认为 None。
- `name` (str, 可选): Actor 的可选名称。默认为 ""。

**返回值:**
- 无：在当前实现中，函数返回 None。未来版本可能会返回用于检索 Actor 实例的 future 或 promise。

**异常:**
- `RuntimeError`: 如果调度器未初始化或 Actor 提交失败。

## 使用示例

### 基本 Actor 提交
```python
from ray_multicluster_scheduler.app.client_api.submit_actor import initialize_scheduler, submit_actor

# 初始化调度器（通常在应用程序中只做一次）
initialize_scheduler(task_lifecycle_manager)

# 定义一个简单的 Actor 类
@ray.remote
class CounterActor:
    def __init__(self, initial_value=0):
        self.value = initial_value

    def increment(self, amount=1):
        self.value += amount
        return self.value

# 提交 Actor
submit_actor(CounterActor, args=(10,))
```

### 带资源需求的 Actor
```python
# 提交带有特定资源需求的 Actor
submit_actor(
    DataProcessorActor,
    args=(config,),
    resource_requirements={"CPU": 4, "memory": 8*1024*1024*1024},  # 8GB
    tags=["stateful", "processor"],
    name="data_processor_actor"
)
```

## 错误处理

API 可能会引发以下异常：

1. `RuntimeError`: 在以下情况下引发：
   - 在调用 `submit_actor` 之前未初始化调度器
   - Actor 未能加入任务队列

## 实现详情

### 全局状态
模块维护一个全局的 `_task_lifecycle_manager` 实例，该实例在所有 API 调用中共享。

### Actor 描述创建
当调用 `submit_actor` 时，它会创建一个具有以下属性的 `TaskDescription` 对象：
- `name`: 提供的名称或空字符串
- `func_or_class`: 要实例化的 Actor 类
- `args`: Actor 构造函数的位置参数
- `kwargs`: Actor 构造函数的关键字参数
- `resource_requirements`: Actor 的资源需求
- `tags`: 与 Actor 关联的标签
- `is_actor`: 对于 Actor 提交始终为 True

### Actor 提交流程
1. 验证调度器是否已初始化
2. 从提供的参数创建 `TaskDescription` 对象
3. 将 Actor 提交到任务生命周期管理器的队列
4. 返回 None（未来版本可能会返回用于实例检索的 future）

## 最佳实践

1. **初始化**: 确保在提交 Actor 之前初始化调度器。
2. **错误处理**: 始终将 Actor 提交包装在 try-except 块中以处理潜在错误。
3. **资源规范**: 指定资源需求以确保 Actor 被调度到具有足够资源的集群上。
4. **标记**: 使用标签帮助调度器做出更好的集群放置决策。
5. **命名**: 为 Actor 提供有意义的名称以帮助调试和监控。

## 限制

1. **实例检索**: 当前实现不提供检索 Actor 实例的机制。
2. **异步实例化**: 所有 Actor 提交都是异步的，但没有直接的方法等待实例化完成。
3. **错误传播**: 来自 Actor 实例化的异常目前不会传播回调用方。

## 未来增强功能

1. **实例 Futures**: 返回可用于检索 Actor 实例的 futures 或 promises。
2. **同步实例化**: 添加对阻塞直到 Actor 实例化完成的支持。
3. **增强错误处理**: 改进来自 Actor 实例化的错误传播。
4. **批量提交**: 添加对一次提交多个 Actor 的支持。