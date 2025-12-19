# Ray Multicluster Scheduler - CatBoost 示例

这个目录包含了将 CatBoost 训练任务适配到 Ray 多集群调度框架的示例代码。

## 文件说明

### 1. `catboost_simplified.py`
这是一个简化版的 CatBoost 训练程序，移除了对 Ray 的直接依赖，使其可以作为普通 Python 函数在我们的调度框架中执行。

主要修改：
- 移除了所有的 `@ray.remote` 装饰器
- 移除了 `Train.remote()` 和 `.train.remote()` 调用
- 移除了 `ray.wait()` 和 `ray.get()` 调用
- 用模拟类替换了原始的数据库和 MLflow 组件

### 2. `submit_catboost_job.py`
这是将 CatBoost 训练任务提交到 Ray 多集群调度框架的脚本，使用了新的统一调度器接口。

功能：
- 使用 `initialize_scheduler_environment()` 一键初始化调度环境
- 使用 `submit_task` 将训练任务提交到调度器
- 指定资源需求和标签

### 3. `catboost_modification_guide.md`
详细说明了如何修改原始的 CatBoost 程序以适配我们的调度框架。

### 4. `unified_scheduler_example.py`
演示如何使用新的统一调度器接口的示例脚本。

### 5. 测试脚本
- `test_catboost_simple.py` - 简单测试，不连接实际 Ray 集群
- `test_catboost_scheduler.py` - 完整测试，会尝试连接 Ray 集群

## 如何使用

### 1. 直接运行简化版程序
```bash
python demo/catboost_simplified.py
```

### 2. 通过调度器提交任务（使用统一接口）
```bash
python demo/submit_catboost_job.py
```

### 3. 运行统一接口示例
```bash
python demo/unified_scheduler_example.py
```

### 4. 运行测试
```bash
python demo/test_catboost_simple.py
```

## 新的统一调度器接口

我们引入了新的统一调度器接口，大大简化了任务提交流程：

### 1. 环境初始化
```python
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment

# 一键初始化所有调度器组件
task_lifecycle_manager = initialize_scheduler_environment()
```

### 2. 任务提交
```python
from ray_multicluster_scheduler.app.client_api.unified_scheduler import submit_task, submit_actor

# 提交任务
submit_task(
    func=my_function,
    args=(arg1, arg2),
    resource_requirements={"CPU": 2, "memory": 4 * 1024 * 1024 * 1024},
    tags=["ml", "training"],
    name="my_task"
)

# 提交 Actor
submit_actor(
    actor_class=MyActorClass,
    args=(arg1, arg2),
    resource_requirements={"CPU": 2, "memory": 4 * 1024 * 1024 * 1024},
    tags=["ml", "actor"],
    name="my_actor"
)
```

## 修改原始程序的要点

根据 `catboost_modification_guide.md`，要将原始的 CatBoost 程序适配到我们的调度框架，需要：

1. **移除 Ray 初始化代码**
   - 删除 `import ray` 和 `ray.init()` 调用
   - 删除 `@ray.remote` 装饰器

2. **重构主逻辑**
   - 将全局代码封装到函数中
   - 确保主入口点清晰

3. **调整并行处理**
   - 将 `Train.remote()` 替换为 `Train()`
   - 将 `train.remote()` 替换为 `train()`

4. **适配资源声明**
   - 通过 `submit_task` 的 `resource_requirements` 参数声明资源需求

## 资源需求

提交到调度器的任务可以指定以下资源需求：
- CPU 核心数
- 内存大小
- GPU 数量（如果需要）

示例：
```python
resource_requirements = {
    "CPU": 4,
    "memory": 8 * 1024 * 1024 * 1024,  # 8GB
    "GPU": 1  # 如果需要 GPU
}
```

## 标签系统

任务可以附加标签，调度器会根据标签选择合适的集群：
```python
tags = ["ml", "catboost", "training", "finance"]
```

## 注意事项

1. **环境变量**：确保在运行调度器的环境中设置了必要的环境变量
2. **网络连接**：调度器需要能够连接到配置的 Ray 集群
3. **依赖项**：确保所有依赖项在目标集群上可用