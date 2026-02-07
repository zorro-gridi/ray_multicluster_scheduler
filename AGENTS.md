# Ray Multi-Cluster Scheduler - AI Agent 开发指南

## 1. 构建和测试命令

### 1.1 开发环境安装

```bash
# 开发模式安装（推荐）
pip install -e .

# 带开发依赖安装
pip install -e ".[dev]"
```

### 1.2 运行测试

```bash
# 运行所有测试
pytest

# 运行单个测试文件
pytest path/to/test_file.py

# 运行单个测试函数
pytest path/to/test_file.py::test_function_name

# 带覆盖率报告运行
pytest --cov=ray_multicluster_scheduler
```

### 1.3 项目启动

```bash
# 使用入口点启动
ray-multicluster-scheduler

# 或直接运行主模块
python -m ray_multicluster_scheduler.main
```

## 2. 代码风格指南

### 2.1 导入顺序和规范

导入顺序应遵循：标准库 → 第三方库 → 内部包

```python
# 标准库
import time
import signal
import sys

# 第三方库
import ray
from ray.job_submission import JobSubmissionClient

# 内部包
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
```

### 2.2 类型注解

使用 `typing` 模块，所有公共函数和方法的参数及返回值都需要类型注解：

```python
from typing import Dict, List, Optional, Any, Callable, Union

def dispatch_task(self, task_desc: TaskDescription, target_cluster: str = None) -> Any:
    """分发任务到指定集群。"""
    pass
```

### 2.3 命名约定

- **类名**：PascalCase（如 `PolicyEngine`, `TaskQueue`, `ClusterCircuitBreakerManager`）
- **函数/变量**：snake_case（如 `dispatch_task`, `cluster_name`, `resource_available`）
- **常量**：UPPER_CASE（如 `RESOURCE_THRESHOLD`, `MAX_QUEUE_SIZE`）
- **私有方法**：_snake_case（如 `_is_duplicate_task`, `_prepare_runtime_env`）

### 2.4 错误处理

- 使用自定义异常层次结构（继承 `SchedulerError`）
- 捕获异常时记录日志并打印 traceback
- 抛出有意义的错误信息

```python
from ray_multicluster_scheduler.common.exception import TaskSubmissionError

try:
    result = self.circuit_breaker_manager.call_cluster(target_cluster, submit_func)
except Exception as e:
    logger.error(f"Failed to submit task {task_id}: {e}")
    import traceback
    traceback.print_exc()
    raise TaskSubmissionError(f"Could not submit task: {e}")
```

### 2.5 日志记录

- 模块级别使用 `logger = get_logger(__name__)`
- 使用不同级别：`debug`, `info`, `warning`, `error`
- 支持中英双语日志消息

```python
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)

logger.info(f"任务 {task_id} 已加入全局队列")
logger.warning(f"首选集群 {cluster_name} 资源紧张")
logger.error(f"连接失败: {e}")
```

### 2.6 并发编程

- 使用 `threading.Lock()` 保护共享数据
- 使用 `threading.Condition()` 进行线程间通信
- 所有共享数据访问都在上下文管理器中

```python
import threading

class TaskQueue:
    def __init__(self):
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def enqueue(self, item):
        with self.condition:
            self.queue.append(item)
            self.condition.notify()

    def dequeue(self):
        with self.condition:
            while not self.queue:
                if not self.condition.wait(timeout=1.0):
                    return None
            return self.queue.popleft()
```

### 2.7 文档字符串

使用 Google 风格，包含 Args 和 Returns：

```python
def enqueue_global(self, task_desc: TaskDescription) -> bool:
    """将任务添加到全局队列。

    Args:
        task_desc: 要加入队列的任务描述

    Returns:
        True 如果任务成功加入队列，False 如果队列已满

    Raises:
        ValueError: 如果 task_desc 为 None
    """
    pass
```

### 2.8 数据模型

使用 `@dataclass` 定义数据类，在 `__post_init__` 中初始化默认值：

```python
from dataclasses import dataclass

@dataclass
class JobDescription:
    job_id: str
    entrypoint: str
    runtime_env: Optional[Dict[str, Any]] = None
    preferred_cluster: Optional[str] = None

    def __post_init__(self):
        if not self.job_id:
            self.job_id = f"job_{uuid.uuid4().hex[:20]}"
```

### 2.9 代码注释

- 关键逻辑使用中文注释
- 说明 WHY 而不是 WHAT
- 复杂算法添加解释性注释

```python
# 40秒规则：同一集群40秒内只能提交一次顶级任务
# 这是为了防止任务在短时间内集中到同一集群导致资源竞争
if self.cluster_submission_history.is_cluster_available_and_record(cluster_name, is_top_level):
    pass
```

## 3. 项目特定约定

### 3.1 集群配置

- 集群配置文件：`clusters.yaml`
- 支持多集群配置，包括 head_address、dashboard、weight、tags、runtime_env
- 集群优先级：prefer 为 true 的集群优先调度

### 3.2 调度策略

- **资源使用率阈值**：70%（`PolicyEngine.RESOURCE_THRESHOLD = 0.7`）
  - CPU、GPU、内存任一超过阈值则触发负载均衡
- **40秒提交限制**：顶级任务同一集群 40 秒内只能提交一次
  - 子任务不受此限制
  - 任务会进入队列等待直到集群可用

### 3.3 资源管理

- 使用 `ClusterMonitor` 监控集群资源状态
- 通过 `ResourceSnapshot` 获取实时资源信息
- 支持 CPU（核心数）、内存（MB）、GPU 资源跟踪

### 3.4 任务类型

- **Task**：使用 `ray.remote()` 提交的远程函数或 Actor
- **Job**：使用 `JobSubmissionClient` 提交的作业
- 两类任务有独立的队列和调度逻辑

### 3.5 电路断路器

- 失败阈值：默认 5 次
- 恢复超时：默认 60 秒
- 三种状态：CLOSED、OPEN、HALF_OPEN

---

**重要提示**：在提交代码前，确保运行 `pytest` 验证测试通过。项目使用 Ray 框架，注意 Ray 的并发特性和资源管理机制。
