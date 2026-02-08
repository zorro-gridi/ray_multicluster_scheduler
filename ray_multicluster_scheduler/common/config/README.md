# 调度系统统一配置模块

## 概述

`SchedulerSettings` 模块提供调度系统全部常量参数的统一管理，支持环境变量、配置文件和代码默认值三种配置方式。

## 使用方法

### 基本导入

```python
from ray_multicluster_scheduler.common.config import settings

# 使用默认值
print(settings.SUBMISSION_WAIT_TIME)  # 60.0
```

### 导入执行模式枚举

```python
from ray_multicluster_scheduler.common.config import ExecutionMode

# 使用执行模式常量
print(ExecutionMode.SERIAL)    # serial
print(ExecutionMode.PARALLEL)  # parallel
```

### 配置源优先级（从高到低）

1. **环境变量** `SCHEDULER_*` - 最高优先级，直接覆盖默认值
2. **配置文件** `~/.ray_scheduler/settings.yaml` - 中优先级
3. **代码默认值** - 最低优先级

### 从配置文件加载

```python
# 加载默认配置文件 (~/.ray_scheduler/settings.yaml)
settings.load()

# 加载指定配置文件
settings.load("/path/to/settings.yaml")
```

## 常量参数参考

### 调度策略常量 (Policy Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `EXECUTION_MODE` | "serial" | `SCHEDULER_EXECUTION_MODE` | str | 任务执行模式（serial/parallel） |
| `SUBMISSION_WAIT_TIME` | 60 | `SCHEDULER_SUBMISSION_WAIT_TIME` | float | 同一集群连续提交顶级任务的最小间隔（秒） |
| `RESOURCE_THRESHOLD` | 0.7 | `SCHEDULER_RESOURCE_THRESHOLD` | float | 资源使用率阈值（70%） |
| `RESOURCE_CHECK_INTERVAL` | 5 | `SCHEDULER_RESOURCE_CHECK_INTERVAL` | int | 资源检查间隔（秒） |

### 超时管理常量 (Timeout Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `JOB_COMPLETION_TIMEOUT` | 600.0 | `SCHEDULER_JOB_COMPLETION_TIMEOUT` | float | 等待 Job 完成的超时时间（秒） |
| `TASK_COMPLETION_TIMEOUT` | 600.0 | `SCHEDULER_TASK_COMPLETION_TIMEOUT` | float | 等待 Task 完成的超时时间（秒） |
| `ACTOR_READY_TIMEOUT` | 300 | `SCHEDULER_ACTOR_READY_TIMEOUT` | int | 等待 Actor 就绪的超时时间（秒） |
| `DEFAULT_TASK_TIMEOUT` | 300 | `SCHEDULER_DEFAULT_TASK_TIMEOUT` | int | 默认任务超时时间（秒） |
| `CONNECTION_TIMEOUT` | 300 | `SCHEDULER_CONNECTION_TIMEOUT` | int | Ray 连接超时时间（秒） |
| `JOB_CLIENT_TIMEOUT` | 300 | `SCHEDULER_JOB_CLIENT_TIMEOUT` | int | Job 客户端超时时间（秒） |
| `JOB_CLIENT_CACHE_TIMEOUT` | 300 | `SCHEDULER_JOB_CLIENT_CACHE_TIMEOUT` | int | Job 客户端缓存超时时间（秒） |
| `JOB_CLIENT_CONNECTION_TEST_TIMEOUT` | 5 | `SCHEDULER_JOB_CLIENT_CONNECTION_TEST_TIMEOUT` | int | Job 客户端连接测试超时时间（秒） |

### 轮询与间隔常量 (Polling Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `JOB_STATUS_CHECK_INTERVAL` | 15 | `SCHEDULER_JOB_STATUS_CHECK_INTERVAL` | int | 检查 Job 状态的间隔（秒） |
| `HEALTH_CHECK_INTERVAL` | 30 | `SCHEDULER_HEALTH_CHECK_INTERVAL` | int | 健康检查间隔（秒） |
| `CACHE_TIMEOUT` | 5.0 | `SCHEDULER_CACHE_TIMEOUT` | float | 集群快照缓存时间（秒） |
| `EMPTY_QUEUE_SLEEP` | 15.0 | `SCHEDULER_EMPTY_QUEUE_SLEEP` | float | 队列为空时主循环休眠时间（秒） |
| `QUEUE_CONDITION_WAIT_TIMEOUT` | 1.0 | `SCHEDULER_QUEUE_CONDITION_WAIT_TIMEOUT` | float | 队列条件变量等待超时时间（秒） |
| `ACTOR_NOTIFICATION_WAIT` | 1.0 | `SCHEDULER_ACTOR_NOTIFICATION_WAIT` | float | Actor 通知等待超时时间（秒） |
| `JOB_POLL_CHECK_INTERVAL` | 5 | `SCHEDULER_JOB_POLL_CHECK_INTERVAL` | int | 轮询 Job 状态默认检查间隔（秒） |
| `PROMETHEUS_QUERY_TIMEOUT` | 20 | `SCHEDULER_PROMETHEUS_QUERY_TIMEOUT` | int | Prometheus HTTP 查询超时时间（秒） |

### 队列管理常量 (Queue Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `TASK_QUEUE_MAX_SIZE` | 1000 | `SCHEDULER_TASK_QUEUE_MAX_SIZE` | int | 任务队列最大容量 |
| `TASK_QUEUE_DEFAULT_MAX_SIZE` | 100 | `SCHEDULER_TASK_QUEUE_DEFAULT_MAX_SIZE` | int | 任务队列默认最大容量 |

### 熔断器常量 (Circuit Breaker Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | 5 | `SCHEDULER_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | int | 熔断器故障阈值（失败次数） |
| `CIRCUIT_BREAKER_RECOVERY_TIMEOUT` | 60.0 | `SCHEDULER_CIRCUIT_BREAKER_RECOVERY_TIMEOUT` | float | 熔断器恢复超时时间（秒） |

### 重试与退避常量 (Retry & Backoff Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `RETRY_SLEEP` | 0.5 | `SCHEDULER_RETRY_SLEEP` | float | 连接重试时的休眠时间（秒） |
| `BACKOFF_TIME` | 30.0 | `SCHEDULER_BACKOFF_TIME` | float | 背压激活时的退避时间（秒） |
| `WORKER_THREAD_JOIN_TIMEOUT` | 5.0 | `SCHEDULER_WORKER_THREAD_JOIN_TIMEOUT` | float | 工作线程优雅关闭等待时间（秒） |
| `ERROR_LOOP_SLEEP` | 0.1 | `SCHEDULER_ERROR_LOOP_SLEEP` | float | 错误状态下短暂延迟（秒） |
| `REPEATED_ERROR_SLEEP` | 1.0 | `SCHEDULER_REPEATED_ERROR_SLEEP` | float | 重复错误时休眠时间（秒） |

### 其他常量 (Miscellaneous Constants)

| 常量名 | 默认值 | 环境变量 | 类型 | 说明 |
|--------|--------|----------|------|------|
| `CLUSTER_WEIGHT_DEFAULT` | 1.0 | `SCHEDULER_CLUSTER_WEIGHT_DEFAULT` | float | 集群权重默认值 |

## 执行模式 (Execution Mode)

### 模式说明

调度器支持两种任务执行模式：

- **串行模式 (SERIAL)**: 默认模式，集群中一次只允许一个 Ray Job 调度执行，防止资源竞争
- **并行模式 (PARALLEL)**: 同时允许多个 Ray Job 调度执行，受资源阈值限制

### 配置方法

```python
from ray_multicluster_scheduler.common.config import settings, ExecutionMode

# 查看当前模式
print(settings.EXECUTION_MODE)  # 默认: "serial"

# 设置为串行模式（通过环境变量）
# export SCHEDULER_EXECUTION_MODE=serial

# 设置为并行模式（通过环境变量）
# export SCHEDULER_EXECUTION_MODE=parallel
```

### 配置文件示例

```yaml
# ~/.ray_scheduler/settings.yaml
execution_mode: serial
submission_wait_time: 60
resource_threshold: 0.7
```

### 验证命令

```bash
# 查看当前执行模式
python -c "from ray_multicluster_scheduler.common.config import settings; print(settings.EXECUTION_MODE)"

# 串行模式测试
SCHEDULER_EXECUTION_MODE=serial python -c "from ray_multicluster_scheduler.common.config import settings, ExecutionMode; print(settings.EXECUTION_MODE)"

# 并行模式测试
SCHEDULER_EXECUTION_MODE=parallel python -c "from ray_multicluster_scheduler.common.config import settings, ExecutionMode; print(settings.EXECUTION_MODE)"
```

## 配置示例

### 环境变量配置

```bash
# 设置执行模式
export SCHEDULER_EXECUTION_MODE=serial  # 串行模式
export SCHEDULER_EXECUTION_MODE=parallel  # 并行模式

# 设置提交等待时间
export SCHEDULER_SUBMISSION_WAIT_TIME=60

# 设置资源阈值
export SCHEDULER_RESOURCE_THRESHOLD=0.8

# 设置任务完成超时
export SCHEDULER_TASK_COMPLETION_TIMEOUT=900

# 设置 Prometheus 查询超时
export SCHEDULER_PROMETHEUS_QUERY_TIMEOUT=30
```

### 配置文件

创建 `~/.ray_scheduler/settings.yaml` 文件：

```yaml
# 调度策略配置
submission_wait_time: 60
resource_threshold: 0.8

# 超时配置
job_completion_timeout: 900.0
task_completion_timeout: 900.0
connection_timeout: 300

# 轮询间隔
job_status_check_interval: 10
health_check_interval: 30
cache_timeout: 10.0

# 熔断器配置
circuit_breaker_failure_threshold: 5
circuit_breaker_recovery_timeout: 120.0
```

### 验证命令

```bash
# 运行 settings 集成测试
pytest test/test_settings_integration.py -v

# 验证 settings 模块导入
python -c "from ray_multicluster_scheduler.common.config import settings; print(settings.to_dict())"

# 验证环境变量覆盖
SCHEDULER_SUBMISSION_WAIT_TIME=30 python -c "from ray_multicluster_scheduler.common.config import settings; print(settings.SUBMISSION_WAIT_TIME)"
```

## API 参考

### SchedulerSettings 类

```python
class SchedulerSettings:
    def load(self, config_file: Optional[str] = None) -> "SchedulerSettings"
    """从配置文件加载配置"""

    def to_dict(self) -> Dict[str, Any]
    """将配置导出为字典"""

    def _convert_value(self, value: str, default: Any) -> Any
    """将字符串值转换为正确的类型（内部方法）"""
```

### 全局单例

```python
from ray_multicluster_scheduler.common.config import settings

# settings 是自动加载配置的全局单例
# 无需手动调用 load()，除非需要重新加载配置
```

## 模块集成

以下模块使用 `settings` 常量：

| 模块路径 | 使用常量 |
|----------|----------|
| `scheduler/connection/ray_client_pool.py` | `CONNECTION_TIMEOUT` |
| `scheduler/connection/job_client_pool.py` | `JOB_CLIENT_CACHE_TIMEOUT`, `JOB_CLIENT_CONNECTION_TEST_TIMEOUT` |
| `scheduler/lifecycle/task_lifecycle_manager.py` | `JOB_COMPLETION_TIMEOUT`, `TASK_COMPLETION_TIMEOUT`, `JOB_STATUS_CHECK_INTERVAL` |
| `scheduler/monitor/cluster_monitor.py` | `CACHE_TIMEOUT`, `HEALTH_CHECK_INTERVAL` |
| `scheduler/monitor/prometheus_client.py` | `PROMETHEUS_QUERY_TIMEOUT` |
| `scheduler/policy/cluster_submission_history.py` | `SUBMISSION_WAIT_TIME` |
| `scheduler/policy/policy_engine.py` | `RESOURCE_THRESHOLD`, `RESOURCE_CHECK_INTERVAL` |
| `scheduler/queue/task_queue.py` | `TASK_QUEUE_DEFAULT_MAX_SIZE` |
| `common/circuit_breaker.py` | `CIRCUIT_BREAKER_FAILURE_THRESHOLD`, `CIRCUIT_BREAKER_RECOVERY_TIMEOUT` |
| `control_plane/config/__init__.py` | 使用 `settings` 替代硬编码 |

## 注意事项

1. **环境变量优先级**：环境变量会覆盖配置文件和默认值
2. **类型转换**：系统会自动将环境变量字符串转换为正确类型（int/float/bool）
3. **配置热更新**：修改环境变量后需要重启应用才能生效
4. **线程安全**：`settings` 是模块级单例，在应用生命周期内应保持不变
