---
exception_type: SerialExecutionError
symptoms:
  - 串行模式下任务仍被并行调度
  - is_cluster_busy() 检查被绕过
  - 集群繁忙时新任务未进入队列等待
module: scheduler/lifecycle/task_lifecycle_manager.py
related_files:
  - scheduler/queue/task_queue.py
  - scheduler/policy/cluster_submission_history.py
tags: [serial, concurrency, scheduling, bug-fix]
created: 2026-02-09
verified: false
verified_date:
resolution_time:
status: draft
---

# 串行调度模式检查绕过问题

## 异常类型
`SchedulerError` > `SerialExecutionError`

## 问题场景
在串行调度模式下，当任务/作业来自集群专属队列（`source_cluster_queue` 不为空）时，直接调用 `dispatch_task()`/`dispatch_job()`，跳过了 `is_cluster_busy()` 检查。这导致即使集群正在执行任务，新任务仍会被立即调度，违反了严格串行执行的设计。

## 问题根因

### 代码路径分析

`_process_task` 和 `_process_job` 方法存在两条处理路径：

| 路径 | 场景 | 是否经过 is_cluster_busy() 检查 |
|------|------|-------------------------------|
| 路径1 | `source_cluster_queue` 存在（来自集群专属队列） | ❌ 跳过，直接调度 |
| 路径2 | `source_cluster_queue` 为空（来自全局队列） | ✅ 经过 PolicyEngine 检查 |

### 核心问题位置

1. **`_process_task`** (第852-853行):
   ```python
   # 直接调度到指定集群 - 跳过了串行模式检查
   future = self.dispatcher.dispatch_task(task_desc, source_cluster_queue)
   ```

2. **`_process_job`** (第1179-1180行):
   ```python
   # 直接调度到指定集群 - 跳过了串行模式检查
   actual_submission_id = self.dispatcher.dispatch_job(converted_job_desc, source_cluster_queue)
   ```

## 解决方案

### 方案：添加串行模式前置检查

在任务/作业实际提交前进行串行模式检查，确保无论任务来自哪个队列路径，都必须通过串行检查才能提交。

### 实现步骤

#### 1. 添加统一检查方法（task_queue.py）

```python
@classmethod
def check_serial_mode(cls, cluster_name: str, is_top_level_task: bool = True) -> bool:
    """
    串行模式检查（统一检查方法）

    Args:
        cluster_name: 集群名称
        is_top_level_task: 是否为顶级任务

    Returns:
        True: 集群可用，可以继续处理
        False: 集群繁忙，需要返回队列等待
    """
    from ray_multicluster_scheduler.common.config import settings, ExecutionMode

    if settings.EXECUTION_MODE != ExecutionMode.SERIAL:
        return True

    # 串行模式：检查是否有任务正在执行
    if cls.is_cluster_busy(cluster_name):
        logger.debug(f"集群 {cluster_name} 在串行模式下繁忙，有任务正在执行")
        return False

    return True
```

#### 2. 修改 _process_task 方法

在路径1的 `dispatch_task` 调用前添加：

```python
# 串行模式检查：确保集群可用
from ray_multicluster_scheduler.common.config import settings, ExecutionMode
if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
    if TaskQueue.is_cluster_busy(source_cluster_queue):
        logger.warning(f"集群 {source_cluster_queue} 在串行模式下繁忙，任务 {task_desc.task_id} 重新入队")
        self.task_queue.enqueue(task_desc, source_cluster_queue)
        if not self._is_duplicate_task_in_tracked_list(task_desc):
            self.queued_tasks.append(task_desc)
        return
```

#### 3. 修改 _process_job 方法

在路径1的 `dispatch_job` 调用前添加：

```python
# 串行模式检查：确保集群可用
from ray_multicluster_scheduler.common.config import settings, ExecutionMode
if settings.EXECUTION_MODE == ExecutionMode.SERIAL:
    if TaskQueue.is_cluster_busy(source_cluster_queue):
        logger.warning(f"集群 {source_cluster_queue} 在串行模式下繁忙，作业 {job_desc.job_id} 重新入队")
        self.task_queue.enqueue_job(job_desc, source_cluster_queue)
        if not self._is_duplicate_job_in_tracked_list(job_desc):
            self.queued_jobs.append(job_desc)
        return
```

## 验证方法

### 单元测试

```python
def test_serial_mode_task_rejection():
    """串行模式下，集群繁忙时任务是否正确进入队列等待"""
    # 模拟集群有任务正在执行（is_cluster_busy() 返回 True）
    TaskQueue._running_tasks = {"task_001": {"cluster": "centos", "type": "task", "submitted_at": time.time()}}

    # 设置串行模式
    settings = get_settings()
    settings.EXECUTION_MODE = ExecutionMode.SERIAL

    # 创建任务
    task_desc = TaskDescription(task_id="task_002", func_or_class=test_func)

    # 处理任务 - 应该被拒绝并重新入队
    lifecycle_manager._process_task(task_desc, cluster_snapshots, "centos")

    # 验证任务被重新入队
    assert TaskQueue.is_task_in_cluster_queue("task_002", "centos")
```

### 集成测试

1. **测试场景**：连续提交多个 Ray Job 到同一集群
2. **验证方法**：
   - 提交 Job A，启动后立即提交 Job B
   - 验证 Job B 被正确拒绝进入队列
   - Job A 完成后，Job B 才开始执行

## 回滚方案

如需回滚，注释掉以下位置的检查代码：
- `_process_task` 第855-863行
- `_process_job` 第1181-1189行

## 相关配置

- `settings.EXECUTION_MODE`: 执行模式（serial/parallel）
- `TaskQueue.is_cluster_busy()`: 检查集群是否有运行中的任务
- `TaskQueue.register_running_task()`: 注册运行中的任务
- `TaskQueue.unregister_running_task()`: 注销已完成的任务
