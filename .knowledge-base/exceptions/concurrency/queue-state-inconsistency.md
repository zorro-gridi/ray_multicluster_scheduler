---
exception_type: QueueStateInconsistencyError
symptoms:
  - 任务提交后停留在队列中无法调度
  - queued_jobs 跟踪列表与实际队列状态不一致
  - 等待任务无法被重新评估调度
  - 重新评估方法重复处理已处理的任务
module: scheduler/lifecycle/task_lifecycle_manager.py
related_files:
  - ray_multicluster_scheduler/scheduler/lifecycle/task_lifecycle_manager.py
  - ray_multicluster_scheduler/scheduler/queue/task_queue.py
tags: [队列状态, 任务调度, 资源等待, 状态同步, 死循环]
created: 2026-02-09
verified: false
resolution_time: 2h
status: draft
---

# 任务队列调度死锁问题

## 异常类型
`SchedulerError` > `QueueStateInconsistencyError`

## 问题场景

用户通过 `submit_job` 接口提交了3个任务：
1. 2个任务被立即调度执行
2. 1个任务（job_a433e102c31845fc8df2）在全局队列等待
3. 当一个任务完成后，等待的任务没有被调度到空闲集群

## 根本原因

### 核心问题：队列状态不一致

`TaskLifecycleManager` 的 `_worker_loop()` 和 `_re_evaluate_queued_jobs()` 方法中存在队列状态同步问题：

**问题流程：**

1. 任务提交时：
   ```python
   self.task_queue.enqueue_job(job_desc)  # 加入实际队列
   self.queued_jobs.append(job_desc)       # 加入跟踪列表 ✓
   ```

2. `_re_evaluate_queued_jobs()` 处理任务时：
   ```python
   self._process_job(job_desc, cluster_snapshots, None)
   ```
   - `_process_job()` 从 `global_job_queue` 中取出任务
   - 但是 `queued_jobs` 中仍然保留该任务引用 ✗

3. 下一次 `_worker_loop()` 迭代：
   ```python
   has_queued_jobs = len(self.queued_jobs) > 0  # True（残留引用）
   has_global_jobs = len(self.task_queue.global_job_queue) > 0  # False（已清空）
   ```
   - 重新评估触发，但实际队列已空

4. 循环等待：`_re_evaluate_queued_jobs()` 再次尝试处理已处理的任务，导致死循环

## 调用链分析

```
submit_job()
  ↓
task_queue.enqueue_job() + queued_jobs.append()
  ↓
_worker_loop()
  ↓
_re_evaluate_queued_jobs()
  ↓
_process_job()  ← 从队列取出，但未从 queued_jobs 移除
  ↓
_worker_loop() 再次检查 queued_jobs，仍有残留引用
```

## 解决方案

### 方案：同步清理 + 处理后清理

#### 1. 添加 TaskQueue 辅助方法

**文件**: `ray_multicluster_scheduler/scheduler/queue/task_queue.py`

```python
def get_all_global_task_ids(self) -> set:
    """获取全局队列中所有任务的ID"""
    with self.lock:
        return self.global_task_ids.copy()

def get_all_global_job_ids(self) -> set:
    """获取全局作业队列中所有作业的ID"""
    with self.lock:
        return self.global_job_ids.copy()
```

#### 2. 重构 _re_evaluate_queued_jobs()

**文件**: `ray_multicluster_scheduler/scheduler/lifecycle/task_lifecycle_manager.py`

在遍历之前先同步清理：

```python
def _re_evaluate_queued_jobs(self, cluster_snapshots, cluster_info):
    try:
        # 1. 同步清理：移除已不在实际队列中的作业
        try:
            global_job_ids = self.task_queue.get_all_global_job_ids()
            self.queued_jobs = [j for j in self.queued_jobs if j.job_id in global_job_ids]
        except Exception:
            pass

        if not self.queued_jobs:
            return

        # ... 后续处理逻辑不变
```

#### 3. 重构 _re_evaluate_queued_tasks()

同样的同步清理逻辑应用于任务队列：

```python
def _re_evaluate_queued_tasks(self, cluster_snapshots, cluster_info):
    try:
        # 1. 同步清理：移除已不在实际队列中的任务
        try:
            global_task_ids = self.task_queue.get_all_global_task_ids()
            self.queued_tasks = [t for t in self.queued_tasks if t.task_id in global_task_ids]
        except Exception:
            pass

        if not self.queued_tasks:
            return
        # ...
```

#### 4. _process_job() finally 块中清理

```python
finally:
    job_desc.is_processing = False
    # 从 queued_jobs 跟踪列表中移除已处理的作业
    if job_desc in self.queued_jobs:
        self.queued_jobs.remove(job_desc)
    # ...注销运行任务
```

#### 5. _process_task() finally 块中清理

```python
finally:
    task_desc.is_processing = False
    # 从 queued_tasks 跟踪列表中移除已处理的任务
    if task_desc in self.queued_tasks:
        self.queued_tasks.remove(task_desc)
    # ...注销运行任务
```

## 预防措施

1. **设计原则**：采用"先清理，再处理"的策略
   - 在所有依赖 `queued_jobs` 的操作之前，先同步清理已不在实际队列中的任务
   - 确保 `queued_jobs` 和 `task_queue` 中的队列保持一致

2. **代码规范**：
   - 任何从队列取出的操作，必须在处理完成后清理跟踪列表
   - 在重新评估方法入口处添加同步清理逻辑

3. **测试覆盖**：
   - 验证 `queued_jobs` 和 `global_job_queue` 的同步逻辑
   - 测试边界情况：空队列、并发操作

## 相关案例

- [串行调度模式检查绕过问题](./serial-execution.md)

## 验证方法

1. **集成测试**：
   ```
   场景：提交3个任务，验证队列调度行为
   1. 提交任务A、B、C
   2. A和B被调度，C在队列等待
   3. A完成，等待B完成
   4. B完成后，检查C是否被调度
   预期：C应该被正确调度到空闲集群
   ```

2. **日志验证**：
   - 观察队列状态变化日志
   - 观察重新评估日志
   - 观察任务调度日志
