# 并发执行类异常

## 异常类型
- `SchedulerError` (并发相关)

## 典型症状
- 任务重复执行
- queued_tasks 与 TaskQueue 状态不一致
- 缺少 is_processing 并发标记
- 竞态条件
- 串行模式下任务仍被并行调度

## 案例列表

| 日期 | 标题 | 状态 |
|------|------|------|
| 2026-02-09 | [串行调度模式检查绕过问题](./serial-execution.md) | draft |
| 2026-02-09 | [任务队列调度死锁问题](./queue-state-inconsistency.md) | draft |

## 相关文件
- `scheduler/lifecycle/task_lifecycle_manager.py`
- `scheduler/queue/task_queue.py`
- `scheduler/queue/backpressure_controller.py`
