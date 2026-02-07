# 资源调度类异常

## 异常类型
- `NoHealthyClusterError`
- `PolicyEvaluationError` (资源相关)

## 典型症状
- 所有集群资源超过 70% 阈值
- 无可用集群
- 任务进入队列但无法调度
- Job status 未知

## 案例列表

| 日期 | 标题 | 状态 |
|------|------|------|
| 暂无案例 | - | - |

## 相关文件
- `scheduler/policy/policy_engine.py`
- `scheduler/scheduler_core/dispatcher.py`
- `scheduler/monitor/cluster_monitor.py`
