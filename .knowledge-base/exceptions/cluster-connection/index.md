# 集群连接类异常

## 异常类型
- `ClusterConnectionError`
- `TaskSubmissionError` (连接相关)

## 典型症状
- JobSubmissionClient 创建失败
- Ray Client 连接超时
- 端口无法访问
- Dashboard 地址配置错误

## 案例列表

| 日期 | 标题 | 状态 |
|------|------|------|
| 暂无案例 | - | - |

## 相关文件
- `scheduler/scheduler_core/dispatcher.py`
- `scheduler/connection/ray_client_pool.py`
- `scheduler/connection/job_client_pool.py`
