# 问题描述
用户通过 submit_job 接口提交了两个 ray job，两个集群初始资源使用率全部超过使用率阈值，任务进入全局任务队列排队，是合理的。但根据mac集群第二次查询到的资源状态信息，可知此时集群资源并未超过使用率阈值70%，是可用集群；（centos 集群使用率超阈值，暂时不可用）按照负载均衡调度策略，任务队列中的job应该提交到 mac 集群调度，但是根据控制台信息可知，任务持续在全局队列中等待调度

# mac 集群资源状态
## 第一次查询结果
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=1.01/8.00 (87.38% 已使用), 内存=9.06/16.00GiB (43.38% 已使用), GPU=0/0, 节点数=0, 评分=0.3

## 第二次查询结果
2026-01-13 11:23:45,815 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=3.54/8.00 (55.75% 已使用), 内存=9.40/16.00GiB (41.25% 已使用), GPU=0/0, 节点数=0, 评分=2.9

# 控制台打印信息
2026-01-13 11:21:46,444 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [centos] 资源状态: CPU=2.63/20.00 (86.85% 已使用), 内存=7.49/31.19GiB (75.99% 已使用), GPU=0/0, 节点数=0, 评分=0.4
2026-01-13 11:21:46,444 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for centos with stats: CPU usage=86.85%, Mem usage=75.99%, Score=0.4
2026-01-13 11:21:46,444 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster mac
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=1.01/8.00 (87.38% 已使用), 内存=9.06/16.00GiB (43.38% 已使用), GPU=0/0, 节点数=0, 评分=0.3
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for mac with stats: CPU usage=87.38%, Mem usage=43.38%, Score=0.3
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.policy.policy_engine - INFO - 未指定首选集群，使用负载均衡策略选择最优集群
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.policy.policy_engine - WARNING - 所有集群资源使用率都超过阈值，任务 job_b8a48a12d10d4710b9a9 将进入队列等待
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - 作业 job_b8a48a12d10d4710b9a9 需要排队等待资源释放
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.scheduler.queue.task_queue - INFO - 作业 job_b8a48a12d10d4710b9a9 已加入全局作业队列，当前全局作业队列大小: 2
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 任务提交完成，返回 submission_id: job_b8a48a12d10d4710b9a9
2026-01-13 11:21:46,485 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 请使用此 submission_id 查询任务状态: get_job_status('job_b8a48a12d10d4710b9a9')
Job submitted successfully with ID: job_b8a48a12d10d4710b9a9
2026-01-13 11:21:47,490 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 作业 job_fcfb6a4437e8414d93f2 在全局作业队列中等待
2026-01-13 11:21:47,491 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 作业 job_b8a48a12d10d4710b9a9 在全局作业队列中等待
等待 10 秒后再次检查...
2026-01-13 11:23:45,493 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster centos
2026-01-13 11:23:45,773 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [centos] 资源状态: CPU=1.84/20.00 (90.80% 已使用), 内存=7.35/31.19GiB (76.42% 已使用), GPU=0/0, 节点数=0, 评分=0.2
2026-01-13 11:23:45,773 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for centos with stats: CPU usage=90.8%, Mem usage=76.42%, Score=0.2
2026-01-13 11:23:45,773 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster mac
2026-01-13 11:23:45,815 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=3.54/8.00 (55.75% 已使用), 内存=9.40/16.00GiB (41.25% 已使用), GPU=0/0, 节点数=0, 评分=2.9
2026-01-13 11:23:45,815 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for mac with stats: CPU usage=55.75%, Mem usage=41.25%, Score=2.9
2026-01-13 11:23:45,815 - ray_multicluster_scheduler.scheduler.queue.backpressure_controller - WARNING - Backpressure activated: cluster utilization CPU=80.79%, Memory=64.50% exceeds threshold 0.7
2026-01-13 11:23:45,815 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - Backpressure active, applying backoff for 30.00 seconds
Job job_fcfb6a4437e8414d93f2 状态: QUEUED
Job job_fcfb6a4437e8414d93f2 状态: QUEUED，正在等待资源释放
Job job_b8a48a12d10d4710b9a9 状态: QUEUED
Job job_b8a48a12d10d4710b9a9 状态: QUEUED，正在等待资源释放
仍有 2 个job在运行，继续等待...
  运行中的job: ['job_fcfb6a4437e8414d93f2', 'job_b8a48a12d10d4710b9a9']

# 任务需求
根据控制台信息分析可知，任务提交时