# 问题描述
用户再次测试 submit_job 任务提交，待执行队列中的任务状态查询结果为QUEUED，符合功能预期。但是仍然发现存在以下问题：
- 1. 提交id为 job_f035a82cadb14a628378 的任务在全局队列中一直无法调度，从控制台信息可知，mac 集群的资源是可以进行调度的，但是显然任务持续处于 QUEUED 状态

# 控制台打印信息
等待 10 秒后再次检查...
2026-01-13 14:40:41,475 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster centos
2026-01-13 14:40:41,936 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [centos] 资源状态: CPU=3.00/20.00 (85.00% 已使用), 内存=3.52/31.19GiB (88.70% 已使用), GPU=0/0, 节点数=0, 评分=0.5
2026-01-13 14:40:41,936 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for centos with stats: CPU usage=85.0%, Mem usage=88.7%, Score=0.5
2026-01-13 14:40:41,936 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster mac
2026-01-13 14:40:42,067 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=3.47/8.00 (56.62% 已使用), 内存=9.84/16.00GiB (38.50% 已使用), GPU=0/0, 节点数=0, 评分=2.8
2026-01-13 14:40:42,067 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for mac with stats: CPU usage=56.62%, Mem usage=38.5%, Score=2.8
2026-01-13 14:40:42,067 - ray_multicluster_scheduler.scheduler.queue.backpressure_controller - WARNING - Backpressure activated: cluster utilization CPU=76.89%, Memory=71.68% exceeds threshold 0.7
2026-01-13 14:40:42,067 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - Backpressure active, applying backoff for 30.00 seconds
2026-01-13 14:40:42,155 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 查询任务状态，submission_id: 1768286318517, cluster: mac
2026-01-13 14:40:42,176 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 从指定集群 mac 查询任务 1768286318517
2026-01-13 14:40:42,184 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 任务 1768286318517 在集群 mac 的状态: RUNNING
2026-01-13 14:40:42,184 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 作业 job_f035a82cadb14a628378 在全局作业队列中等待
Job 1768286318517 状态: RUNNING
Job job_f035a82cadb14a628378 状态: QUEUED
Job job_f035a82cadb14a628378 状态: QUEUED，正在等待资源释放
仍有 2 个job在运行，继续等待...
  运行中的job: ['1768286318517', 'job_f035a82cadb14a628378']

# 任务需求
请分析以上原因，检查待执行任务队列中的任务在集群资源可用时，能否正常提交调度？