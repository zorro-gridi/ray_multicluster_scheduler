# 测试发现的问题
当从任务队列取出任务提交到集群执行后，系统仍然使用提交ID来查询ray job的状态，导致查询失败。根据以下控制台打印信息可知：
任务 submission_id 为 job_6e8f4b4c709a48fb9558，但是提交到 ray 集群的执行id为 1768275169033，应该使用 ray job id 1768275169033 查询job的状态

# 控制台打印信息
2026-01-13 11:35:55,752 - ray_multicluster_scheduler.scheduler.policy.policy_engine - WARNING - 所有集群资源使用率都超过阈值，任务 job_6e8f4b4c709a48fb9558 将进入队列等待
2026-01-13 11:35:55,752 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - WARNING - 没有可用集群处理作业 job_6e8f4b4c709a48fb9558，重新加入队列
2026-01-13 11:35:55,752 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - 成功重新调度 1 个作业到更优集群
2026-01-13 11:35:55,833 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 查询任务状态，submission_id: 1768275169033, cluster: mac
2026-01-13 11:35:55,852 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 从指定集群 mac 查询任务 1768275169033
2026-01-13 11:35:55,859 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 任务 1768275169033 在集群 mac 的状态: RUNNING
Job 1768275169033 状态: RUNNING
2026-01-13 11:35:56,064 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - INFO - Using dashboard address for cluster centos: http://192.168.5.7:31591
2026-01-13 11:35:56,078 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - INFO - Created JobSubmissionClient for cluster centos with address http://192.168.5.7:31591
2026-01-13 11:35:56,078 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - INFO - Created JobSubmissionClient for cluster centos on demand
2026-01-13 11:35:56,142 - ray_multicluster_scheduler.app.client_api.submit_job - WARNING - 在所有集群和队列中都未找到任务 job_6e8f4b4c709a48fb9558
Traceback (most recent call last):
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/app/client_api/submit_job.py", line 321, in wait_for_all_jobs
    raise RuntimeError(f"Job {submission_id} 状态未知，可能作业已不存在")
RuntimeError: Job job_6e8f4b4c709a48fb9558 状态未知，可能作业已不存在

# 任务需求
请分析优化后的功能是否未使用任务执行id查询任务的状态，导致查询错误