**# 需要验证的问题
用户在实际任务调度测试中发现，若centos和mac集群资源部分、或全部暂时不可用时（例如，资源使用率超过 70% 阈值），继续使用 submit_job 接口向集群调度系统提交新任务时，也会生成 ray job submission id，同时，任务会因为没有可用资源，根据是否设置偏好集群进入全局、或指定集群的待执行任务队列。但是，新生成的ray job submission id 并没有在集群中开始调度，因此，在所有ray集群中都无法查询到新生成的 submission id 对应的 ray job 的状态，导致调度器异常退出，任务调度失败

# 控制台打印信息记录
## ray job 提交记录
2026-01-12 17:49:32,226 - ray_multicluster_scheduler.scheduler.policy.policy_engine - INFO - 未指定首选集群，使用负载均衡策略选择最优集群
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.scheduler.policy.policy_engine - WARNING - 所有集群资源使用率都超过阈值，任务 job_2dba86a6d06e445c9bed 将进入队列等待
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - 作业 job_2dba86a6d06e445c9bed 需要排队等待资源释放
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.scheduler.queue.task_queue - INFO - 作业 job_2dba86a6d06e445c9bed 已加入全局作业队列，当前全局作业队列大小: 1
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 任务提交完成，返回 submission_id: job_2dba86a6d06e445c9bed
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.scheduler.queue.task_queue - INFO - 作业 job_2dba86a6d06e445c9bed 已从全局作业队列中取出，剩余全局作业队列大小: 0
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.app.client_api.submit_job - INFO - 请使用此 submission_id 查询任务状态: get_job_status('job_2dba86a6d06e445c9bed')
Job submitted successfully with ID: job_2dba86a6d06e445c9bed
2026-01-12 17:49:32,227 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - 处理作业 job_2dba86a6d06e445c9bed
2026-01-12 17:49:32,708 - ray_multicluster_scheduler.scheduler.policy.policy_engine - WARNING - 所有集群资源使用率都超过阈值，任务 job_2dba86a6d06e445c9bed 将进入队列等待
2026-01-12 17:49:32,708 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - WARNING - 没有可用集群处理作业 job_2dba86a6d06e445c9bed，重新加入队列

## ray job 状态查询失败
2026-01-12 17:49:35,158 - ray_multicluster_scheduler.app.client_api.submit_job - WARNING - 在所有集群中都未找到任务 job_2dba86a6d06e445c9bed
Job job_2dba86a6d06e445c9bed 状态: UNKNOWN
Job job_2dba86a6d06e445c9bed 状态未知，可能作业已不存在或无法找到
获取job job_2dba86a6d06e445c9bed 状态失败: Job job_2dba86a6d06e445c9bed 状态未知，可能作业已不存在
执行用户函数时出现错误: 无法获取job job_2dba86a6d06e445c9bed 的状态: Job job_2dba86a6d06e445c9bed 状态未知，可能作业已不存在
Traceback (most recent call last):
  File "/Users/zorro/miniconda3/envs/k8s/lib/python3.10/site-packages/ray_multicluster_scheduler/app/client_api/submit_job.py", line 316, in wait_for_all_jobs
    raise RuntimeError(f"Job {submission_id} 状态未知，可能作业已不存在")
RuntimeError: Job job_2dba86a6d06e445c9bed 状态未知，可能作业已不存在

# 任务类型
系统异常分析，不改动系统代码

# 任务需求
请分析验证用户的测试问题是否成立，并给出原因分析。
