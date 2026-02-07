# 发现问题
- 1. 实例化了不必要的连接管理模块
- 2. 当集群所有资源不可用时，轮询获取集群资源状态的等待时间过短

# 控制台打印样例
2025-12-29 11:36:27,621 - ray_multicluster_scheduler.scheduler.connection.ray_client_pool - INFO - Added cluster mac to connection pool
2025-12-29 11:36:27,621 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - INFO - Added cluster mac to job client pool metadata

# 连接管理模块规范
- 1. ray_client_pool 负责管理 submit_task / submit_actor 接口提交的任务
- 2. job_client_pool 负责管理 submit_job 接口提交的任务

# 改善目标
- 1. submit_task / submit_actor 接口提交只实例化 ray_client_pool，submit_job 仅实例化 job_client_pool
- 2. 将每次轮询系统资源快照的等待时间改为 15 秒